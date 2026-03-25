# -*- coding: utf-8 -*-
"""
config_merger.py
职责：统一管理 DataWorks 部署脚本的配置读取与合并。
      从目标环境目录读取 global.json（如果存在），提取易变业务参数，
      并动态覆盖/注入到底层环境模板 JSON 中，返回最终发往 API 的完整配置字典。
"""

import json
import re
import logging
from pathlib import Path
from typing import List, Dict, Any

# 配置模块日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def _parse_all_columns_from_sqls(sql_dir: Path) -> List[str]:
    """内部辅助：从 ddl 目录下所有的 .sql 文件中汇总提取出所有的字段名"""
    columns = []
    
    sql_files = sorted(list(sql_dir.glob("*.sql"))) if sql_dir.exists() else []
    
    # 匹配 CREATE TABLE (...) 或 ALTER TABLE ADD COLUMNS (...)
    # 使用正则表达式寻找列名，支持反引号和普通字母下划线格式
    # 忽略注释、空行以及常见的建立约束的保留字（如 PRIMARY KEY）
    
    col_pattern = re.compile(r'^\s*(?:`([^`]+)`|([a-zA-Z_][a-zA-Z0-9_]*))\s+[A-Za-z]+')
    
    for filepath in sql_files:
        try:
            content = filepath.read_text(encoding="utf-8")
            
            # 使用一个稍微通用的正则找出字段块：
            matches = re.finditer(r'(?:CREATE\s+TABLE|ADD\s+COLUMNS)[^(]*\((.*?)\)(?:\s*(?:COMMENT|PARTITIONED|;|\Z)|$)', content, re.IGNORECASE | re.DOTALL)
            
            for match in matches:
                cols_block = match.group(1)
                # 解析 cols_block 里的列
                for line in cols_block.split('\n'):
                    line = line.strip()
                    if not line or line.startswith('--') or line.startswith('//'):
                        continue
                    
                    # 剥离最后的逗号
                    line = line.rstrip(',')
                    
                    # 使用强大的正则精准提取该行的字段名
                    col_match = col_pattern.match(line)
                    if col_match:
                        col_name = col_match.group(1) or col_match.group(2)
                        if col_name and col_name.upper() not in ('PRIMARY', 'KEY', 'UNIQUE', 'CONSTRAINT', 'INDEX'):
                            if col_name not in columns:
                                columns.append(col_name)
        except Exception as e:
            logger.warning(f"从 {filepath.name} 解析 SQL 字段映射失败: {e}")
            
    return columns

def _load_json_silently(filepath: Path) -> Dict[str, Any]:
    """内部辅助：静默读取 json 文件，不存在或报错时返回空字典"""
    if not filepath.exists():
        return {}
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"加载配置失败 {filepath.name}: {e}")
        return {}

def _load_base_config(project_dir: str, template_name: str) -> Dict[str, Any]:
    """内部辅助：向上遍历寻找 configuration 文件夹读取基础模板配置，不存在则抛出异常"""
    cfg_path = None
    current = Path(project_dir).resolve()
    
    # 向上寻找包含 configuration 的目录
    while current.parent != current:
        if (current / "default-setting").exists():
            cfg_path = current / "default-setting" / template_name
            break
        current = current.parent
        
    if not cfg_path or not cfg_path.exists():
        # 回退到当前工作目录下的 configuration
        cfg_path = Path.cwd() / "default-setting" / template_name
        if not cfg_path.exists():
            raise FileNotFoundError(f"缺少必要的基础模板文件: {cfg_path}")
            
    with open(cfg_path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_merged_node_config(project_dir: str, env: str = "dev") -> Dict[str, Any]:
    """
    加载节点任务配置（用于 process_project.py）：
    读取 integration-config.json 底板，并将 setting-<env>.json 中 [task] 的值覆盖上去。
    """
    # 1. 读底板
    config = _load_base_config(project_dir, "integration-config.json")
    
    # 2. 读 setting-<env>.json
    global_cfg = _load_json_silently(Path(project_dir) / f"setting-{env}.json")
    task_global = global_cfg.get("task", {})

    # 3. 动态属性覆写 (如果有)
    if not task_global:
        return config

    if "node_name" in task_global:
        config["node_name"] = task_global["node_name"]
    if "cron" in task_global:
        config["cron"] = task_global["cron"]
    if "upstream_node_name" in task_global:
        config["upstream_node_name"] = task_global["upstream_node_name"]
    if "downstream_node_name" in task_global:
        config["downstream_node_name"] = task_global["downstream_node_name"]
    
    # Reader 覆盖
    if "reader_datasource" in task_global and "reader" in config:
        config["reader"]["datasource"] = task_global["reader_datasource"]
    if "reader_path" in task_global and "reader" in config:
        config["reader"]["path"] = task_global["reader_path"]

    # Writer 覆盖
    if "writer_datasource" in task_global and "writer" in config:
        config["writer"]["datasource"] = task_global["writer_datasource"]
    if "writer_table" in task_global and "writer" in config:
        config["writer"]["table"] = task_global["writer_table"]
    if "writer_partition" in task_global and "writer" in config:
        config["writer"]["partition"] = task_global["writer_partition"]

    # 4. 动态解析 ddl/*.sql 并注入字段映射
    sql_dir = Path(project_dir) / "ddl"
    columns = _parse_all_columns_from_sqls(sql_dir)
    
    if columns and "reader" in config and "writer" in config:
            # 构造 Reader mapping (默认转为 string 或 binary 取决于你的需求，这里默认用 BINARY 兼容 Parquet)
            reader_cols = []
            for idx, col in enumerate(columns):
                reader_cols.append({
                    "name": col,
                    "type": "BINARY",
                    "index": idx,
                    "originalType": "UTF8",
                    "repetition": "OPTIONAL"
                })
            
            # 构造 Writer mapping (MaxCompute 目标端只需列名数组)
            writer_cols = columns

            config["reader"]["column"] = reader_cols
            config["writer"]["column"] = writer_cols
            logger.info(f"自动从 create-table.sql 中提取了 {len(columns)} 个字段用于数据集成映射。")

    return config


def load_merged_oss_ds_config(project_dir: str, env: str = "dev") -> Dict[str, Any]:
    """
    加载 OSS 数据源配置（用于 create_oss_ds.py）：
    读取 oss-datasource.json 底板，并将 setting-<env>.json 中 [oss_datasource] 的值覆盖上去。
    """
    config = _load_base_config(project_dir, "oss-datasource.json")
    global_cfg = _load_json_silently(Path(project_dir) / f"setting-{env}.json")
    oss_global = global_cfg.get("datasource", {}).get("oss", {})
    
    if not oss_global:
        return config

    if "name" in oss_global:
        config["name"] = oss_global["name"]
    if "bucket" in oss_global:
        config["bucket"] = oss_global["bucket"]
    if "endpoint" in oss_global:
        config["endpoint"] = oss_global["endpoint"]
    
    return config


def load_merged_mc_ds_config(project_dir: str, env: str = "dev") -> Dict[str, Any]:
    """
    加载 MaxCompute 数据源配置（用于 create_mc_ds.py）：
    读取 maxcompute-datasource.json 底板，并将 setting-<env>.json 中 [mc_datasource] 的值覆盖上去。
    """
    config = _load_base_config(project_dir, "maxcompute-datasource.json")
    global_cfg = _load_json_silently(Path(project_dir) / f"setting-{env}.json")
    mc_global = global_cfg.get("datasource", {}).get("mc", {})
    
    if not mc_global:
        return config

    if "name" in mc_global:
        config["name"] = mc_global["name"]
    if "project" in mc_global:
        config["project"] = mc_global["project"]
    if "endpoint" in mc_global:
        config["endpoint"] = mc_global["endpoint"]
        
    return config


def load_merged_upstream_config(project_dir: str, env: str = "dev") -> Dict[str, Any]:
    """
    加载上游赋值节点（CONTROLLER_ASSIGNMENT）的完整合并配置。

    合并顺序（优先级由低到高）：
      1. upstream-node-config.json   — 稳定系统参数（commandTypeId、resource_group 等）
      2. integration-config.json     — 共享的 owner / resource_group / metadata（可覆盖上层值）
      3. setting-<env>.json          — 环境专属参数（node_name、cron、reader_prefix、OSS 配置）

    返回值为扁平化 dict，调用方直接用 config["key"] 读取。

    主要字段：
      node_name, cron, reader_prefix   — 来自 setting-<env>.json task 层
      oss_bucket, oss_endpoint         — 来自 setting-<env>.json datasource.oss 层
      owner, resource_group            — 来自 integration-config.json（被 upstream-node-config.json 覆盖）
      project_id, project_identifier   — 来自 integration-config.json metadata
      commandTypeId, cu, language …    — 来自 upstream-node-config.json
    """
    # ── 第一层：上游节点稳定底板 ─────────────────────────────────────
    upstream_base = _load_base_config(project_dir, "upstream-node-config.json")

    # ── 第二层：从 integration-config.json 读取共享参数 ──────────────
    integration_cfg = _load_base_config(project_dir, "integration-config.json")
    for shared_key in ("owner", "resource_group", "resourceGroupId", "resourceGroupName"):
        if shared_key in integration_cfg:
            upstream_base.setdefault(shared_key, integration_cfg[shared_key])

    # 提取 project_id / project_identifier（从 metadata）
    metadata = integration_cfg.get("metadata", {})
    proj = metadata.get("project", {})
    upstream_base["project_id"] = str(
        proj.get("projectId") or metadata.get("projectId") or ""
    )
    upstream_base["project_identifier"] = str(
        proj.get("projectIdentifier") or metadata.get("projectIdentifier") or ""
    )

    # ── 第三层：setting-<env>.json 环境覆写 ─────────────────────────
    setting = _load_json_silently(Path(project_dir) / f"setting-{env}.json")
    task_cfg = setting.get("task", {})
    oss_cfg  = setting.get("datasource", {}).get("oss", {})

    # task 层覆写
    for key in ("node_name", "upstream_node_name", "cron", "reader_prefix"):
        if key in task_cfg:
            upstream_base[key] = task_cfg[key]

    # OSS 层覆写（扁平化, 前缀 oss_ 以区分其他字段）
    if "bucket" in oss_cfg:
        upstream_base["oss_bucket"] = oss_cfg["bucket"]
    if "endpoint" in oss_cfg:
        upstream_base["oss_endpoint"] = oss_cfg["endpoint"]

    # 去除内嵌注释字段（以 _comment 开头的键）
    return {k: v for k, v in upstream_base.items() if not k.startswith("_comment")}


def load_merged_downstream_config(project_dir: str, env: str = "dev") -> Dict[str, Any]:
    """
    加载下游 Python 节点（PYTHON）的完整合并配置。

    合并顺序（优先级由低到高）：
      1. downstream-node-config.json  — 稳定系统参数（commandTypeId=1322 / cu / resource_group 等）
      2. integration-config.json      — 共享的 owner / resource_group / project metadata
      3. setting-<env>.json           — 环境专属参数（node_name、cron、OSS bucket/endpoint）

    返回值为扁平化 dict，调用方直接用 config["key"] 读取。

    主要字段（除通用调度字段外）：
      node_name           — 节点基准名（脚本拼接 _downstream 后缀）
      cron                — 调度 cron 表达式
      oss_bucket          — OSS Bucket 名称
      oss_endpoint        — OSS Endpoint 地址
      project_id          — DataWorks 工作空间 ID（数字字符串）
      project_identifier  — DataWorks 工作空间标识符（用于构造 flow.depends.output）
      commandTypeId       — 1322（DataWorks Python3 节点类型）
      language            — "python3"
      command             — "PYTHON"
    """
    # ── 第一层：下游节点稳定底板 ─────────────────────────────────────
    base = _load_base_config(project_dir, "downstream-node-config.json")

    # ── 第二层：从 integration-config.json 读取共享参数 ──────────────
    integration_cfg = _load_base_config(project_dir, "integration-config.json")
    for shared_key in ("owner", "resource_group", "resourceGroupId", "resourceGroupName"):
        if shared_key in integration_cfg:
            base.setdefault(shared_key, integration_cfg[shared_key])

    # 提取 project_id / project_identifier（downstream 需要 identifier 构造 flow.depends）
    metadata = integration_cfg.get("metadata", {})
    proj = metadata.get("project", {})
    base["project_id"] = str(
        proj.get("projectId") or metadata.get("projectId") or ""
    )
    base["project_identifier"] = str(
        proj.get("projectIdentifier") or metadata.get("projectIdentifier") or ""
    )

    # ── 第三层：setting-<env>.json 环境覆写 ─────────────────────────
    setting = _load_json_silently(Path(project_dir) / f"setting-{env}.json")
    task_cfg = setting.get("task", {})
    oss_cfg  = setting.get("datasource", {}).get("oss", {})

    # task 层覆写
    for key in ("node_name", "downstream_node_name", "upstream_node_name", "cron"):
        if key in task_cfg:
            base[key] = task_cfg[key]

    # OSS 层覆写（扁平化，前缀 oss_ 以区分）
    if "bucket" in oss_cfg:
        base["oss_bucket"] = oss_cfg["bucket"]
    if "endpoint" in oss_cfg:
        base["oss_endpoint"] = oss_cfg["endpoint"]

    # 去除注释字段
    return {k: v for k, v in base.items() if not k.startswith("_comment")}
