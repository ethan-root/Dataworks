# -*- coding: utf-8 -*-
"""
config_merger.py
职责：统一管理 DataWorks 部署脚本的配置读取与合并。
      从目标环境目录读取 global.json（如果存在），提取易变业务参数，
      并动态覆盖/注入到底层环境模板 JSON 中，返回最终发往 API 的完整配置字典。
"""

import json
import re
from pathlib import Path

def _parse_all_columns_from_sqls(sql_dir: Path) -> list:
    """内部辅助：从 ddl 目录下所有的 .sql 文件中汇总提取出所有的字段名"""
    columns = []
    
    sql_files = sorted(list(sql_dir.glob("*.sql"))) if sql_dir.exists() else []
    
    for filepath in sql_files:
        try:
            content = filepath.read_text(encoding="utf-8")
            
            # 使用一个稍微通用的正则找出字段块：
            # 它可以匹配 `CREATE TABLE xxx (...)` 也可以匹配 `ALTER TABLE xxx ADD COLUMNS (...)`
            matches = re.finditer(r'(?:CREATE\s+TABLE|ADD\s+COLUMNS)[^(]*\((.*?)\)(?:\s*(?:COMMENT|PARTITIONED|;|\Z)|$)', content, re.IGNORECASE | re.DOTALL)
            
            for match in matches:
                cols_block = match.group(1)
                # 解析 cols_block 里的列
                for line in cols_block.split('\n'):
                    line = line.strip()
                    if not line or line.startswith('--') or line.startswith('//'):
                        continue
                        
                    # 尝试提取反引号的内容 `col_name`
                    col_match = re.search(r'`([^`]+)`', line)
                    if col_match:
                        col_name = col_match.group(1)
                        if col_name not in columns:
                            columns.append(col_name)
                    else:
                        # 获取该行的第一个单词
                        parts = line.split()
                        if parts:
                            col_name = parts[0].strip()
                            if col_name.lower() not in ('primary', 'key', 'unique') and col_name not in columns:
                                columns.append(col_name)
        except Exception as e:
            print(f"   [WARN] Failed to parse SQL columns from {filepath.name}: {e}")
            
    return columns

def _load_json_silently(filepath: Path) -> dict:
    """内部辅助：静默读取 json 文件，不存在或报错时返回空字典"""
    if not filepath.exists():
        return {}
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"   [WARN] Failed to load {filepath.name}: {e}")
        return {}

def _load_base_config(project_dir: str, template_name: str) -> dict:
    """内部辅助：向上遍历寻找 configuration 文件夹读取基础模板配置，不存在则抛出异常"""
    cfg_path = None
    current = Path(project_dir).resolve()
    
    # 向上寻找包含 configuration 的目录
    while current.parent != current:
        if (current / "configuration").exists():
            cfg_path = current / "configuration" / template_name
            break
        current = current.parent
        
    if not cfg_path or not cfg_path.exists():
        # 回退到当前工作目录下的 configuration
        cfg_path = Path.cwd() / "configuration" / template_name
        if not cfg_path.exists():
            raise FileNotFoundError(f"Missing required base template: {cfg_path}")
            
    with open(cfg_path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_merged_node_config(project_dir: str, env: str = "dev") -> dict:
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
            print(f"   [INFO] Auto-extracted {len(columns)} columns from create-table.sql for Data Integration Mapping.")

    return config


def load_merged_oss_ds_config(project_dir: str, env: str = "dev") -> dict:
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


def load_merged_mc_ds_config(project_dir: str, env: str = "dev") -> dict:
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
