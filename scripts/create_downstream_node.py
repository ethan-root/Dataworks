# -*- coding: utf-8 -*-
"""
create_downstream_node.py

职责：
  在 DataWorks 中创建（或更新）下游 Python 节点（PYTHON）。
  该节点在数据集成完成后触发，将 OSS 中已同步完毕的 Parquet 文件
  移动到 completed/ 目录。

数据流（节点执行链）：
  上游赋值节点 (_upstream) → 数据集成节点 (DI) → 本节点 (_downstream)
  ↑ 获取文件名               ↑ 同步入库               ↑ 移动已同步文件

配置来源（合并优先级由低到高）：
  1. default-setting/downstream-node-config.json — 稳定系统参数（commandTypeId=1322 / cu / resource_group 等）
  2. default-setting/integration-config.json     — 共享的 owner / resource_group / project metadata
  3. features/<name>/setting-<env>.json        — 环境专属参数（cron / node_name / OSS bucket 等）

本地调试：
  python scripts/create_downstream_node.py --project-dir features/user-feature --env dev

所需环境变量（CI 中由 GitHub Actions secrets 注入）：
  ALIBABA_CLOUD_ACCESS_KEY_ID      — 阿里云 AccessKey ID
  ALIBABA_CLOUD_ACCESS_KEY_SECRET  — 阿里云 AccessKey Secret
  DATAWORKS_PROJECT_ID             — DataWorks 工作空间数字 ID（可覆盖配置中的 project_id）
  ALIYUN_REGION                    — 阿里云地域（默认 cn-shanghai）
"""

import json
import logging
import os
import sys
import traceback
import argparse
from pathlib import Path

# ── 将 scripts/ 目录加入 sys.path，确保在任意 CWD 下均可正确引用同级模块 ──
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from config_merger import load_merged_downstream_config
from dataworks_client import create_client, get_node_id, update_node

from alibabacloud_dataworks_public20240518 import models as dw_models
from alibabacloud_tea_util import models as util_models

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────────────────────────────────────

def _get_env_or_fail(name: str) -> str:
    """读取必填环境变量，不存在时立即退出并报错。"""
    value = os.environ.get(name, "").strip()
    if not value:
        logger.error(f"必填环境变量未设置: '{name}'")
        sys.exit(1)
    return value


def _load_ref_script() -> str:
    """
    读取 move_parquet_to_completed.py 的核心逻辑（截断 main() 之前的部分）。
    使用绝对路径，不依赖 CWD。
    """
    # 确保使用绝对路径加载同一执行目录下的脚本，避免因不同执行路径产生引用失败
    ref_path = SCRIPT_DIR / "move_parquet_to_completed.py"
    if not ref_path.exists():
        logger.error(f"参考脚本不存在: {ref_path}")
        sys.exit(1)

    raw = ref_path.read_text(encoding="utf-8")
    # 截断 main() 和 handler() 测试代码，只保留核心函数定义和 import
    core = raw.split("def main():")[0].rstrip()
    return core


# ─────────────────────────────────────────────────────────────────────────────
# Spec 构建
# ─────────────────────────────────────────────────────────────────────────────

def build_downstream_node_spec(node_config: dict, ak: str, sk: str, upstream_node_id: int, integration_node_id: int) -> tuple:
    """
    构建下游 Python 节点的 DataWorks FlowSpec dict。

    节点依赖链设计：
      flow.depends.output = "{project_identifier}.{base_node_name}"
        → 等待数据集成节点（DI node）完成后才触发执行

      parameters[0].name="args", value="${base_node_name_upstream.outputs}"
        → 从赋值节点接收 Parquet 文件路径，通过 sys.argv[1] 传入脚本

    Args:
        node_config : 由 load_merged_downstream_config() 返回的扁平化配置 dict
        ak          : 阿里云 AccessKey ID（注入到节点脚本内容）
        sk          : 阿里云 AccessKey Secret（同上）
        upstream_node_id : 上游赋值节点的 Node ID 
        integration_node_id : 数据集成节点的 Node ID

    Returns:
        (spec_dict, node_name) 元组
    """
    # 从合并好的扁平字典中清晰读取各个维度的变量值
    base_node_name  = node_config.get("node_name", "downstream_node")
    node_name       = node_config.get("downstream_node_name", base_node_name + "_downstream")
    cron_expr       = node_config.get("cron",             "00 00 00-23/1 * * ?")
    resource_group  = node_config.get("resource_group",   "")
    owner_id        = node_config.get("owner",            "")
    oss_bucket      = node_config.get("oss_bucket",       "")
    oss_endpoint    = node_config.get("oss_endpoint",     "")
    project_identifier = node_config.get("project_identifier", "")
    project_id_str  = node_config.get("project_id",       "")
    command_type_id = int(node_config.get("commandTypeId", 1322))
    cu              = str(node_config.get("cu",           "0.5"))
    language        = node_config.get("language",         "python3")

    # ── 构建 flow.depends.output ──────────────────────────────────────
    if not upstream_node_id or not integration_node_id:
        logger.error("上游赋值节点或数据集成节点未创建，无法建立依赖。请先执行之前的 CI 步骤。")
        sys.exit(1)

    # 上游赋值节点的参数引用（文件名传递）
    upstream_assignment_node = node_config.get("upstream_node_name", base_node_name + "_upstream")
    file_path_param_value    = f"${{{upstream_assignment_node}.outputs}}"

    # ── 构建节点执行脚本内容 ─────────────────────────────────────────
    core_logic = _load_ref_script()

    node_script_content = core_logic + f"""


if __name__ == '__main__':
    import sys as _sys

    # OSS 连接配置（部署时由 CI/CD 注入，非密钥信息）
    _endpoint = '{oss_endpoint}'
    _bucket   = '{oss_bucket}'

    # AK/SK 由 CI/CD 在部署时注入到脚本内容
    # TODO: 后续可迁移为 DataWorks RAM 角色认证，彻底消除密钥注入
    _ak = '{ak}'
    _sk = '{sk}'

    # 文件路径由上游赋值节点通过 DataWorks 参数机制传入（sys.argv[1]）
    _file_path = ''
    if len(_sys.argv) > 1:
        _file_path = _sys.argv[1].strip()

    # 当未能从上游获取参数时主动退出，避免抛出底层执行异常
    if not _file_path:
        print('ERROR: 未接收到来自上游赋值节点的文件路径，停止执行！', file=_sys.stderr)
        _sys.exit(1)

    print(f'接收到文件路径: {{_file_path}}')
    result = move_to_completed(_ak, _sk, _endpoint, _bucket, _file_path)

    if result is None:
        print('ERROR: 文件移动失败（路径层级不足或 OSS 操作异常）', file=_sys.stderr)
        _sys.exit(1)
"""

    # ── 构建 DataWorks FlowSpec dict ─────────────────────────────────
    spec_dict = {
        "version": "1.1.0",
        "kind": "CycleWorkflow",
        "spec": {
            "nodes": [
                {
                    "recurrence":             node_config.get("recurrence", "Normal"),
                    "maxInternalConcurrency": int(node_config.get("maxInternalConcurrency", 0)),
                    "timeout":                int(node_config.get("timeout", 0)),
                    "timeoutUnit":            node_config.get("timeoutUnit", "HOURS"),
                    "instanceMode":           node_config.get("instanceMode", "Immediately"),
                    "rerunMode":              node_config.get("rerunMode", "Allowed"),
                    "rerunTimes":             int(node_config.get("rerunTimes", 0)),
                    "rerunInterval":          int(node_config.get("rerunInterval", 180000)),
                    "script": {
                        "path":     node_name,
                        "language": language,
                        "runtime": {
                            "command":       "PYTHON",
                            "commandTypeId": command_type_id,
                            "cu":            cu,
                        },
                        "content": node_script_content,
                        # 接收上游赋值节点（_upstream）产出的文件名，通过 sys.argv[1] 传入脚本
                        "parameters": [
                            {
                                "artifactType": "Variable",
                                "name":         "-",   # 使用 DataWorks 约定的匿名参数符号 "-" 保证正确传递
                                "scope":        "NodeParameter",
                                "type":         "NoKvVariableExpression",
                                "value": f"${{{upstream_assignment_node}.outputs}}"
                            }
                        ],
                    },
                    "trigger": {
                        "type":         "Scheduler",
                        "cron":         cron_expr,
                        "cycleType":    node_config.get("cycleType", "NotDaily"),
                        "startTime":    node_config.get("startTime", "1970-01-01 00:00:00"),
                        "endTime":      node_config.get("endTime",   "9999-01-01 00:00:00"),
                        "timezone":     node_config.get("timezone",  "Asia/Shanghai"),
                        "delaySeconds": int(node_config.get("delaySeconds", 0)),
                    },
                    "runtimeResource": {
                        "resourceGroup": resource_group,
                    },
                    "name":  node_name,
                    "owner": owner_id,
                    "inputs": {
                        "variables": [
                            {
                                "artifactType": "Variable",
                                "inputName": "outputs",
                                "name": "outputs",
                                "scope": "NodeContext",
                                "type": "NodeOutput",
                                "value": "${outputs}",
                                "node": {
                                    "nodeId": str(upstream_node_id),
                                    "output": str(upstream_node_id),
                                    "refTableName": upstream_assignment_node
                                }
                            }
                        ]
                    }
                }
            ],
            # 下游节点依赖上游赋值节点和数据集成节点（DI node），确保链路完整
            "flow": [
                {
                    "depends": [
                        {
                            "type":         "Normal",
                            "output":       str(upstream_node_id),
                            "sourceType":   "Manual",
                            "refTableName": f"{base_node_name}_upstream"
                        },
                        {
                            "type":         "Normal",
                            "output":       str(integration_node_id),
                            "sourceType":   "Manual",
                            "refTableName": base_node_name
                        }
                    ]
                }
            ],
        },
    }

    return spec_dict, node_name


# ─────────────────────────────────────────────────────────────────────────────
# 创建 / 更新节点
# ─────────────────────────────────────────────────────────────────────────────

def create_dw_downstream_node(node_config: dict) -> None:
    """
    对 DataWorks 下游 Python 节点执行 upsert（存在则更新，不存在则创建）。

    Args:
        node_config : 由 load_merged_downstream_config() 返回的合并配置 dict
    """
    # ── 1. 获取运行时凭证 ─────────────────────────────────────────────
    ak = _get_env_or_fail("ALIBABA_CLOUD_ACCESS_KEY_ID")
    sk = _get_env_or_fail("ALIBABA_CLOUD_ACCESS_KEY_SECRET")

    # ── 2. 确定 project_id（环境变量优先，配置文件兜底）────────────────
    # 优先使用环境变量以便在多空间 CI 部署中灵活切换，同时保留配置文件为回滚保障
    project_id_str = (
        os.environ.get("DATAWORKS_PROJECT_ID", "").strip()
        or node_config.get("project_id", "")
    )
    if not project_id_str:
        logger.error("project_id 未配置：请设置环境变量 DATAWORKS_PROJECT_ID 或检查 integration-config.json")
        sys.exit(1)
    project_id = int(project_id_str)

    # ── 3. 初始化 DataWorks 客户端并获取前置节点 ID ───────────────────
    client = create_client()
    base_node_name = node_config.get("node_name", "downstream_node")
    upstream_node_name = node_config.get("upstream_node_name", f"{base_node_name}_upstream")
    upstream_node_id = get_node_id(client, project_id, upstream_node_name)
    integration_node_id = get_node_id(client, project_id, base_node_name)

    # ── 4. 构建 spec ─────────────────────────────────────────────────
    spec_dict, node_name = build_downstream_node_spec(node_config, ak, sk, upstream_node_id, integration_node_id)
    spec_json = json.dumps(spec_dict, ensure_ascii=False)

    logger.info(f"开始 upsert 下游节点: {node_name}  (project_id={project_id})")

    # ── 5. 检查节点是否已存在 ─────────────────────────────────────────
    ds_file_id = get_node_id(client, project_id, node_name)

    if ds_file_id:
        # ── UPDATE 分支 ──────────────────────────────────────────────
        logger.info(f"[UPDATE] 节点 '{node_name}' 已存在 (file_id={ds_file_id})，执行增量更新...")
        update_node(client, project_id, ds_file_id, spec_dict)

    else:
        # ── CREATE 分支 ──────────────────────────────────────────────
        logger.info(f"[CREATE] 节点 '{node_name}' 不存在，开始创建...")
        create_req = dw_models.CreateNodeRequest(
            project_id=project_id,
            spec=spec_json,
            scene="DATAWORKS_PROJECT",
        )
        runtime = util_models.RuntimeOptions()
        try:
            resp = client.create_node_with_options(create_req, runtime)
            logger.info("✓ 创建成功！")
            logger.info(json.dumps(resp.body.to_map(), indent=2, ensure_ascii=False))
        except Exception as error:
            logger.error(f"✗ 创建失败: {getattr(error, 'message', str(error))}")
            if hasattr(error, "data") and error.data:
                logger.error(f"  阿里云建议: {error.data.get('Recommend', '')}")
            traceback.print_exc()
            # 遇到致命错误时通知操作环境立刻终止构建流程
            sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# 入口
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="创建或更新 DataWorks 下游 Python 节点（Parquet 文件移动）")
    parser.add_argument("--project-dir", required=True,
                        help="Feature 项目目录，如 features/user-feature")
    parser.add_argument("--env", required=True,
                        help="部署环境，如 dev / qa / preprod / prod")
    args = parser.parse_args()

    # 加载三层合并配置
    node_config = load_merged_downstream_config(args.project_dir, args.env)

    logger.info("=== 合并后配置（脱敏）===")
    for k, v in node_config.items():
        logger.info(f"  {k}: {v}")
    logger.info("=" * 40)

    create_dw_downstream_node(node_config)


if __name__ == "__main__":
    main()
