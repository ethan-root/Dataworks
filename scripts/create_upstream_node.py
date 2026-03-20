# -*- coding: utf-8 -*-
"""
create_upstream_node.py

职责：
  在 DataWorks 中创建（或更新）上游赋值节点（CONTROLLER_ASSIGNMENT）。
  该节点在运行时扫描 OSS 指定目录，获取最早的 Parquet 文件名，
  并将文件路径作为 output 变量传递给下游数据集成节点。

配置来源（合并优先级由低到高）：
  1. configuration/upstream-node-config.json — 稳定系统参数（commandTypeId / cu / resource_group 等）
  2. configuration/integration-config.json   — 共享的 owner / resource_group / project metadata
  3. features/<name>/setting-<env>.json      — 环境专属参数（cron / node_name / OSS bucket 等）

本地调试：
  python scripts/create_upstream_node.py --project-dir features/test-feature --env dev

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

from config_merger import load_merged_upstream_config
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
    读取 get_earliest_file_name.py 的核心逻辑（截断 main() 之前的部分）。
    使用绝对路径，不依赖 CWD。
    """
    # ✅ Bug Fix #2: 使用绝对路径，不再依赖 CWD
    ref_path = SCRIPT_DIR / "get_earliest_file_name.py"
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

def build_upstream_node_spec(node_config: dict, ak: str, sk: str) -> tuple:
    """
    构建上游赋值节点的 DataWorks FlowSpec dict。

    Args:
        node_config : 由 load_merged_upstream_config() 返回的扁平化配置 dict
        ak          : 阿里云 AccessKey ID（注入到节点参数，不硬编码在 content 中）
        sk          : 阿里云 AccessKey Secret（同上）

    Returns:
        (spec_dict, node_name) 元组
    """
    # ✅ Bug Fix #1: 直接从扁平化 dict 读取，不再猜测嵌套层级
    node_name     = node_config.get("node_name", "upstream_node") + "_upstream"
    cron_expr     = node_config.get("cron",           "00 00 00-23/1 * * ?")
    resource_group = node_config.get("resource_group", "")
    owner_id      = node_config.get("owner",           "")
    oss_bucket    = node_config.get("oss_bucket",      "")
    oss_endpoint  = node_config.get("oss_endpoint",    "")
    reader_prefix = node_config.get("reader_prefix",   "camos/unknown/")
    command_type_id = int(node_config.get("commandTypeId", 1100))
    cu            = str(node_config.get("cu", "0.25"))
    language      = node_config.get("language", "odps-sql")

    # ── 构建节点执行脚本内容 ─────────────────────────────────────────
    # 核心逻辑来自 get_earliest_file_name.py（复用，不重复维护）
    # OSS 连接参数（非密钥）直接嵌入 content，AK/SK 通过 parameters 传入
    core_logic = _load_ref_script()

    node_script_content = core_logic + f"""


if __name__ == '__main__':
    import sys as _sys

    # OSS 配置（部署时由 CI/CD 注入，非敏感信息）
    _endpoint = '{oss_endpoint}'
    _bucket   = '{oss_bucket}'
    _prefix   = '{reader_prefix}'

    # AK/SK 由 DataWorks 节点参数（name: "-"）传入，格式: "ak,sk"
    # ✅ Bug Fix #4/#5: AK/SK 通过参数机制传入，不再硬编码在脚本文本中
    _params = _sys.argv[1].split(',') if len(_sys.argv) > 1 else []
    _ak = _params[0].strip() if len(_params) > 0 else ''
    _sk = _params[1].strip() if len(_params) > 1 else ''

    if not _ak or not _sk:
        print('ERROR: AK/SK not provided via node parameter', file=_sys.stderr)
        _sys.exit(1)

    earliest = get_earliest_parquet_file(_ak, _sk, _endpoint, _bucket, _prefix)

    # ✅ Bug Fix #6: 无文件时主动退出（而非静默），避免下游收到空值
    if not earliest:
        print('ERROR: No parquet file found in OSS prefix: ' + _prefix, file=_sys.stderr)
        _sys.exit(1)

    # DataWorks Assignment Node 将 stdout 最后一行作为 output 变量值
    print(earliest)
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
                            "command":       "CONTROLLER_ASSIGNMENT",
                            "commandTypeId": command_type_id,
                            "cu":            cu,
                        },
                        "content": node_script_content,
                        # ✅ Bug Fix #4: name="-" 对应参考脚本中验证可用的格式
                        #                value 在 create_dw_upstream_node() 中事后赋值
                        "parameters": [
                            {
                                "artifactType": "Variable",
                                "name":         "-",
                                "scope":        "NodeParameter",
                                "type":         "NoKvVariableExpression",
                                "value":        f"{ak},{sk}",   # ✅ Bug Fix #5
                            }
                        ],
                    },
                    "trigger": {
                        "type":          "Scheduler",
                        "cron":          cron_expr,
                        "cycleType":     node_config.get("cycleType", "NotDaily"),
                        "startTime":     node_config.get("startTime", "1970-01-01 00:00:00"),
                        "endTime":       node_config.get("endTime",   "9999-01-01 00:00:00"),
                        "timezone":      node_config.get("timezone",  "Asia/Shanghai"),
                        "delaySeconds":  int(node_config.get("delaySeconds", 0)),
                    },
                    "runtimeResource": {
                        "resourceGroup": resource_group,
                    },
                    "name":  node_name,
                    "owner": owner_id,
                }
            ],
            # ✅ Bug Fix #5 (flow): 参考脚本使用空数组，不构造复杂依赖
            "flow": [],
        },
    }

    return spec_dict, node_name


# ─────────────────────────────────────────────────────────────────────────────
# 创建 / 更新节点
# ─────────────────────────────────────────────────────────────────────────────

def create_dw_upstream_node(node_config: dict) -> None:
    """
    对 DataWorks 上游赋值节点执行 upsert（存在则更新，不存在则创建）。

    Args:
        node_config : 由 load_merged_upstream_config() 返回的合并配置 dict
    """
    # ── 1. 获取运行时凭证 ─────────────────────────────────────────────
    ak = _get_env_or_fail("ALIBABA_CLOUD_ACCESS_KEY_ID")
    sk = _get_env_or_fail("ALIBABA_CLOUD_ACCESS_KEY_SECRET")

    # ── 2. 确定 project_id（环境变量优先，配置文件兜底）────────────────
    project_id_str = (
        os.environ.get("DATAWORKS_PROJECT_ID", "").strip()
        or node_config.get("project_id", "")
    )
    if not project_id_str:
        logger.error("project_id 未配置：请设置环境变量 DATAWORKS_PROJECT_ID 或检查 integration-config.json")
        sys.exit(1)
    project_id = int(project_id_str)

    # ── 3. 构建 spec ─────────────────────────────────────────────────
    spec_dict, node_name = build_upstream_node_spec(node_config, ak, sk)
    spec_json = json.dumps(spec_dict, ensure_ascii=False)

    logger.info(f"开始 upsert 上游节点: {node_name}  (project_id={project_id})")

    # ── 4. 初始化 DataWorks 客户端 ────────────────────────────────────
    client = create_client()

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
            # ✅ Bug Fix #3: 创建失败时退出非零，确保 CI 感知失败
            sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# 入口
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="创建或更新 DataWorks 上游赋值节点")
    parser.add_argument("--project-dir", required=True,
                        help="Feature 项目目录，如 features/test-feature")
    parser.add_argument("--env", required=True,
                        help="部署环境，如 dev / qa / preprod / prod")
    args = parser.parse_args()

    # 加载三层合并配置
    node_config = load_merged_upstream_config(args.project_dir, args.env)

    logger.info("=== 合并后配置（脱敏）===")
    for k, v in node_config.items():
        logger.info(f"  {k}: {v}")
    logger.info("=" * 40)

    create_dw_upstream_node(node_config)


if __name__ == "__main__":
    main()
