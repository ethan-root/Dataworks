# -*- coding: utf-8 -*-
"""
create_upstream_node.py

职责：
1. 读取 scrips_add/get_earliest_file_name.py 中的 Python 逻辑
2. 将环境变量和配置（AK/SK / endpoint / bucket / prefix）以硬编码形式注入 Python
3. 在 DataWorks 中创建赋值节点 (CONTROLLER_ASSIGNMENT)，该节点在运行时查询 OSS 获取最新文件名，并将结果传递给下游
"""

import os
import sys
import json
import logging
import argparse
from pathlib import Path

# 添加项目根目录到 sys.path 方便引用
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from config_merger import merge_config
from dataworks_client import create_client, get_node_id, update_node

# import DataWorks public SDK models
from alibabacloud_dataworks_public20240518 import models as dataworks_public_20240518_models
from alibabacloud_tea_util import models as util_models

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def get_env_or_fail(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        logger.error(f"Environment variable '{name}' is required.")
        sys.exit(1)
    return value


def build_upstream_node_spec(config, project_dir, env, ak, sk) -> dict:
    task_config = config.get("task", {})
    
    # 获取原始节点名并添加 _upstream 后缀
    node_name = task_config.get("node_name", "upstream_node") + "_upstream"
    
    oss_config = config.get("datasource", {}).get("oss", {})
    bucket = oss_config.get("bucket", "YOUR_BUCKET")
    endpoint = oss_config.get("endpoint", "YOUR_ENDPOINT")
    prefix = task_config.get("reader_prefix", "camos/unknown/")
    
    owner_id = config.get("owner", "")
    resource_group = config.get("resource_group", "")
    cron_expr = task_config.get("cron", "00 00 00-23/1 * * ?")
    
    # ---------------------------------------------------------
    # 读取参考脚本（scripts/get_earliest_file_name.py）
    # ---------------------------------------------------------
    ref_script_path = Path("scripts/get_earliest_file_name.py")
    if not ref_script_path.exists():
        logger.error(f"参考脚本不存在: {ref_script_path}")
        sys.exit(1)
        
    raw_content = ref_script_path.read_text(encoding="utf-8")
    
    # 截断掉末尾的 main() 和 handler() 测试代码，只保留核心的 get_earliest_parquet_file 类和 import
    core_logic = raw_content.split("def main():")[0]
    
    # 把 AK/SK 等环境配置通过 Python 字符串注入到执行代码末尾
    # DW 的 Assignment Node 会将 print 的最后一行作为 output 传递给下游
    executable_script = core_logic + f"""

if __name__ == '__main__':
    # CI/CD 自动注入配置
    ak = '{ak}'
    sk = '{sk}'
    endpoint = '{endpoint}'
    bucket = '{bucket}'
    prefix = '{prefix}'
    
    earliest = get_earliest_parquet_file(ak, sk, endpoint, bucket, prefix)
    if earliest:
        # DW assignment node outputs the print result
        print(earliest)
"""

    # 包装为 DW API Spec
    spec_dict = {
        "version": "1.1.0",
        "kind": "CycleWorkflow",
        "spec": {
            "nodes": [
                {
                    "recurrence": "Normal",
                    "maxInternalConcurrency": 0,
                    "timeout": 0,
                    "timeoutUnit": "HOURS",
                    "instanceMode": "Immediately",
                    "rerunMode": "Allowed",
                    "rerunTimes": 0,
                    "rerunInterval": 180000,
                    "script": {
                        "path": node_name,
                        "language": "odps-sql",   # 依据用户在 FC 成功测试的经验，保持一致
                        "runtime": {
                            "command": "CONTROLLER_ASSIGNMENT",
                            "commandTypeId": 1100,
                            "cu": "0.25"
                        },
                        "content": executable_script,
                        "parameters": [
                            {
                                "artifactType": "Variable",
                                "name": "outputs", # 给 assignment 显式配置一个 output parameter 规范
                                "scope": "NodeParameter",
                                "type": "NoKvVariableExpression",
                                "value": "${outputs}"
                            }
                        ]
                    },
                    "trigger": {
                        "type": "Scheduler",
                        "cron": cron_expr,
                        "cycleType": "NotDaily",
                        "startTime": "1970-01-01 00:00:00",
                        "endTime": "9999-01-01 00:00:00",
                        "timezone": "Asia/Shanghai",
                        "delaySeconds": 0
                    },
                    "runtimeResource": {
                        "resourceGroup": resource_group
                    },
                    "name": node_name,
                    "owner": owner_id,
                }
            ],
            "flow": []
        }
    }
    
    return spec_dict, node_name


def create_dw_upstream_node(config, project_dir, env):
    """
    创建 DataWorks 上游赋值节点
    """
    client = create_client()
    
    project_id = config.get("metadata", {}).get("projectId")
    if not project_id:
        logger.error("projectId not found in config.metadata")
        sys.exit(1)
        
    ak = get_env_or_fail("ALIBABA_CLOUD_ACCESS_KEY_ID")
    sk = get_env_or_fail("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
    
    spec_dict, node_name = build_upstream_node_spec(config, project_dir, env, ak, sk)
    spec_json = json.dumps(spec_dict, ensure_ascii=False)
    
    logger.info(f"Preparing to upsert UPSTREAM node: {node_name}")
    
    # 检查节点是否存在（通过 client + list_files 包装的 get_node_id 函数查 file_id）
    ds_file_id = get_node_id(client, project_id, node_name)
    
    if ds_file_id:
        logger.info(f"[UPDATE] Upstream Node '{node_name}' already exists. Updating...")
        # 调用封装好的 update_node 完成 diff 后更新
        update_node(client, project_id, ds_file_id, spec_dict)
    else:
        logger.info(f"[CREATE] Upstream Node '{node_name}' not found. Creating new node...")
        create_node_request = dataworks_public_20240518_models.CreateNodeRequest(
            project_id=project_id,
            spec=spec_json,
            scene='DATAWORKS_PROJECT'
        )
        runtime = util_models.RuntimeOptions()
        try:
            resp = client.create_node_with_options(create_node_request, runtime)
            logger.info("✓ 创建成功！")
        except Exception as error:
            logger.error(f"✗ 创建失败: {error}")
            import traceback
            traceback.print_exc()


def main():
    parser = argparse.ArgumentParser(description="Create Upstream Assignment Node")
    parser.add_argument("--project-dir", required=True, help="项目目录 (如 features/test-feature)")
    parser.add_argument("--env", required=True, help="环境名称 (如 dev, qa)")
    args = parser.parse_args()
    
    config = merge_config(args.project_dir, args.env)
    
    # 创建（或更新）DataWorks 上游节点
    create_dw_upstream_node(config, args.project_dir, args.env)


if __name__ == "__main__":
    main()
