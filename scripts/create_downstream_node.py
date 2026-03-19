# -*- coding: utf-8 -*-
"""
create_downstream_node.py
职责：
1. 读取 scrips_add/move_parquet_to_completed.py 中的 Python 逻辑
2. 将环境变量和配置（AK/SK / endpoint / bucket）以硬编码形式注入 Python
3. 在 DataWorks 中创建下游节点 (PYTHON)，并通过 flow.depends 链接到数据集成节点
4. 将上游赋值节点的参数（文件名）传入作为参数
"""

import os
import sys
import json
import logging
import argparse
from pathlib import Path

# 添加项目跟目录到 sys.path 方便引用
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from config_merger import load_merged_node_config
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


def build_downstream_node_spec(config, project_dir, env, ak, sk) -> dict:
    task_config = config.get("task", {})
    
    # 获取原始节点名并添加 _downstream 后缀
    base_node_name = task_config.get("node_name", "integration_node")
    node_name = base_node_name + "_downstream"
    
    # 上游（数据集成节点）的 Output 标识符，通常用来做 depends 链接
    # （DW 数据集成节点的默认产出名称是 {projectIdentifier}.{node_name} )
    project_identifier = config.get("metadata", {}).get("projectIdentifier", "")
    if project_identifier:
        upstream_output_name = f"{project_identifier}.{base_node_name}"
    else:
        # Fallback 保底，依赖自身的 project_id 或者其他形式
        upstream_output_name = f"{config.get('metadata', {}).get('projectId')}.{base_node_name}"
    
    oss_config = config.get("datasource", {}).get("oss", {})
    bucket = oss_config.get("bucket", "YOUR_BUCKET")
    endpoint = oss_config.get("endpoint", "YOUR_ENDPOINT")
    
    owner_id = config.get("owner", "")
    resource_group = config.get("resource_group", "")
    cron_expr = task_config.get("cron", "00 00 00-23/1 * * ?")
    
    # ---------------------------------------------------------
    # 读取参考脚本（scripts/move_parquet_to_completed.py）
    # ---------------------------------------------------------
    ref_script_path = Path("scripts/move_parquet_to_completed.py")
    if not ref_script_path.exists():
        logger.error(f"参考脚本不存在: {ref_script_path}")
        sys.exit(1)
        
    raw_content = ref_script_path.read_text(encoding="utf-8")
    
    # 截断掉末尾的 main() 和 handler() 测试代码，只保留核心函数
    core_logic = raw_content.split("def main():")[0]
    
    # Python 节点的参数接收可以通过 sys.argv 取参数 1 (DataWorks Python 节点特有的形式)
    executable_script = core_logic + f"""

if __name__ == '__main__':
    import sys
    # CI/CD 自动注入配置
    ak = '{ak}'
    sk = '{sk}'
    endpoint = '{endpoint}'
    bucket = '{bucket}'
    
    # 从 DataWorks 节点的调度参数传递进来的文件名 (赋值节点产出)
    # 此处默认作为 Python 脚本的第一个传入参数，对应 parameters -> args
    file_path = ''
    if len(sys.argv) > 1:
        file_path = sys.argv[1].strip()
        
    if not file_path:
        print("未接收到传递的文件名，将停止执行！")
    else:
        print(f"接收到文件系统调度参数: {{file_path}}")
        move_to_completed(ak, sk, endpoint, bucket, file_path)
"""

    upstream_assignment_node_name = base_node_name + "_upstream"

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
                        "language": "python3",   # 用户提供的使用了 python3
                        "runtime": {
                            "command": "PYTHON",
                            "commandTypeId": 1322,
                            "cu": "0.5"
                        },
                        "content": executable_script,
                        "parameters": [
                            {
                                "artifactType": "Variable",
                                "name": "args",  # Python 节点的参数，作为 sys.argv[1] 传入
                                "scope": "NodeParameter",
                                "type": "NoKvVariableExpression",
                                # 动态绑定上游赋值节点的 output 名。
                                # 用户在集成节点和下游节点都可以通过调度变量引用赋值节点的产出。
                                "value": f"${{{upstream_assignment_node_name}.outputs}}"
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
            # 将依赖指向 Data Integration 节点，确保证据链：Upstream (获取) -> Integration (入库) -> Downstream (移动)
            "flow": [
                {
                    "depends": [
                        {
                            "type": "Normal",
                            "output": upstream_output_name,
                            "sourceType": "Manual"
                        }
                    ]
                }
            ]
        }
    }
    
    return spec_dict, node_name


def create_dw_downstream_node(config, project_dir, env):
    """
    创建 DataWorks 下游清理节点
    """
    client = create_client()
    
    project_id = config.get("metadata", {}).get("projectId")
    if not project_id:
        logger.error("projectId not found in config.metadata")
        sys.exit(1)
        
    ak = get_env_or_fail("ALIBABA_CLOUD_ACCESS_KEY_ID")
    sk = get_env_or_fail("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
    
    spec_dict, node_name = build_downstream_node_spec(config, project_dir, env, ak, sk)
    spec_json = json.dumps(spec_dict, ensure_ascii=False)
    
    logger.info(f"Preparing to upsert DOWNSTREAM node: {node_name}")
    
    # 检查节点是否存在
    ds_file_id = get_node_id(client, project_id, node_name)
    
    if ds_file_id:
        logger.info(f"[UPDATE] Downstream Node '{node_name}' already exists. Updating...")
        update_node(client, project_id, ds_file_id, spec_dict)
    else:
        logger.info(f"[CREATE] Downstream Node '{node_name}' not found. Creating new node...")
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
    parser = argparse.ArgumentParser(description="Create Downstream File Move Node")
    parser.add_argument("--project-dir", required=True, help="项目目录 (如 features/test-feature)")
    parser.add_argument("--env", required=True, help="环境名称 (如 dev, qa)")
    args = parser.parse_args()
    
    config = load_merged_node_config(args.project_dir, args.env)
    
    # 创建（或更新）DataWorks 下游节点
    create_dw_downstream_node(config, args.project_dir, args.env)


if __name__ == "__main__":
    main()
