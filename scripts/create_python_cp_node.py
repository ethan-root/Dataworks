# -*- coding: utf-8 -*-
"""
create_python_cp_node.py
职责：创建下游 DataWorks Python 节点（MaxCompute Data Delete 节点）。
用途：清理 MaxCompute 分区表，按照业务设置保留最新的 N 个分区，删除其余的旧分区。
"""

import os
import sys
import json
import logging
import argparse
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from config_merger import load_merged_node_config
import dataworks_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def create_dw_delete_node(config, project_dir, env):
    """
    创建 DataWorks 删除分区节点
    """
    client = dataworks_client.create_client()
    task_config = config.get("task", {})
    
    project_id = config.get("metadata", {}).get("projectId")
    if not project_id:
        logger.error("projectId not found in config.metadata")
        sys.exit(1)
        
    node_name = task_config.get("node_name", "delete_node") + "_delete_partitions"
    
    # 获取需要清理的表名和要保留的个数
    writer_config = config.get("writer", {})
    table_name = writer_config.get("table", "unknown_table")
    mc_config = config.get("datasource", {}).get("mc", {})
    mc_project = mc_config.get("project", "unknown_project")
    mc_endpoint = mc_config.get("endpoint", "")
    
    # 从 setting-<env>.json["task"] 获取保留的分区个数配置，默认保留 30 个
    retention_count = task_config.get("mc_partition_retention", 30)
    
    # 注入到 DW 节点脚本中的 Python 片段
    script_content = f"""# CI/CD 自动创建下游节点 - 清理 MaxCompute 旧分区
# 策略：按个数保留最新 {retention_count} 个分区，删除其余的

from odps import ODPS
import os

# 读取 DataWorks 环境变量中的 AK/SK，或者通过绑定的数据源执行
access_id = os.environ.get('ALIBABA_CLOUD_ACCESS_KEY_ID')
secret_key = os.environ.get('ALIBABA_CLOUD_ACCESS_KEY_SECRET')
endpoint = '{mc_endpoint}'
project_name = '{mc_project}'
table_name = '{table_name}'
retention_count = {retention_count}

if not access_id or not secret_key:
    # 备选如果是在 DataWorks 内置 PyODPS 节点中，odps 对象已经全局可用，可以省去 AK/SK 显式初始化
    # 此处假设用户使用了独立 Python 节点，需要自己初始化
    print("Warning: AK/SK not found in env, assuming builtin ODPS environment.")
else:
    o = ODPS(access_id, secret_key, project_name, endpoint)

try:
    t = o.get_table(table_name)
    partitions = list(t.partitions)
    
    if not partitions:
        print(f"Table {{table_name}} has no partitions.")
    else:
        # 按名称（通常含有时间戳等）排序，降序排列，最新在前面
        sorted_partitions = sorted(partitions, key=lambda p: str(p.name), reverse=True)
        
        print(f"Total partitions: {{len(sorted_partitions)}}. Retention: {{retention_count}}")
        
        if len(sorted_partitions) > retention_count:
            to_delete = sorted_partitions[retention_count:]
            print(f"Need to delete {{len(to_delete)}} old partitions.")
            
            for p in to_delete:
                print(f"Dropping partition: {{p.name}}")
                p.drop()
            print("Cleanup finished!")
        else:
            print("No old partitions to delete.")
            
except Exception as e:
    print(f"Cleanup failed: {{e}}")
    raise e
"""
    
    base_node_name = task_config.get("node_name", "delete_node")
    downstream_node_name = f"{base_node_name}_downstream"
    integration_node_name = base_node_name
    
    downstream_node_id = dataworks_client.get_node_id(client, project_id, downstream_node_name)
    integration_node_id = dataworks_client.get_node_id(client, project_id, integration_node_name)
    
    depends_list = []
    if downstream_node_id:
        depends_list.append({
            "type": "Normal",
            "output": str(downstream_node_id),
            "sourceType": "Manual",
            "refTableName": downstream_node_name
        })
    if integration_node_id:
        depends_list.append({
            "type": "Normal",
            "output": str(integration_node_id),
            "sourceType": "Manual",
            "refTableName": integration_node_name
        })
        
    # 如果都没找到，兜底使用 root
    if not depends_list:
        root_output = f"{config.get('metadata', {}).get('projectIdentifier', '')}_root"
        if root_output == "_root":
            root_output = f"{config.get('metadata', {}).get('projectId', '')}_root"
        depends_list.append({
            "type": "Normal",
            "output": root_output,
            "sourceType": "Manual"
        })
        
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
                        "language": "python3",
                        "runtime": {
                            "command": "PYTHON",
                            "commandTypeId": 1322,
                            "cu": "0.5"
                        },
                        "content": script_content
                    },
                    "trigger": {
                        "type": "Scheduler",
                        "cron": task_config.get("cron", "00 00 00-23/1 * * ?"),
                        "cycleType": "NotDaily",
                        "startTime": "1970-01-01 00:00:00",
                        "endTime": "9999-01-01 00:00:00",
                        "timezone": "Asia/Shanghai",
                        "delaySeconds": 0
                    },
                    "runtimeResource": {
                        "resourceGroup": config.get("resource_group", "")
                    },
                    "name": node_name,
                    "owner": config.get("owner", "")
                }
            ],
            "flow": [
                {
                    "depends": depends_list
                }
            ]
        }
    }

    spec_json = json.dumps(spec_dict, ensure_ascii=False)
    
    ds_file_id = dataworks_client.get_node_id(client, project_id, node_name)
    if ds_file_id:
        logger.info(f"[UPDATE] Delete Node '{node_name}' already exists. Updating...")
        dataworks_client.update_node(client, project_id, ds_file_id, spec_dict)
    else:
        logger.info(f"[CREATE] Delete Node '{node_name}' not found. Creating new node...")
        from alibabacloud_dataworks_public20240518 import models as dw_models
        from alibabacloud_tea_util import models as util_models
        create_node_request = dw_models.CreateNodeRequest(
            project_id=project_id,
            spec=spec_json,
            scene='DATAWORKS_PROJECT'
        )
        runtime = util_models.RuntimeOptions()
        try:
            client.create_node_with_options(create_node_request, runtime)
            logger.info("✓ 创建成功！")
        except Exception as error:
            logger.error(f"✗ 创建失败: {error}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
            
    logger.info(f"DataWorks 分区清理节点已注册/更新: {node_name} (表: {table_name}, 保留个数: {retention_count})")

def main():
    parser = argparse.ArgumentParser(description="Create DataWorks Partition Delete Node")
    parser.add_argument(
        "--project-dir", type=str, required=True,
        help="项目目录路径"
    )
    parser.add_argument(
        "--env", type=str, required=True,
        help="环境名称"
    )
    # 兼容原有的 --node-type 参数，虽然现在此脚本只做 delete
    parser.add_argument(
        "--node-type", type=str, default="delete",
        help="节点类型 (兼容旧参)"
    )
    args = parser.parse_args()

    logger.info(f"Processing create_python_cp_node.py (Delete Node)")
    
    if args.node_type == "cp":
        logger.error("Error: --node-type cp is deprecated. Use get_earliest_parquet.py / create_upstream_node.py instead.")
        sys.exit(1)
    
    config = load_merged_node_config(args.project_dir, args.env)
    
    create_dw_delete_node(config, args.project_dir, args.env)


if __name__ == "__main__":
    main()
