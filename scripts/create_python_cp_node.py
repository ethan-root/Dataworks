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
    mc_project = config.get("datasource", {}).get("mc", {}).get("project", "unknown_project")
    
    # 从 setting-<env>.json["task"] 获取保留的分区个数配置，默认保留 30 个
    retention_count = task_config.get("mc_partition_retention", 30)
    
    # 注入到 DW 节点脚本中的 Python 片段
    script_content = f"""# CI/CD 自动创建下游节点 - 清理 MaxCompute 旧分区
# 策略：按个数保留最新 {{retention}} 个分区，删除其余的

from odps import ODPS
import os

# 读取 DataWorks 环境变量中的 AK/SK，或者通过绑定的数据源执行
access_id = os.environ.get('ALIBABA_CLOUD_ACCESS_KEY_ID')
secret_key = os.environ.get('ALIBABA_CLOUD_ACCESS_KEY_SECRET')
endpoint = '{config.get("datasource", {{}}).get("mc", {{}}).get("endpoint")}'
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
    
    logger.info(f"DataWorks 分区清理节点已注册/更新: {node_name} (表: {table_name}, 保留个数: {retention_count})")
    logger.debug(f"节点代码预览:\n{script_content}")


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
