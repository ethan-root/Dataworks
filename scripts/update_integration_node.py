# -*- coding: utf-8 -*-
"""
update_integration_node.py
职责：独立脚本，用于增量更新已存在的 DataWorks 数据集成节点。
"""

import argparse
import os
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from dataworks_client import create_client, get_node_id, update_node
from config_merger import load_merged_node_config

def update_project(client, project_id: int, project_dir: str, args) -> None: # 1. 加载合并后的目标配置
    config = load_merged_node_config(args.project_dir, args.env)
    node_name = config["node_name"]
    
    upstream_node_name = f"{node_name}_upstream"
    upstream_node_id = get_node_id(client, project_id, upstream_node_name)
    if upstream_node_id:
        config["depends"] = [{
            "type": "Normal",
            "output": str(upstream_node_id),
            "sourceType": "Manual",
            "refTableName": upstream_node_name
        }]
        config["inputs"] = {
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
                        "refTableName": upstream_node_name
                    }
                }
            ]
        }
    else:
        print(f"   [WARN] 未找到上游节点 '{upstream_node_name}'，已跳过数据依赖配置。")

    print(f"\n{'='*50}")
    print(f"Updating Node: {node_name}  (dir: {project_dir})")
    print(f"{'='*50}")

    ds_node_id = get_node_id(client, project_id, node_name)
    if ds_node_id:
        print(f"[UPDATE] Node '{node_name}' exists (NodeId={ds_node_id}). Updating...")
        update_node(client, project_id, ds_node_id, config)
    else:
        print(f"[ERROR] Node '{node_name}' not found. Use create_integration_node.py to create first.")
        sys.exit(1)
    print(f"Done: {node_name}\n")


def main():
    parser = argparse.ArgumentParser(description="Update DataWorks Integration Node")
    parser.add_argument(
        "--project-dir", type=str, required=True,
        help="项目目录路径"
    )
    parser.add_argument(
        "--env", type=str, required=True,
        help="环境名称"
    )
    args = parser.parse_args()

    project_id_str = os.environ.get("DATAWORKS_PROJECT_ID", "")
    if not project_id_str:
        print("ERROR: DATAWORKS_PROJECT_ID not set")
        sys.exit(1)
    project_id = int(project_id_str)

    client = create_client()
    update_project(client, project_id, args.project_dir, args)

if __name__ == "__main__":
    main()
