# -*- coding: utf-8 -*-
"""
process_project.py
职责：读取单个功能目录的 task-config.json，以 Upsert 方式创建或更新 DataWorks 节点。

Upsert 流程：
  1. 读取 task-config.json
  2. 调用 get_node_id() 精确查找节点是否已存在
  3a. 节点存在 → 调用 update_node() 增量更新
  3b. 节点不存在 → 调用 create_node() 创建新节点
"""

import json
from pathlib import Path

from dataworks_client import create_node, get_node_id, update_node
from config_merger import load_merged_node_config

def process_project(client, project_id: int, project_dir: str, env: str) -> None:
    """
    以 Upsert 方式处理单个功能目录：节点存在则更新，不存在则创建。
    （保留此函数用于向后兼容 deploy.py）
    """
    # 1. 加载合并后配置
    config = load_merged_node_config(project_dir, env)
    node_name = config["node_name"]
    
    upstream_node_name = config.get("upstream_node_name", f"{node_name}_upstream")
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

    print(f"\n{'='*50}")
    print(f"Processing (Upsert): {node_name}  (dir: {project_dir})")
    print(f"{'='*50}")

    ds_node_id = get_node_id(client, project_id, node_name)
    if ds_node_id:
        print(f"[UPDATE] Node '{node_name}' already exists (NodeId={ds_node_id}). Updating...")
        update_node(client, project_id, ds_node_id, config)
    else:
        print(f"[CREATE] Node '{node_name}' not found. Creating new node...")
        create_node(client, config, project_id)
    print(f"Done: {node_name}\n")


def create_project(client, project_id: int, project_dir: str, env: str) -> None:
    """仅创建逻辑"""
    config = load_merged_node_config(project_dir, env)
    node_name = config["node_name"]
    
    upstream_node_name = config.get("upstream_node_name", f"{node_name}_upstream")
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
    print(f"Creating Node: {node_name}  (dir: {project_dir})")
    print(f"{'='*50}")

    ds_node_id = get_node_id(client, project_id, node_name)
    if ds_node_id:
        print(f"[ERROR] Node '{node_name}' already exists (NodeId={ds_node_id}). Use update_integration_node.py to update.")
        import sys
        sys.exit(1)
    else:
        print(f"[CREATE] Node '{node_name}' not found. Creating new node...")
        create_node(client, config, project_id)
    print(f"Done: {node_name}\n")


if __name__ == "__main__":
    import argparse
    import os
    import sys
    from dataworks_client import create_client

    parser = argparse.ArgumentParser(description="Create DataWorks Integration Node")
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
    create_project(client, project_id, args.project_dir, args.env)

