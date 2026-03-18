# -*- coding: utf-8 -*-
"""
check_integration_node.py
职责：检查集成节点是否已在远端存在。
"""

import argparse
import sys
import os
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from dataworks_client import create_client, get_node_id
from config_merger import load_merged_node_config

def main():
    parser = argparse.ArgumentParser(description="Check DataWorks Integration Node")
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

    try:
        # 1. 加载合并后配置（包含 global.json 注入的节点名称）
        config = load_merged_node_config(args.project_dir, args.env)
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    node_name = config.get("node_name")
    if not node_name:
        print("ERROR: node_name not found in configuration.")
        sys.exit(1)

    print(f"Checking node '{node_name}' in Project {project_id}...")
    client = create_client()
    
    ds_node_id = get_node_id(client, project_id, node_name)
    if ds_node_id:
        print(f"✅ Node '{node_name}' exists. Node ID: {ds_node_id}")
    else:
        print(f"❌ Node '{node_name}' does not exist.")
        sys.exit(1)

if __name__ == "__main__":
    main()
