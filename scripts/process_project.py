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

def process_project(client, project_id: int, project_dir: str) -> None:
    """
    以 Upsert 方式处理单个功能目录：节点存在则更新，不存在则创建。

    Args:
        client:      DataWorks SDK 客户端（由 deploy.py 传入）
        project_id:  DataWorks 工作空间 ID
        project_dir: 功能目录路径，如 "feature/test-feature"
                     该目录下必须有 task-config.json 文件
    """
    # ── 第一步：读取配置（已引入合并覆盖逻辑）───────────────────
    config = load_merged_node_config(project_dir)

    node_name = config["node_name"]
    print(f"\n{'='*50}")
    print(f"Processing: {node_name}  (dir: {project_dir})")
    print(f"{'='*50}")

    # ── 第二步：查询节点是否已存在 ─────────────────────────────
    # get_node_id 返回的是 Data Studio 节点 ID（即 API 中的 file_id）
    # 这个 ID 在节点创建后即存在，UpdateNode 也是使用此 ID
    ds_node_id = get_node_id(client, project_id, node_name)

    # ── 第三步：Upsert ─────────────────────────────────────────
    if ds_node_id:
        # 节点已存在 → diff + 更新
        print(f"[UPDATE] Node '{node_name}' already exists (NodeId={ds_node_id}). Updating...")
        update_node(client, project_id, ds_node_id, config)
    else:
        # 节点不存在 → 创建
        print(f"[CREATE] Node '{node_name}' not found. Creating new node...")
        create_node(client, config, project_id)

    print(f"Done: {node_name}\n")
