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


def process_project(client, project_id: int, project_dir: str) -> None:
    """
    以 Upsert 方式处理单个功能目录：节点存在则更新，不存在则创建。

    Args:
        client:      DataWorks SDK 客户端（由 deploy.py 传入）
        project_id:  DataWorks 工作空间 ID
        project_dir: 功能目录路径，如 "feature/test-feature"
                     该目录下必须有 task-config.json 文件
    """
    # ── 第一步：读取配置 ────────────────────────────────────────
    config_path = Path(project_dir) / "task-config.json"
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    node_name = config["node_name"]
    print(f"\n{'='*50}")
    print(f"Processing: {node_name}  (dir: {project_dir})")
    print(f"{'='*50}")

    # ── 第二步：查询节点是否已存在 ─────────────────────────────
    # 用 file_id 判断节点是否存在（node_id 只有提交发布后才有值，可能为 None）
    file_id, node_id = get_node_id(client, project_id, node_name)

    # ── 第三步：Upsert ─────────────────────────────────────────
    if file_id:
        # 节点已存在（FileId 有值）
        if node_id:
            # 已发布到调度系统 → 可以 UpdateNode
            print(f"[UPDATE] Node '{node_name}' exists and published (NodeId={node_id}). Updating...")
            update_node(client, project_id, node_id, config)
        else:
            # 节点文件已创建但尚未提交/发布（NodeId=None）→ 跳过更新
            print(f"[SKIP]   Node '{node_name}' exists (FileId={file_id}) but not yet submitted (NodeId=None).")
            print(f"         Please submit/publish the node in DataWorks Studio first, then re-run to update.")
    else:
        # 节点不存在 → 创建
        print(f"[CREATE] Node '{node_name}' not found. Creating new node...")
        create_node(client, config, project_id)

    print(f"Done: {node_name}\n")
