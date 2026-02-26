# -*- coding: utf-8 -*-
"""
process_project.py
职责：读取单个项目的 config.json，调用 DataWorks API 创建节点。

这个文件是 deploy.py 和 dataworks_client.py 之间的"桥梁"：
  deploy.py         → 找到项目目录
  process_project.py → 读取 config.json，传给 API
  dataworks_client.py → 实际调用 DataWorks API
"""

import json
from pathlib import Path

# 从同目录的 dataworks_client.py 导入 create_node 函数
from dataworks_client import create_node


def process_project(client, project_id: int, project_dir: str) -> None:
    """
    处理单个项目：读取配置并在 DataWorks 中创建对应的定时同步节点。

    Args:
        client:      DataWorks SDK 客户端（由 deploy.py 传入）
        project_id:  DataWorks 工作空间 ID
        project_dir: 项目目录路径，如 "projects/Test"
                     该目录下必须有 task-config.json 文件
    """
    # ── 第一步：读取项目配置 ───────────────────────────────────
    config_path = Path(project_dir) / "task-config.json"
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    # 从配置中取出节点名，用于日志打印
    node_name = config["node_name"]   # 例如 "github_test"

    # ── 第二步：调用 API 创建节点 ──────────────────────────────
    print(f"Creating node: {node_name}")
    create_node(client, config, project_id)   # 实际的 API 调用在 dataworks_client.py 中

    print(f"Done: {node_name}")
