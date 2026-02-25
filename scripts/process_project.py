# -*- coding: utf-8 -*-
"""
process_project.py

读取项目目录下的 config.json，调用 create_node。
"""

import json
from pathlib import Path

from dataworks_client import create_node


def process_project(client, project_id: int, project_dir: str) -> None:
    config_path = Path(project_dir) / "config.json"
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    node_name = config["node_name"]
    print(f"Creating node: {node_name}")
    create_node(client, config, project_id)
    print(f"Done: {node_name}")
