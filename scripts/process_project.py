"""
process_project.py — 单项目处理逻辑
读取 config.json → 遍历 Tables → 检查/创建节点

使用 2024-05-18 API，CreateNode 一步完成（无需 Submit + Deploy）
"""

import json
import logging
from pathlib import Path

from dataworks_client import DataWorksClient

logger = logging.getLogger(__name__)


def process_project(client: DataWorksClient, project_dir: str) -> dict:
    """
    处理单个品牌项目的所有 Table

    Args:
        client:      DataWorksClient 实例
        project_dir: 项目目录路径（包含 config.json）

    Returns:
        {"created": N, "skipped": N, "failed": N}
    """
    config_path = Path(project_dir) / "config.json"
    if not config_path.exists():
        logger.error(f"config.json not found in {project_dir}")
        return {"created": 0, "skipped": 0, "failed": 1}

    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    project_name = config["ProjectName"]
    tables = config["Tables"]
    table_count = len(tables)

    logger.info("=" * 65)
    logger.info(f"Processing Project: {project_name}  ({table_count} table(s))")
    logger.info("=" * 65)

    stats = {"created": 0, "skipped": 0, "failed": 0}

    for i, table in enumerate(tables):
        table_name = table["Name"]
        node_name = f"{project_name}_{table_name}"

        logger.info("-" * 65)
        logger.info(f"[{i + 1}/{table_count}] Node: {node_name}")
        logger.info("-" * 65)

        try:
            # Step 1: 检查节点是否已存在
            if client.node_exists(node_name):
                logger.info(f"SKIP: Node '{node_name}' already exists.")
                stats["skipped"] += 1
                continue

            # Step 2: 确保数据源存在
            client.ensure_oss_datasource(config)
            client.ensure_odps_datasource(config)

            # Step 3: 生成 spec 并创建节点（一步完成，立即生效）
            node_id = client.create_node(node_name, config, i)
            logger.info(f"SUCCESS: Node '{node_name}' created (NodeId: {node_id}).")
            stats["created"] += 1

        except Exception as e:
            logger.error(f"FAIL: Node '{node_name}' — {e}")
            stats["failed"] += 1
            continue

    logger.info("=" * 65)
    logger.info(
        f"Project '{project_name}' done | "
        f"Created={stats['created']} | Skipped={stats['skipped']} | Failed={stats['failed']}"
    )
    logger.info("=" * 65)

    return stats
