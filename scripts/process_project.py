"""
process_project.py — 单项目处理逻辑

设计原则：数据源由 DataWorks 控制台预先配置，脚本只负责创建节点。
"""

import json
import logging
from pathlib import Path

from dataworks_client import DataWorksClient

logger = logging.getLogger(__name__)


def process_project(client: DataWorksClient, project_dir: str) -> dict:
    """
    处理单个品牌项目的所有 Table，为每个 Table 创建定时同步节点。

    Args:
        client:      DataWorksClient 实例
        project_dir: 项目目录（包含 config.json）

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

    logger.info("=" * 65)
    logger.info(f"Project: {project_name}  ({len(tables)} table(s))")
    logger.info("=" * 65)

    stats = {"created": 0, "skipped": 0, "failed": 0}

    for i, table in enumerate(tables):
        node_name = f"{project_name}_{table['Name']}"
        logger.info(f"[{i + 1}/{len(tables)}] {node_name}")

        try:
            node_id = client.create_node(node_name, config, i)
            logger.info(f"  ✅ Created (NodeId: {node_id})")
            stats["created"] += 1
        except Exception as e:
            err_str = str(e)
            # 节点已存在时跳过（非致命，继续处理下一个）
            if "AlreadyExists" in err_str or "already exists" in err_str.lower():
                logger.warning(f"  ⚠️  Node '{node_name}' already exists, skipping.")
                stats["skipped"] += 1
            else:
                logger.error(f"  ❌ Failed: {e}")
                stats["failed"] += 1

    logger.info("-" * 65)
    logger.info(
        f"Done: Created={stats['created']} | Skipped={stats['skipped']} | Failed={stats['failed']}"
    )
    return stats
