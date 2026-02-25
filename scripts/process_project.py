"""
process_project.py — 单项目处理逻辑
读取 config.json → 遍历 Tables → 检查/创建/提交/发布 Job
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
        client: DataWorksClient 实例
        project_dir: 项目目录路径 (包含 config.json)

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
    logger.info(f"Processing Project: {project_name} ({table_count} tables)")
    logger.info("=" * 65)

    stats = {"created": 0, "skipped": 0, "failed": 0}

    for i, table in enumerate(tables):
        table_name = table["Name"]
        job_name = f"{project_name}_{table_name}"

        logger.info("-" * 65)
        logger.info(f"[{i+1}/{table_count}] Processing: {job_name}")
        logger.info("-" * 65)

        try:
            # Step 1: 检查 Job 是否已存在
            if client.job_exists(job_name):
                logger.info(f"SKIP: Job '{job_name}' already exists.")
                stats["skipped"] += 1
                continue

            # Step 2: 确保数据源存在
            logger.info("Ensuring data sources exist ...")
            client.ensure_oss_datasource(config)
            client.ensure_odps_datasource(config)

            # Step 3: 生成 TaskContent 并创建同步任务
            logger.info("Generating task content ...")
            task_content = client.generate_task_content(config, i)
            logger.info(f"TaskContent: {json.dumps(task_content, indent=2)}")

            resource_group = config["ResourceGroupIdentifier"]
            file_id = client.create_sync_task(job_name, task_content, resource_group)

            if not file_id:
                logger.error(f"FAIL: Failed to create sync task '{job_name}'")
                stats["failed"] += 1
                continue

            # Step 4: 提交任务
            client.submit_file(file_id)

            # Step 5: 发布到生产
            client.deploy_file(file_id)

            logger.info(f"SUCCESS: Job '{job_name}' created and deployed (FileId: {file_id}).")
            stats["created"] += 1

        except Exception as e:
            logger.error(f"FAIL: Job '{job_name}' — {e}")
            stats["failed"] += 1
            continue

    logger.info("=" * 65)
    logger.info(f"Project '{project_name}' Summary: "
                f"Created={stats['created']} | Skipped={stats['skipped']} | Failed={stats['failed']}")
    logger.info("=" * 65)

    return stats
