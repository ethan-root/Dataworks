"""
deploy.py — 主入口
使用 DataWorks 2024-05-18 API 创建数据集成定时节点

用法:
    python scripts/deploy.py                                        # 处理所有项目
    python scripts/deploy.py --projects Gucci                       # 指定项目
    python scripts/deploy.py --projects Gucci,Balenciaga            # 多个项目
    python scripts/deploy.py --step check_cli                       # 验证连接
    python scripts/deploy.py --step check_datasources --project-dir projects/Test
    python scripts/deploy.py --step create_oss_ds   --project-dir projects/Test
    python scripts/deploy.py --step create_odps_ds  --project-dir projects/Test
    python scripts/deploy.py --step create_node     --project-dir projects/Test

环境变量:
    ALIBABA_CLOUD_ACCESS_KEY_ID     阿里云 AK ID
    ALIBABA_CLOUD_ACCESS_KEY_SECRET 阿里云 AK Secret
    ALIYUN_REGION                   区域 (如 cn-shanghai)
    DATAWORKS_PROJECT_ID            DataWorks 工作空间 ID
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from dataworks_client import DataWorksClient
from process_project import process_project

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# =========================================================================
# 工具函数
# =========================================================================

def get_env_or_fail(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        logger.error(f"Environment variable '{name}' is required.")
        sys.exit(1)
    return value


def create_client() -> DataWorksClient:
    return DataWorksClient(
        access_key_id=get_env_or_fail("ALIBABA_CLOUD_ACCESS_KEY_ID"),
        access_key_secret=get_env_or_fail("ALIBABA_CLOUD_ACCESS_KEY_SECRET"),
        region=get_env_or_fail("ALIYUN_REGION"),
        project_id=int(get_env_or_fail("DATAWORKS_PROJECT_ID")),
    )


def _load_config(project_dir: str) -> dict:
    config_path = Path(project_dir) / "config.json"
    if not config_path.exists():
        logger.error(f"config.json not found in {project_dir}")
        sys.exit(1)
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


# =========================================================================
# 分步测试命令
# =========================================================================

def step_check_cli(client: DataWorksClient):
    """验证 SDK 连接 & 列出资源组"""
    logger.info("=" * 60)
    logger.info("Testing DataWorks SDK connection ...")
    logger.info("=" * 60)
    groups = client.list_resource_groups()
    logger.info(f"✅ Connection OK — {len(groups)} resource group(s) found.")
    for g in groups:
        logger.info(f"  └ Identifier: {g.identifier}  Status: {g.status}")


def step_check_datasources(client: DataWorksClient, project_dir: str):
    """检查数据源是否存在"""
    config = _load_config(project_dir)
    oss_ds = config["OSS"]["DataSourceName"]
    odps_ds = config["MaxCompute"]["DataSourceName"]
    logger.info("=" * 60)
    logger.info("Checking DataSources ...")
    logger.info("=" * 60)
    oss_ok = client.datasource_exists(oss_ds)
    odps_ok = client.datasource_exists(odps_ds)
    logger.info(f"  OSS        '{oss_ds}':  {'✅ EXISTS' if oss_ok else '❌ NOT FOUND'}")
    logger.info(f"  MaxCompute '{odps_ds}': {'✅ EXISTS' if odps_ok else '❌ NOT FOUND'}")


def step_create_oss_ds(client: DataWorksClient, project_dir: str):
    """创建 OSS 数据源"""
    config = _load_config(project_dir)
    client.ensure_oss_datasource(config)


def step_create_odps_ds(client: DataWorksClient, project_dir: str):
    """创建 MaxCompute 数据源"""
    config = _load_config(project_dir)
    client.ensure_odps_datasource(config)


def step_create_node(client: DataWorksClient, project_dir: str):
    """创建数据集成定时节点（CreateNode，立即生效）"""
    config = _load_config(project_dir)
    project_name = config["ProjectName"]
    table_name = config["Tables"][0]["Name"]
    node_name = f"{project_name}_{table_name}"

    logger.info("=" * 60)
    logger.info(f"Creating Node: {node_name}")
    logger.info("=" * 60)

    # 打印 spec 供调试
    spec_json = client.build_node_spec(config, 0, node_name)
    logger.info(f"Node spec:\n{json.dumps(json.loads(spec_json), indent=2, ensure_ascii=False)}")

    node_id = client.create_node(node_name, config, 0)
    logger.info(f"✅ Node created! NodeId = {node_id}")

    # 输出给 GitHub Actions
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"node_id={node_id}\n")


# =========================================================================
# 全量部署
# =========================================================================

def deploy_all(client: DataWorksClient, projects_dir: str, target_projects: str = ""):
    projects_path = Path(projects_dir)

    logger.info("")
    logger.info("╔" + "═" * 60 + "╗")
    logger.info("║   DataWorks Data Integration — Deployment Tool            ║")
    logger.info("╠" + "═" * 60 + "╣")
    logger.info(f"║  Project ID:   {client.project_id}")
    logger.info(f"║  Projects Dir: {projects_path}")
    logger.info(f"║  Target:       {target_projects or 'ALL'}")
    logger.info("╚" + "═" * 60 + "╝")

    project_dirs = []
    if target_projects:
        for name in [n.strip() for n in target_projects.split(",") if n.strip()]:
            d = projects_path / name
            if not d.is_dir():
                logger.error(f"Project directory not found: {d}")
                sys.exit(1)
            project_dirs.append(str(d))
    else:
        project_dirs = [
            str(d) for d in sorted(projects_path.iterdir())
            if d.is_dir() and (d / "config.json").exists()
        ]

    if not project_dirs:
        logger.error(f"No project directories with config.json found in {projects_path}")
        sys.exit(1)

    logger.info(f"Found {len(project_dirs)} project(s).")

    total_ok, total_fail = 0, 0
    for project_dir in project_dirs:
        stats = process_project(client, project_dir)
        if stats["failed"] > 0:
            total_fail += 1
        else:
            total_ok += 1

    logger.info("")
    logger.info("╔" + "═" * 60 + "╗")
    logger.info("║                  Deployment Complete                      ║")
    logger.info("╠" + "═" * 60 + "╣")
    logger.info(f"║  Succeeded: {total_ok}  Failed: {total_fail}")
    logger.info("╚" + "═" * 60 + "╝")

    if total_fail > 0:
        logger.error("Some projects failed.")
        sys.exit(1)


# =========================================================================
# CLI
# =========================================================================

def main():
    parser = argparse.ArgumentParser(description="DataWorks Data Integration Deployment Tool")
    parser.add_argument("--projects", type=str, default="",
                        help="Comma-separated project names (default: all)")
    parser.add_argument("--projects-dir", type=str, default="projects",
                        help="Projects root directory (default: projects)")
    parser.add_argument("--project-dir", type=str, default="",
                        help="Single project directory (for step testing)")
    parser.add_argument("--step", type=str, default="",
                        choices=["", "check_cli", "check_datasources",
                                 "create_oss_ds", "create_odps_ds", "create_node"],
                        help="Execute a single step (for testing)")

    args = parser.parse_args()
    client = create_client()

    if args.step:
        pdir = args.project_dir or "projects/Test"
        if args.step == "check_cli":
            step_check_cli(client)
        elif args.step == "check_datasources":
            step_check_datasources(client, pdir)
        elif args.step == "create_oss_ds":
            step_create_oss_ds(client, pdir)
        elif args.step == "create_odps_ds":
            step_create_odps_ds(client, pdir)
        elif args.step == "create_node":
            step_create_node(client, pdir)
    else:
        deploy_all(client, args.projects_dir, args.projects)


if __name__ == "__main__":
    main()
