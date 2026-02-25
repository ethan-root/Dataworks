"""
deploy.py — 主入口
控制 DataWorks 创建定时任务的 CLI 入口
支持全量部署、指定项目部署、分步测试执行

用法:
    python scripts/deploy.py                                          # 处理所有项目
    python scripts/deploy.py --projects Gucci                         # 指定项目
    python scripts/deploy.py --projects Gucci,Balenciaga              # 多个项目
    python scripts/deploy.py --project-dir projects/Test --step check_cli      # 测试 CLI 连接
    python scripts/deploy.py --project-dir projects/Test --step check_datasources
    python scripts/deploy.py --project-dir projects/Test --step create_job
    python scripts/deploy.py --project-dir projects/Test --step submit --file-id 12345
    python scripts/deploy.py --project-dir projects/Test --step deploy --file-id 12345

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

# 将 scripts/ 目录加入 sys.path，以便导入同目录模块
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from dataworks_client import DataWorksClient
from process_project import process_project

# ---- 日志配置 ----
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_env_or_fail(name: str) -> str:
    """获取必须的环境变量"""
    value = os.environ.get(name, "").strip()
    if not value:
        logger.error(f"Environment variable '{name}' is required but not set.")
        sys.exit(1)
    return value


def create_client() -> DataWorksClient:
    """从环境变量创建 DataWorksClient"""
    return DataWorksClient(
        access_key_id=get_env_or_fail("ALIBABA_CLOUD_ACCESS_KEY_ID"),
        access_key_secret=get_env_or_fail("ALIBABA_CLOUD_ACCESS_KEY_SECRET"),
        region=get_env_or_fail("ALIYUN_REGION"),
        project_id=int(get_env_or_fail("DATAWORKS_PROJECT_ID")),
    )


# =========================================================================
# 分步测试命令
# =========================================================================

def step_check_cli(client: DataWorksClient):
    """Step 1: 验证 SDK 连接"""
    logger.info("=" * 60)
    logger.info("Testing DataWorks SDK connection ...")
    logger.info("=" * 60)
    groups = client.list_resource_groups()
    logger.info(f"✅ Connection successful! Found {len(groups)} resource group(s).")
    for g in groups:
        logger.info(f"  Identifier: {g.identifier}")


def step_check_datasources(client: DataWorksClient, project_dir: str):
    """Step 3: 检查数据源是否存在"""
    config = _load_config(project_dir)
    oss_ds = config["OSS"]["DataSourceName"]
    odps_ds = config["MaxCompute"]["DataSourceName"]

    logger.info("=" * 60)
    logger.info("Checking DataSources ...")
    logger.info("=" * 60)

    oss_exists = client.datasource_exists(oss_ds)
    odps_exists = client.datasource_exists(odps_ds)

    logger.info(f"  OSS '{oss_ds}': {'✅ EXISTS' if oss_exists else '❌ NOT FOUND'}")
    logger.info(f"  MaxCompute '{odps_ds}': {'✅ EXISTS' if odps_exists else '❌ NOT FOUND'}")


def step_create_oss_ds(client: DataWorksClient, project_dir: str):
    """Step 4: 创建 OSS 数据源"""
    config = _load_config(project_dir)
    client.ensure_oss_datasource(config)


def step_create_odps_ds(client: DataWorksClient, project_dir: str):
    """Step 5: 创建 MaxCompute 数据源"""
    config = _load_config(project_dir)
    client.ensure_odps_datasource(config)


def step_create_job(client: DataWorksClient, project_dir: str):
    """Step 6: 创建同步 Job"""
    config = _load_config(project_dir)
    project_name = config["ProjectName"]
    table_name = config["Tables"][0]["Name"]
    job_name = f"{project_name}_{table_name}"

    logger.info("=" * 60)
    logger.info(f"Creating DI Sync Task: {job_name}")
    logger.info("=" * 60)

    task_content = client.generate_task_content(config, 0)
    logger.info(f"TaskContent:\n{json.dumps(task_content, indent=2)}")

    resource_group = config["ResourceGroupIdentifier"]
    file_id = client.create_sync_task(job_name, task_content, resource_group)
    logger.info(f"✅ Task created! FileId = {file_id}")
    logger.info(f"📝 Use --file-id {file_id} for submit/deploy steps")

    # 输出 FileId 供 GitHub Actions 使用
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as f:
            f.write(f"file_id={file_id}\n")


def step_submit(client: DataWorksClient, file_id: int):
    """Step 7: 提交任务"""
    logger.info("=" * 60)
    logger.info(f"Submitting FileId: {file_id}")
    logger.info("=" * 60)
    client.submit_file(file_id)
    logger.info("✅ File submitted successfully.")


def step_deploy(client: DataWorksClient, file_id: int):
    """Step 8: 发布到生产"""
    logger.info("=" * 60)
    logger.info(f"Deploying FileId: {file_id}")
    logger.info("=" * 60)
    client.deploy_file(file_id)
    logger.info("✅ File deployed to production.")


# =========================================================================
# 全量部署
# =========================================================================

def deploy_all(client: DataWorksClient, projects_dir: str, target_projects: str = ""):
    """遍历项目目录，逐一处理"""
    projects_path = Path(projects_dir)

    logger.info("")
    logger.info("╔" + "═" * 60 + "╗")
    logger.info("║   DataWorks Data Integration — Deployment Tool            ║")
    logger.info("╠" + "═" * 60 + "╣")
    logger.info(f"║  Project ID: {client.project_id}")
    logger.info(f"║  Projects Dir: {projects_path}")
    logger.info(f"║  Target: {target_projects or 'ALL'}")
    logger.info("╚" + "═" * 60 + "╝")

    # 构建项目列表
    project_dirs = []
    if target_projects:
        for name in target_projects.split(","):
            name = name.strip()
            d = projects_path / name
            if not d.is_dir():
                logger.error(f"Project directory not found: {d}")
                sys.exit(1)
            project_dirs.append(str(d))
    else:
        for d in sorted(projects_path.iterdir()):
            if d.is_dir() and (d / "config.json").exists():
                project_dirs.append(str(d))

    if not project_dirs:
        logger.error(f"No project directories found in {projects_path}")
        sys.exit(1)

    logger.info(f"Found {len(project_dirs)} project(s) to process.")

    total_success = 0
    total_failed = 0

    for project_dir in project_dirs:
        stats = process_project(client, project_dir)
        if stats["failed"] > 0:
            total_failed += 1
        else:
            total_success += 1

    logger.info("")
    logger.info("╔" + "═" * 60 + "╗")
    logger.info("║                  Deployment Complete                       ║")
    logger.info("╠" + "═" * 60 + "╣")
    logger.info(f"║  Projects Succeeded: {total_success}")
    logger.info(f"║  Projects Failed:    {total_failed}")
    logger.info("╚" + "═" * 60 + "╝")

    if total_failed > 0:
        logger.error("Some projects failed. Check the logs above.")
        sys.exit(1)

    logger.info("All projects deployed successfully!")


# =========================================================================
# Helper
# =========================================================================

def _load_config(project_dir: str) -> dict:
    """加载 config.json"""
    config_path = Path(project_dir) / "config.json"
    if not config_path.exists():
        logger.error(f"config.json not found in {project_dir}")
        sys.exit(1)
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


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
                        help="Single project directory for step execution")
    parser.add_argument("--step", type=str, default="",
                        choices=["", "check_cli", "check_datasources",
                                 "create_oss_ds", "create_odps_ds",
                                 "create_job", "submit", "deploy"],
                        help="Execute a single step (for testing)")
    parser.add_argument("--file-id", type=int, default=0,
                        help="FileId for submit/deploy steps")

    args = parser.parse_args()

    client = create_client()

    if args.step:
        # ---- 分步执行 ----
        if args.step == "check_cli":
            step_check_cli(client)
        elif args.step == "check_datasources":
            step_check_datasources(client, args.project_dir or "projects/Test")
        elif args.step == "create_oss_ds":
            step_create_oss_ds(client, args.project_dir or "projects/Test")
        elif args.step == "create_odps_ds":
            step_create_odps_ds(client, args.project_dir or "projects/Test")
        elif args.step == "create_job":
            step_create_job(client, args.project_dir or "projects/Test")
        elif args.step == "submit":
            if not args.file_id:
                logger.error("--file-id is required for submit step")
                sys.exit(1)
            step_submit(client, args.file_id)
        elif args.step == "deploy":
            if not args.file_id:
                logger.error("--file-id is required for deploy step")
                sys.exit(1)
            step_deploy(client, args.file_id)
    else:
        # ---- 全量部署 ----
        deploy_all(client, args.projects_dir, args.projects)


if __name__ == "__main__":
    main()
