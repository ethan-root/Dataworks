# -*- coding: utf-8 -*-
"""
publish_node.py
职责：将指定 DataWorks 项目中的节点提交并发布到生产环境。

DataWorks 发布流程（2024-05-18 API）：
  1. ListFiles         — 按节点名搜索，获取 FileId
  2. SubmitFile        — 提交指定 FileId，生成 DeploymentId
  3. GetDeployment     — 轮询部署状态，直到 Success 或 Fail

所需环境变量：
  ALIBABA_CLOUD_ACCESS_KEY_ID
  ALIBABA_CLOUD_ACCESS_KEY_SECRET
  ALIYUN_REGION
  DATAWORKS_PROJECT_ID
"""

import argparse
import json
import sys
import os
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from dataworks_client import create_client

from alibabacloud_dataworks_public20240518 import models as dw_models
from alibabacloud_tea_util import models as util_models


# ── 轮询参数 ──────────────────────────────────────────────
POLL_INTERVAL_SEC = 10   # 每次轮询间隔（秒）
POLL_TIMEOUT_SEC  = 300  # 最长等待时间（秒）


def get_file_id(client, project_id: int, node_name: str) -> int:
    """
    通过节点名查找对应的 FileId。
    DataWorks 中创建节点时会生成一个 FileId，提交时需要用它。

    Args:
        client:     DataWorks SDK Client
        project_id: DataWorks 工作空间 ID
        node_name:  节点名（即 task-config.json 中的 node_name 字段）
    Returns:
        FileId（整数）
    Raises:
        SystemExit: 未找到节点时退出
    """
    print(f"🔍 Searching for node '{node_name}' in project {project_id}...")
    request = dw_models.ListFilesRequest(
        project_id=project_id,
        keyword=node_name,
        page_size=10,
    )
    resp = client.list_files_with_options(request, util_models.RuntimeOptions())
    files = resp.body.data.files if (resp.body.data and resp.body.data.files) else []

    # 精确匹配节点名（keyword 是模糊搜索，可能返回多个）
    matched = [f for f in files if f.file_name == node_name]
    if not matched:
        print(f"❌ Node '{node_name}' not found. Has it been created via create_node?")
        print(f"   Available files: {[f.file_name for f in files]}")
        sys.exit(1)

    file_id = matched[0].file_id
    print(f"✅ Found node '{node_name}', FileId={file_id}")
    return file_id


def submit_file(client, project_id: int, file_id: int, comment: str = "Auto-publish by GitHub Actions") -> int:
    """
    提交指定 FileId，触发发布流程，返回 DeploymentId。

    Args:
        client:     DataWorks SDK Client
        project_id: DataWorks 工作空间 ID
        file_id:    要提交的文件 ID
        comment:    发布备注（可选）
    Returns:
        DeploymentId（整数）
    """
    print(f"📤 Submitting FileId={file_id} for project {project_id}...")
    request = dw_models.SubmitFileRequest(
        project_id=project_id,
        file_id=file_id,
        comment=comment,
    )
    resp = client.submit_file_with_options(request, util_models.RuntimeOptions())
    deployment_id = resp.body.deployment_id
    if not deployment_id:
        print("❌ SubmitFile returned no DeploymentId, check DataWorks console for errors.")
        sys.exit(1)
    print(f"✅ Submitted successfully. DeploymentId={deployment_id}")
    return deployment_id


def wait_for_deployment(client, project_id: int, deployment_id: int) -> None:
    """
    轮询 GetDeployment API，直到部署成功或失败。

    Args:
        client:        DataWorks SDK Client
        project_id:    DataWorks 工作空间 ID
        deployment_id: 由 SubmitFile 返回的 Deployment ID
    Raises:
        SystemExit: 部署失败或超时时退出
    """
    print(f"⏳ Polling deployment status (DeploymentId={deployment_id})...")
    elapsed = 0
    while elapsed < POLL_TIMEOUT_SEC:
        request = dw_models.GetDeploymentRequest(
            project_id=project_id,
            deployment_id=deployment_id,
        )
        resp = client.get_deployment_with_options(request, util_models.RuntimeOptions())
        status = resp.body.deployment.status if resp.body.deployment else "Unknown"

        print(f"   [{elapsed:>3}s] Status: {status}")

        if status == "Success":
            print(f"✅ Deployment succeeded!")
            return
        elif status in ("Fail", "Rejected", "Abort"):
            print(f"❌ Deployment failed with status: {status}")
            # 打印错误详情（如有）
            detail = resp.body.deployment
            if detail:
                print(json.dumps(detail.to_map(), indent=2, ensure_ascii=False))
            sys.exit(1)

        # 状态为 Waiting / Running / Deploying 等，继续等待
        time.sleep(POLL_INTERVAL_SEC)
        elapsed += POLL_INTERVAL_SEC

    print(f"❌ Deployment timed out after {POLL_TIMEOUT_SEC}s. Check DataWorks console.")
    sys.exit(1)


def main():
    # ── 解析命令行参数 ──────────────────────────────────────
    parser = argparse.ArgumentParser(description="Publish DataWorks Node to Production")
    parser.add_argument(
        "--project-dir", type=str, default="projects/Test",
        help="项目目录路径（需包含 task-config.json）"
    )
    parser.add_argument(
        "--comment", type=str, default="Auto-publish by GitHub Actions",
        help="发布备注"
    )
    args = parser.parse_args()

    # ── 读取节点名 ──────────────────────────────────────────
    config_path = Path(args.project_dir) / "task-config.json"
    if not config_path.exists():
        print(f"ERROR: task-config.json not found in {args.project_dir}")
        sys.exit(1)
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    node_name = config["node_name"]

    # ── 读取工作空间 ID ─────────────────────────────────────
    project_id_str = os.environ.get("DATAWORKS_PROJECT_ID", "")
    if not project_id_str:
        print("ERROR: DATAWORKS_PROJECT_ID not set")
        sys.exit(1)
    project_id = int(project_id_str)

    # ── 初始化客户端 ────────────────────────────────────────
    client = create_client()
    print(f"🚀 Publishing node '{node_name}' in project {project_id}...")

    # ── 执行三步发布流程 ────────────────────────────────────
    file_id = get_file_id(client, project_id, node_name)
    deployment_id = submit_file(client, project_id, file_id, args.comment)
    wait_for_deployment(client, project_id, deployment_id)

    print(f"\n🎉 Node '{node_name}' has been published to production environment.")


if __name__ == "__main__":
    main()
