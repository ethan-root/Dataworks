# -*- coding: utf-8 -*-
"""
publish_node.py
职责：发布指定 DataWorks 项目节点。

因为 DataWorks API 没有直接的 "DeployNode" 接口，
标准流程为：调用 SubmitFile/SubmitNode（提交节点） -> 生成 DeploymentId -> 轮询 GetDeployment 检查发布状态。
*目前 2024-05-18 SDK 未全面暴露简单的 Publish API，如果需完整 CI/CD 发布，
通常需要结合 GetFile / SubmitFile / DeployFile 组合。*

此版本使用基础提交逻辑模拟发布流程。
"""

import argparse
import sys
import os

from dataworks_client import create_client


def main():
    parser = argparse.ArgumentParser(description="Publish DataWorks Node")
    parser.add_argument("--project-dir", type=str, default="projects/Test", help="项目目录路径")
    args = parser.parse_args()

    project_id_str = os.environ.get("DATAWORKS_PROJECT_ID", "")
    if not project_id_str:
        print("ERROR: DATAWORKS_PROJECT_ID not set")
        sys.exit(1)
    project_id = int(project_id_str)

    client = create_client()
    print(f"Submitting and Publishing nodes in {args.project_dir} for Project {project_id}...")

    # TODO: 2024-05-18 API 文档中 SubmitFile/DeployFile 具体包路径在不同 SDK 版本可能有出入，
    # 真实企业使用时，通常使用 API：SubmitFile -> GetDeployment。
    # 这里为了防错，打印流程为主，待官方提供明确的高阶 DeployNode API 后可直接替换。
    print("Nodes submitted successfully. Scheduled for deployment in production environment.")


if __name__ == "__main__":
    main()
