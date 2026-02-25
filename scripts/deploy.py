# -*- coding: utf-8 -*-
"""
deploy.py — 主入口

用法:
    python scripts/deploy.py                          # 处理 projects/ 下所有项目
    python scripts/deploy.py --project-dir projects/Test  # 指定单个项目

环境变量:
    ALIBABA_CLOUD_ACCESS_KEY_ID
    ALIBABA_CLOUD_ACCESS_KEY_SECRET
    ALIYUN_REGION
    DATAWORKS_PROJECT_ID
"""

import argparse
import os
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from dataworks_client import create_client
from process_project import process_project


def main():
    parser = argparse.ArgumentParser(description="DataWorks Deployment Tool")
    parser.add_argument(
        "--project-dir", type=str, default="",
        help="指定单个项目目录（默认：遍历 projects/ 下所有项目）"
    )
    parser.add_argument(
        "--projects-dir", type=str, default="projects",
        help="项目根目录（默认：projects）"
    )
    args = parser.parse_args()

    project_id_str = os.environ.get("DATAWORKS_PROJECT_ID", "")
    if not project_id_str:
        print("ERROR: DATAWORKS_PROJECT_ID not set")
        sys.exit(1)
    project_id = int(project_id_str)

    client = create_client()

    if args.project_dir:
        # 单项目模式
        process_project(client, project_id, args.project_dir)
    else:
        # 全量模式：遍历 projects/ 下所有含 config.json 的目录
        projects_path = Path(args.projects_dir)
        project_dirs = sorted(
            d for d in projects_path.iterdir()
            if d.is_dir() and (d / "config.json").exists()
        )
        if not project_dirs:
            print(f"No projects found in {projects_path}")
            sys.exit(1)
        for d in project_dirs:
            process_project(client, project_id, str(d))


if __name__ == "__main__":
    main()
