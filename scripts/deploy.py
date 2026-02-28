# -*- coding: utf-8 -*-
"""
deploy.py
职责：创建 DataWorks 定时节点的命令行入口。

支持两种运行模式：
  1. 单项目模式：--project-dir feature/test-feature   （只创建 test-feature 的节点）
  2. 全量模式：  不传参数                            （自动扫描 feature/ 下所有项目）

所需环境变量（在 GitHub Actions 中通过 secrets 注入）：
  ALIBABA_CLOUD_ACCESS_KEY_ID      : 阿里云 AccessKey ID
  ALIBABA_CLOUD_ACCESS_KEY_SECRET  : 阿里云 AccessKey Secret
  ALIYUN_REGION                    : 地域（如 cn-shanghai）
  DATAWORKS_PROJECT_ID             : DataWorks 工作空间 ID（数字）
"""

import argparse
import os
import sys
from pathlib import Path

# 把 scripts/ 目录加入 Python 搜索路径，确保能 import 同目录的模块
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

# 导入同目录的两个模块
from dataworks_client import create_client    # 初始化 DataWorks SDK 客户端
from process_project import process_project   # 读取 config.json 并创建节点


def main():
    # ── 解析命令行参数 ────────────────────────────────────────────
    parser = argparse.ArgumentParser(description="DataWorks Deployment Tool")
    parser.add_argument(
        "--project-dir", type=str, default="",
        help="指定单个功能目录路径（留空则自动扫描 --projects-dir 下所有功能）"
    )
    parser.add_argument(
        "--projects-dir", type=str, default="feature",
        help="功能根目录，默认为 'feature'"
    )
    args = parser.parse_args()

    # ── 读取工作空间 ID（必须是整数）─────────────────────────────
    project_id_str = os.environ.get("DATAWORKS_PROJECT_ID", "")
    if not project_id_str:
        print("ERROR: DATAWORKS_PROJECT_ID not set")
        sys.exit(1)
    project_id = int(project_id_str)   # API 要求传整数

    # ── 初始化 DataWorks 客户端 ───────────────────────────────────
    # create_client() 会从环境变量读取 AK/SK/Region
    client = create_client()

    # ── 根据参数选择运行模式 ──────────────────────────────────────
    if args.project_dir:
        # 模式1：单项目 —— 直接处理指定目录
        process_project(client, project_id, args.project_dir)
    else:
        # 模式2：全量 —— 扫描 feature/ 下所有包含 task-config.json 的子目录（现为环境目录）
        projects_path = Path(args.projects_dir)
        project_dirs = []
        for feature_dir in projects_path.iterdir():
            if feature_dir.is_dir():
                for env_dir in feature_dir.iterdir():
                    if env_dir.is_dir() and (env_dir / "task-config.json").exists():
                        project_dirs.append(env_dir)
        
        project_dirs = sorted(project_dirs)
        if not project_dirs:
            print(f"No projects found in {projects_path}")
            sys.exit(1)
        for d in project_dirs:
            process_project(client, project_id, str(d))


if __name__ == "__main__":
    main()
