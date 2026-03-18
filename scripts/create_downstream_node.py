# -*- coding: utf-8 -*-
"""
create_downstream_node.py
职责：创建下游节点（Downstream Node），将已同步完成的 Parquet 文件移动至 completed 目录。

⚠️  当前为占位实现（Placeholder）：打印提示后正常退出（exit 0），不阻断流水线。
    待后续完成节点内容配置（downstream.json）后，替换为真正的创建逻辑。
"""

import argparse
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))


def main():
    parser = argparse.ArgumentParser(description="Create DataWorks Downstream Node (Placeholder)")
    parser.add_argument(
        "--project-dir", type=str, required=True,
        help="项目目录路径"
    )
    parser.add_argument(
        "--env", type=str, required=True,
        help="环境名称"
    )
    args = parser.parse_args()

    print(f"[PLACEHOLDER] create_downstream_node.py")
    print(f"  project-dir : {args.project_dir}")
    print(f"  env         : {args.env}")
    print(f"  ⚠️  下游节点（Downstream Node）功能尚未实现，跳过此步骤。")
    print(f"  ℹ️  待 downstream.json 配置完成后，替换为真实创建逻辑。")


if __name__ == "__main__":
    main()
