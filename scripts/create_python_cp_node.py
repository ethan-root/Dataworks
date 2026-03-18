# -*- coding: utf-8 -*-
"""
create_python_cp_node.py
职责：创建 DataWorks Python 节点，用途包括：
  - Copy Parquet File 节点：读取最新 Parquet 文件名，传递给数据集成节点
  - MaxCompute Data Delete 节点：按配置的保留策略删除过期的 MC 表分区

⚠️  当前为占位实现（Placeholder）：打印提示后正常退出（exit 0），不阻断流水线。
    待后续完成 python_cp_node.json 配置及脚本内容后，替换为真正的创建逻辑。
"""

import argparse
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))


def main():
    parser = argparse.ArgumentParser(description="Create DataWorks Python CP/Delete Node (Placeholder)")
    parser.add_argument(
        "--project-dir", type=str, required=True,
        help="项目目录路径"
    )
    parser.add_argument(
        "--env", type=str, required=True,
        help="环境名称"
    )
    parser.add_argument(
        "--node-type", type=str, default="cp",
        choices=["cp", "delete"],
        help="节点类型：cp=Copy Parquet File, delete=MaxCompute Data Delete"
    )
    args = parser.parse_args()

    node_desc = {
        "cp":     "Python 节点 - Copy Parquet File（赋值节点/上游节点）",
        "delete": "Python 节点 - MaxCompute Data Delete（分区清理节点）",
    }
    print(f"[PLACEHOLDER] create_python_cp_node.py  (node-type={args.node_type})")
    print(f"  project-dir : {args.project_dir}")
    print(f"  env         : {args.env}")
    print(f"  节点描述    : {node_desc[args.node_type]}")
    print(f"  ⚠️  该节点功能尚未实现，跳过此步骤。")
    print(f"  ℹ️  待 python_cp_node.json 配置完成后，替换为真实创建逻辑。")


if __name__ == "__main__":
    main()
