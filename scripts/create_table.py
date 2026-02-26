# -*- coding: utf-8 -*-
"""
create_table.py
职责：在 MaxCompute 中执行建表 SQL，创建目标数据表。

SQL 从项目目录下的 create-table.sql 文件读取，无需修改脚本代码。
特点：幂等操作（SQL 内含 IF NOT EXISTS，表已存在时不报错）

所需环境变量（在 GitHub Actions 中通过 secrets 注入）：
  ALIBABA_CLOUD_ACCESS_KEY_ID      : 阿里云 AccessKey ID
  ALIBABA_CLOUD_ACCESS_KEY_SECRET  : 阿里云 AccessKey Secret
  MAXCOMPUTE_PROJECT               : MaxCompute 项目名（如 maxcompute_parquet_test）
  MAXCOMPUTE_ENDPOINT              : MaxCompute API 地址
"""

import argparse
import os
import sys
from pathlib import Path

# pyodps：MaxCompute 官方 Python SDK
from odps import ODPS


def get_env_or_fail(name: str) -> str:
    """从环境变量读取值，若不存在则报错退出。"""
    value = os.environ.get(name, "").strip()
    if not value:
        print(f"ERROR: Environment variable '{name}' is required.")
        sys.exit(1)
    return value


def main():
    # ── 解析命令行参数 ────────────────────────────────────────────
    parser = argparse.ArgumentParser(description="Create MaxCompute table from create-table.sql")
    parser.add_argument(
        "--project-dir", type=str, default="projects/Test",
        help="项目目录路径，该目录下必须有 create-table.sql 文件（task-config.json 同目录）"
    )
    args = parser.parse_args()

    # ── 读取 SQL 文件 ─────────────────────────────────────────────
    sql_path = Path(args.project_dir) / "create-table.sql"
    if not sql_path.exists():
        print(f"ERROR: create-table.sql not found in {args.project_dir}")
        sys.exit(1)

    ddl = sql_path.read_text(encoding="utf-8").strip()
    print(f"DDL:\n{ddl}\n")

    # ── 连接 MaxCompute ───────────────────────────────────────────
    # ODPS() 是 pyodps 的入口，等同于登录 MaxCompute
    o = ODPS(
        access_id=get_env_or_fail("ALIBABA_CLOUD_ACCESS_KEY_ID"),
        secret_access_key=get_env_or_fail("ALIBABA_CLOUD_ACCESS_KEY_SECRET"),
        project=get_env_or_fail("MAXCOMPUTE_PROJECT"),
        endpoint=get_env_or_fail("MAXCOMPUTE_ENDPOINT"),
    )

    # ── 执行建表 SQL ──────────────────────────────────────────────
    # execute_sql() 会同步等待 SQL 执行完成
    print(f"Executing create-table.sql ...")
    o.execute_sql(ddl)
    print(f"✅ Table created (or already exists).")


if __name__ == "__main__":
    main()
