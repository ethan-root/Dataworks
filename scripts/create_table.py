# -*- coding: utf-8 -*-
"""
create_table.py — 在 MaxCompute 中创建目标表（IF NOT EXISTS，幂等）

用法:
    python scripts/create_table.py --project-dir projects/Test

环境变量:
    ALIBABA_CLOUD_ACCESS_KEY_ID
    ALIBABA_CLOUD_ACCESS_KEY_SECRET
    MAXCOMPUTE_PROJECT
    MAXCOMPUTE_ENDPOINT
"""

import argparse
import json
import os
import sys
from pathlib import Path

from odps import ODPS


def get_env_or_fail(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        print(f"ERROR: Environment variable '{name}' is required.")
        sys.exit(1)
    return value


def build_ddl(table_name: str, odps_table: dict) -> str:
    """
    从 config["odps_table"] 生成 CREATE TABLE IF NOT EXISTS DDL。
    与用户提供的 SQL 模板完全对齐。
    """
    col_lines = []
    for col in odps_table["columns"]:
        col_lines.append(
            f"    `{col['name']}`  {col['type']}  COMMENT '{col['comment']}'"
        )
    cols_str = ",\n".join(col_lines)

    comment   = odps_table.get("comment", "")
    partition = odps_table.get("partition", "pt STRING")
    lifecycle = odps_table.get("lifecycle", 36500)

    ddl = (
        f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        f"{cols_str}\n"
        f")\n"
        f"COMMENT '{comment}'\n"
        f"PARTITIONED BY ({partition})\n"
        f"LIFECYCLE {lifecycle};"
    )
    return ddl


def main():
    parser = argparse.ArgumentParser(description="Create MaxCompute table from config.json")
    parser.add_argument("--project-dir", type=str, default="projects/Test",
                        help="项目目录（包含 config.json）")
    args = parser.parse_args()

    # 读取配置
    config_path = Path(args.project_dir) / "config.json"
    if not config_path.exists():
        print(f"ERROR: config.json not found in {args.project_dir}")
        sys.exit(1)

    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    odps_table = config.get("odps_table")
    if not odps_table:
        print("ERROR: 'odps_table' not defined in config.json")
        sys.exit(1)

    table_name = config["writer"]["table"]

    # 连接 MaxCompute
    o = ODPS(
        access_id=get_env_or_fail("ALIBABA_CLOUD_ACCESS_KEY_ID"),
        secret_access_key=get_env_or_fail("ALIBABA_CLOUD_ACCESS_KEY_SECRET"),
        project=get_env_or_fail("MAXCOMPUTE_PROJECT"),
        endpoint=get_env_or_fail("MAXCOMPUTE_ENDPOINT"),
    )

    # 生成并打印 DDL
    ddl = build_ddl(table_name, odps_table)
    print(f"DDL:\n{ddl}\n")

    # 执行建表
    print(f"Creating table '{table_name}' (IF NOT EXISTS) ...")
    o.execute_sql(ddl)
    print(f"✅ Table '{table_name}' ready.")


if __name__ == "__main__":
    main()
