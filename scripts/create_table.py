# -*- coding: utf-8 -*-
"""
create_table.py
职责：在 MaxCompute（阿里云大数据计算服务）中创建目标数据表。

特点：
  - 幂等操作：使用 CREATE TABLE IF NOT EXISTS，表已存在时不报错
  - 表结构从 config.json 的 "odps_table" 字段读取，无需修改脚本
  - 使用 pyodps（MaxCompute 官方 Python SDK）

所需环境变量（在 GitHub Actions 中通过 secrets 注入）：
  ALIBABA_CLOUD_ACCESS_KEY_ID      : 阿里云 AccessKey ID
  ALIBABA_CLOUD_ACCESS_KEY_SECRET  : 阿里云 AccessKey Secret
  MAXCOMPUTE_PROJECT               : MaxCompute 项目名（如 maxcompute_parquet_test）
  MAXCOMPUTE_ENDPOINT              : MaxCompute API 地址（如 http://service.cn-shanghai.maxcompute.aliyun.com/api）
"""

import argparse
import json
import os
import sys
from pathlib import Path

# pyodps：MaxCompute 官方 Python SDK
from odps import ODPS


def get_env_or_fail(name: str) -> str:
    """
    从环境变量读取值，若不存在则打印错误并退出。
    避免后续调用时出现难以理解的错误。
    """
    value = os.environ.get(name, "").strip()
    if not value:
        print(f"ERROR: Environment variable '{name}' is required.")
        sys.exit(1)
    return value


def build_ddl(table_name: str, odps_table: dict) -> str:
    """
    根据 config.json 中 "odps_table" 的配置，生成建表 SQL（DDL）。

    生成结果示例：
      CREATE TABLE IF NOT EXISTS test (
          `col1`  STRING          COMMENT '*',
          `col3`  DECIMAL(38,18)  COMMENT '*'
      )
      COMMENT 'null'
      PARTITIONED BY (pt STRING)
      LIFECYCLE 36500;

    Args:
        table_name: 目标表名（来自 config["writer"]["table"]）
        odps_table: 表结构配置（来自 config["odps_table"]）
    Returns:
        DDL SQL 字符串
    """
    # ── 拼接列定义部分 ────────────────────────────────────────────
    col_lines = []
    for col in odps_table["columns"]:
        # 格式：`列名`  类型  COMMENT '备注'
        col_lines.append(
            f"    `{col['name']}`  {col['type']}  COMMENT '{col['comment']}'"
        )
    cols_str = ",\n".join(col_lines)   # 列定义之间用逗号+换行分隔

    # ── 读取其他表属性 ────────────────────────────────────────────
    comment   = odps_table.get("comment", "")            # 表注释
    partition = odps_table.get("partition", "pt STRING") # 分区字段定义
    lifecycle = odps_table.get("lifecycle", 36500)        # 数据生命周期（天）36500≈100年

    # ── 拼接完整 DDL ──────────────────────────────────────────────
    ddl = (
        f"CREATE TABLE IF NOT EXISTS {table_name} (\n"  # IF NOT EXISTS：表存在则跳过
        f"{cols_str}\n"
        f")\n"
        f"COMMENT '{comment}'\n"
        f"PARTITIONED BY ({partition})\n"   # 分区表，按 pt（日期）分区
        f"LIFECYCLE {lifecycle};"           # 数据保留天数
    )
    return ddl


def main():
    # ── 解析命令行参数 ────────────────────────────────────────────
    parser = argparse.ArgumentParser(description="Create MaxCompute table from config.json")
    parser.add_argument(
        "--project-dir", type=str, default="projects/Test",
        help="项目目录路径，该目录下必须有 config.json"
    )
    args = parser.parse_args()

    # ── 读取 config.json ──────────────────────────────────────────
    config_path = Path(args.project_dir) / "config.json"
    if not config_path.exists():
        print(f"ERROR: config.json not found in {args.project_dir}")
        sys.exit(1)

    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    # 检查 config.json 中是否有建表配置
    odps_table = config.get("odps_table")
    if not odps_table:
        print("ERROR: 'odps_table' not defined in config.json")
        sys.exit(1)

    # 目标表名来自 writer.table（与 DataWorks 节点配置保持一致）
    table_name = config["writer"]["table"]   # 例如 "test"

    # ── 连接 MaxCompute ───────────────────────────────────────────
    # ODPS() 是 pyodps 的入口，等同于登录 MaxCompute
    o = ODPS(
        access_id=get_env_or_fail("ALIBABA_CLOUD_ACCESS_KEY_ID"),
        secret_access_key=get_env_or_fail("ALIBABA_CLOUD_ACCESS_KEY_SECRET"),
        project=get_env_or_fail("MAXCOMPUTE_PROJECT"),    # MaxCompute 项目名
        endpoint=get_env_or_fail("MAXCOMPUTE_ENDPOINT"),  # MaxCompute API 地址
    )

    # ── 生成并打印 DDL（方便在 Actions 日志中确认）────────────────
    ddl = build_ddl(table_name, odps_table)
    print(f"DDL:\n{ddl}\n")

    # ── 执行建表 ──────────────────────────────────────────────────
    # execute_sql() 会同步等待 SQL 执行完成
    print(f"Creating table '{table_name}' (IF NOT EXISTS) ...")
    o.execute_sql(ddl)
    print(f"✅ Table '{table_name}' ready.")


if __name__ == "__main__":
    main()
