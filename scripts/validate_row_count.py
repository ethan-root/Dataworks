# -*- coding: utf-8 -*-
"""
validate_row_count.py
临时验证脚本：对比外部表（OSS Parquet）与内部表的行数。
用法：
    python scripts/validate_row_count.py \
        --ext-table val_feature_demo_ext \
        --int-table user
"""

import os
import argparse
from odps import ODPS


def get_env_or_fail(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        print(f"ERROR: Environment variable '{name}' is required.")
        import sys; sys.exit(1)
    return value


def main():
    parser = argparse.ArgumentParser(description="Validate row counts: external vs internal table")
    parser.add_argument("--ext-table", default="val_feature_demo_ext", help="外部表名")
    parser.add_argument("--int-table", default="user", help="内部表名")
    args = parser.parse_args()

    o = ODPS(
        access_id=get_env_or_fail("ALIBABA_CLOUD_ACCESS_KEY_ID"),
        secret_access_key=get_env_or_fail("ALIBABA_CLOUD_ACCESS_KEY_SECRET"),
        project=get_env_or_fail("MAXCOMPUTE_PROJECT"),
        endpoint=get_env_or_fail("MAXCOMPUTE_ENDPOINT"),
    )

    sql = f"""
    SELECT '{args.ext_table}' AS source, COUNT(*) AS row_count
    FROM {args.ext_table}
    UNION ALL
    SELECT '{args.int_table}' AS source, COUNT(*) AS row_count
    FROM {args.int_table}
    """

    print(f"Executing SQL:\n{sql}")
    # hints: allow full scan on partitioned tables (needed when no partition predicate is specified)
    inst = o.execute_sql(sql, hints={'odps.sql.allow.fullscan': 'true'})

    with inst.open_reader() as reader:
        results = {row['source']: row['row_count'] for row in reader}

    ext_count = results.get(args.ext_table, 'N/A')
    int_count = results.get(args.int_table, 'N/A')

    print("\n========== 行数对比结果 ==========")
    print(f"  外部表 ({args.ext_table}): {ext_count} 行")
    print(f"  内部表 ({args.int_table}): {int_count} 行")
    print("====================================")

    if ext_count == int_count:
        print("✅ 数据一致，校验通过！")
    else:
        print(f"❌ 数据不一致！差异：{abs(ext_count - int_count)} 行")
        import sys; sys.exit(1)


if __name__ == "__main__":
    main()
