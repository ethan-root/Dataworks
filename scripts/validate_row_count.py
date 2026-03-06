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


def print_table_data(o, table_name, label):
    """查询并打印表的实际数据（最多 20 行）"""
    print(f"\n--- {label} ({table_name}) 实际数据 ---")
    try:
        inst = o.execute_sql(
            f"SELECT * FROM {table_name} LIMIT 20",
            hints={'odps.sql.allow.fullscan': 'true'}
        )
        with inst.open_reader() as reader:
            rows = list(reader)
            if not rows:
                print("  (空表，无数据)")
                return
            # 打印列名（pyodps 返回 TableColumn 对象，需取 .name）
            cols = [col.name for col in rows[0]._columns]
            print("  " + " | ".join(cols))
            print("  " + "-" * (len(" | ".join(cols)) + 2))
            for row in rows:
                print("  " + " | ".join(str(row[c]) for c in cols))
    except Exception as e:
        print(f"  查询失败: {e}")


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

    # 打印两张表的实际数据，方便排查差异
    print_table_data(o, args.ext_table, "外部表")
    print_table_data(o, args.int_table, "内部表")

    if ext_count == int_count:
        print("\n✅ 数据一致，校验通过！")
    else:
        print(f"\n❌ 数据不一致！差异：{abs(ext_count - int_count)} 行")
        import sys; sys.exit(1)


if __name__ == "__main__":
    main()

