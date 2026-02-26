# -*- coding: utf-8 -*-
"""
clean_mc_tables.py
职责：清理 MaxCompute 中创建时间大于 30 天的表，并支持白名单过滤。

所需环境变量：
  ALIBABA_CLOUD_ACCESS_KEY_ID
  ALIBABA_CLOUD_ACCESS_KEY_SECRET
  MAXCOMPUTE_PROJECT
  MAXCOMPUTE_ENDPOINT
"""

import os
import sys
import time
from datetime import datetime, timedelta

from odps import ODPS


# 白名单：这些表永远不会被自动删除（可以根据实际情况修改）
WHITELIST_TABLES = [
    "product_dim",
    "user_dim",
    "core_metrics"
]

def get_env_or_fail(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        print(f"ERROR: Environment variable '{name}' is required.")
        sys.exit(1)
    return value

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Clean old MaxCompute tables")
    parser.add_argument("--execute", action="store_true", help="实际执行删除操作（不传则为 dry-run）")
    parser.add_argument("--days", type=int, default=30, help="删除多少天前的表（默认30）")
    args = parser.parse_args()

    print("Connecting to MaxCompute...")
    o = ODPS(
        access_id=get_env_or_fail("ALIBABA_CLOUD_ACCESS_KEY_ID"),
        secret_access_key=get_env_or_fail("ALIBABA_CLOUD_ACCESS_KEY_SECRET"),
        project=get_env_or_fail("MAXCOMPUTE_PROJECT"),
        endpoint=get_env_or_fail("MAXCOMPUTE_ENDPOINT"),
    )

    threshold_date = datetime.now() - timedelta(days=args.days)
    print(f"Threshold date: {threshold_date.strftime('%Y-%m-%d %H:%M:%S')} ({args.days} days ago)")
    print(f"Whitelist tables (will not be deleted): {WHITELIST_TABLES}")
    print("-" * 50)

    # 必须要设置 extended=True 才能获取到表属性(creation_time)
    tables = o.list_tables(extended=True)
    
    count_deleted = 0
    count_kept = 0
    count_whitelisted = 0

    for table in tables:
        table_name = table.name
        
        # 白名单过滤
        if table_name in WHITELIST_TABLES:
            print(f"[SKIP] '{table_name}' is in whitelist.")
            count_whitelisted += 1
            continue

        # 获取创建时间 (如果底层 API 没返回，则跳过防止报错)
        creation_time = getattr(table, 'creation_time', None)
        if not creation_time:
            print(f"[SKIP] '{table_name}' has no creation_time.")
            count_kept += 1
            continue

        if creation_time < threshold_date:
            if args.execute:
                print(f"[DELETE] Dropping table '{table_name}' (created {creation_time})")
                table.drop(async_=False) # 同步删除
            else:
                print(f"[DRY-RUN] Will drop table '{table_name}' (created {creation_time})")
            count_deleted += 1
        else:
            count_kept += 1

    print("-" * 50)
    print(f"Summary:")
    print(f"  Deleted (or will delete): {count_deleted}")
    print(f"  Kept (newer than {args.days} days): {count_kept}")
    print(f"  Skipped (whitelist): {count_whitelisted}")
    
    if not args.execute:
        print("\nNote: This was a DRY-RUN. To actually delete tables, add the --execute flag.")

if __name__ == "__main__":
    main()
