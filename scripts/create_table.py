# -*- coding: utf-8 -*-
"""
create_table.py
职责：在 MaxCompute 中执行建表 SQL 和 DDL 迁移工具。

按时间戳顺序执行 features/<feature_name>/ddl 目录下的 SQL 文件。
已执行的 SQL 文件会自动跳过（通过检查 database_changelog 表）。
支持从 CI/CD 环境直接读取变量。
"""

import os
import re
import hashlib
from datetime import datetime
from pathlib import Path
from odps import ODPS
from odps.errors import ODPSError
import argparse

def calculate_file_hash(file_path):
    """
    计算文件的 MD5 哈希值。
    用于记录 DDL 文件是否发生过篡改或变更，作为 database_changelog 表的校验依据。
    """
    md5_hash = hashlib.md5()
    with open(file_path, "rb") as f:
        # 分块读取大文件以节省内存
        for chunk in iter(lambda: f.read(4096), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()

def extract_timestamp_from_filename(filename):
    """
    从 SQL 文件名中提取前置的时间戳。
    例如从 '202602141515_create.sql' 提取 '202602141515'，用于保证 SQL 脚本按时间线线性执行。
    """
    match = re.match(r'^(\d{10,14})', filename)
    if match:
        return match.group(1)
    return None

def get_sorted_sql_files(directory):
    """
    扫描指定目录下的所有 .sql 文件，并按时间戳排序。
    返回列表供自动化发布引擎顺次执行，保证数据库的版本状态单向演进。
    """
    dir_path = Path(directory)
    if dir_path.name != 'ddl':
        dir_path = dir_path / 'ddl'
    
    if not dir_path.exists():
        print(f"✗ 目录不存在: {dir_path}")
        return []
    
    sql_files = []
    # 遍历该目录下所有 .sql 文件
    for file in dir_path.glob("*.sql"):
        filename = file.name
        timestamp = extract_timestamp_from_filename(filename)
        if timestamp:
            sql_files.append({
                'path': str(file),
                'filename': filename,
                'timestamp': timestamp
            })
        else:
            print(f"⚠ 跳过文件（无时间戳前缀规范）: {filename}")
    
    # 强制让所有 SQL 依照时间先后排序执行
    sql_files.sort(key=lambda x: x['timestamp'])
    return sql_files

def check_if_executed(o, ddl_file):
    """
    查询中心 Changelog 元数据表，验证当前 SQL 文件是否已经在远端 MaxCompute 执行成功过。
    以此保证 CI/CD 流程多次反复触发具备幂等性（Idempotent），不会重复建表或抛错。
    """
    sql = f"""
    SELECT COUNT(*) as cnt
    FROM database_changelog
    WHERE ddl_file = '{ddl_file}' AND status = 'SUCCESS'
    """
    try:
        with o.execute_sql(sql).open_reader() as reader:
            for record in reader:
                return record['cnt'] > 0
    except Exception as e:
        print(f"⚠ 检查执行记录失败（可能是表不存在导致）: {e}")
        return False

def extract_table_name_from_sql(sql_content):
    """
    利用正则表达式从 SQL 内容中粗略提取目标表明。
    用于写入 database_changelog 进行影响记录和可视化显示。
    """
    patterns = [
        r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[a-zA-Z0-9_]+\.)?`?([a-zA-Z0-9_]+)`?',
        r'ALTER\s+TABLE\s+(?:[a-zA-Z0-9_]+\.)?`?([a-zA-Z0-9_]+)`?',
        r'DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:[a-zA-Z0-9_]+\.)?`?([a-zA-Z0-9_]+)`?',
    ]
    for pattern in patterns:
        match = re.search(pattern, sql_content, re.IGNORECASE)
        if match:
            return match.group(1)
    return 'unknown'

def execute_sql_file(o, file_info):
    file_path = file_info['path']
    filename = file_info['filename']
    print(f"\n{'='*70}")
    print(f"执行文件: {filename}")
    print(f"{'='*70}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read().strip()
    except Exception as e:
        return {
            'filename': filename,
            'table_name': 'unknown',
            'status': 'FAILED',
            'error': f'读取文件失败: {e}',
            'hashcode': None
        }
    
    hashcode = calculate_file_hash(file_path)
    table_name = extract_table_name_from_sql(sql_content)
    
    print(f"表名: {table_name}")
    print(f"Hash: {hashcode}")
    print(f"SQL 预览: {sql_content[:200]}...")
    
    try:
        instance = o.execute_sql(sql_content)
        print(f"任务 ID: {instance.id}")
        print("等待执行完成...")
        instance.wait_for_success(timeout=600)
        print("✓ 执行成功")
        return {
            'filename': filename,
            'table_name': table_name,
            'status': 'SUCCESS',
            'error': None,
            'hashcode': hashcode,
            'instance_id': instance.id
        }
    except ODPSError as e:
        error_msg = str(e)
        print(f"✗ 执行失败: {error_msg}")
        return {
            'filename': filename,
            'table_name': table_name,
            'status': 'FAILED',
            'error': error_msg,
            'hashcode': hashcode,
            'instance_id': None
        }
    except Exception as e:
        error_msg = str(e)
        print(f"✗ 未知错误: {error_msg}")
        return {
            'filename': filename,
            'table_name': table_name,
            'status': 'FAILED',
            'error': error_msg,
            'hashcode': hashcode,
            'instance_id': None
        }

def record_execution(o, result):
    table_name = result['table_name']
    ddl_file = result['filename']
    applied_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    hashcode = result['hashcode'] or 'NULL'
    status = result['status']
    
    ddl_file_escaped = ddl_file.replace("'", "\\'")
    
    insert_sql = f"""
    INSERT INTO database_changelog
    (table_name, ddl_file, applied_at, hashcode, status)
    VALUES
    ('{table_name}', '{ddl_file_escaped}', CAST('{applied_at}' AS DATETIME), '{hashcode}', '{status}')
    """
    try:
        print(f"\n记录执行日志...")
        instance = o.execute_sql(insert_sql)
        instance.wait_for_success(timeout=60)
        print(f"✓ 日志记录成功")
    except Exception as e:
        print(f"✗ 日志记录失败: {e}")

def ensure_changelog_table_exists(o, project_dir):
    try:
        print("检查 database_changelog 表...")
        if o.exist_table('database_changelog'):
            print("✓ database_changelog 表已存在")
            return
        print("database_changelog 表不存在，准备创建...")
        # 从 features/shared/ddl-metadata.sql 加载建表语句
        script_dir = Path(__file__).parent
        project_root = script_dir.parent
        ddl_file = project_root / "features" / "shared" / "ddl-metadata.sql"
        if not ddl_file.exists():
            print(f"找不到 shared 目录下的 DDL: {ddl_file}")
            # 兼容如果执行路径不同，通过相对路径失败时的降级
            fallback_path = Path("features/shared/ddl-metadata.sql")
            if fallback_path.exists():
                ddl_file = fallback_path
            else:
                raise FileNotFoundError(f"DDL 文件不存在: {ddl_file} 或 {fallback_path}")
        
        print(f"读取 DDL 文件: {ddl_file}")
        with open(ddl_file, 'r', encoding='utf-8') as f:
            create_table_sql = f.read().strip()
        
        instance = o.execute_sql(create_table_sql)
        instance.wait_for_success()
        print("✓ database_changelog 表创建成功")
        
    except Exception as e:
        print(f"✗ 创建 database_changelog 表失败: {e}")
        raise

def execute_sql_migrations(o, sql_directory, project_dir, skip_executed=True):
    print(f"\n{'#'*70}")
    print(f"# SQL 迁移工具")
    print(f"# 目录: {sql_directory}")
    print(f"{'#'*70}\n")
    
    ensure_changelog_table_exists(o, project_dir)
    
    sql_files = get_sorted_sql_files(sql_directory)
    if not sql_files:
        print("⚠ 未找到任何 SQL 文件")
        return
    
    print(f"\n找到 {len(sql_files)} 个 SQL 文件:")
    for i, file_info in enumerate(sql_files, 1):
        print(f"  {i}. {file_info['filename']} (时间戳: {file_info['timestamp']})")
    
    total = len(sql_files)
    success_count = 0
    failed_count = 0
    skipped_count = 0
    
    for i, file_info in enumerate(sql_files, 1):
        filename = file_info['filename']
        print(f"\n[{i}/{total}] 处理文件: {filename}")
        
        if skip_executed and check_if_executed(o, filename):
            print(f"⚠ 文件已执行过，跳过")
            skipped_count += 1
            continue
        
        result = execute_sql_file(o, file_info)
        record_execution(o, result)
        
        if result['status'] == 'SUCCESS':
            success_count += 1
        else:
            failed_count += 1
            print(f"\n⚠ 文件执行失败: {filename}")
            print(f"错误: {result['error']}")
            print("CI/CD 环境，遇到失败中止执行！")
            import sys
            sys.exit(1)
            
    print(f"\n{'='*70}")
    print(f"执行完成")
    print(f"{'='*70}")
    print(f"总文件数: {total}")
    print(f"成功: {success_count}")
    print(f"失败: {failed_count}")
    print(f"跳过: {skipped_count}")
    print(f"{'='*70}\n")

def get_env_or_fail(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        print(f"ERROR: Environment variable '{name}' is required.")
        import sys
        sys.exit(1)
    return value

def main():
    parser = argparse.ArgumentParser(description='MaxCompute SQL 迁移工具')
    parser.add_argument(
        "--project-dir", type=str, required=True,
        help="项目目录路径 (如 features/user-feature)"
    )
    parser.add_argument(
        "--env", type=str, required=True,
        help="环境名称"
    )
    parser.add_argument('--force', action='store_true', help='强制执行所有文件')
    args = parser.parse_args()
    
    access_id = get_env_or_fail("ALIBABA_CLOUD_ACCESS_KEY_ID")
    secret_key = get_env_or_fail("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
    project = get_env_or_fail("MAXCOMPUTE_PROJECT")
    endpoint = get_env_or_fail("MAXCOMPUTE_ENDPOINT")
    
    print("连接 MaxCompute...")
    o = ODPS(access_id, secret_key, project, endpoint)
    print("✓ 连接成功\n")
    
    sql_dir = Path(args.project_dir) / "ddl"
    print(f"SQL 目录: {sql_dir}")
    
    # 向后兼容 database_update.yml plan/apply 中孤立文件的逻辑
    if not sql_dir.exists() and (Path(args.project_dir) / "create-table.sql").exists():
        print("⚠ 发现单 SQL 文件模式 (create-table.sql)...")
        ensure_changelog_table_exists(o, args.project_dir)
        file_info = {
            'path': str(Path(args.project_dir) / "create-table.sql"),
            'filename': 'create-table.sql',
            'timestamp': datetime.now().strftime('%Y%m%d%H%M%S')
        }
        result = execute_sql_file(o, file_info)
        if result['status'] == 'SUCCESS':
            print("✓ 单文件模式执行完毕")
            return
        else:
            import sys
            sys.exit(1)
            
    if not sql_dir.exists():
        print(f"✗ SQL 目录不存在: {sql_dir}")
        import sys
        sys.exit(1)
        
    print(f"✓ SQL 目录验证通过\n")
    
    execute_sql_migrations(
        o, 
        str(sql_dir), 
        args.project_dir,
        skip_executed=not args.force
    )

if __name__ == "__main__":
    main()
