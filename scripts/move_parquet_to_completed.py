# -*- coding: utf-8 -*-
import logging
import json
import oss2
import argparse


def move_to_completed(access_key_id, access_key_secret, endpoint, bucket_name, file_path):
    """
    将 OSS 文件移动到最上级目录的 completed 下

    例如:
      camos/user_feature/user_feature_2026031715.parquet
      → camos/completed/user_feature/user_feature_2026031715.parquet

      camos/order_feature/order_feature_2026031715.parquet
      → camos/completed/order_feature/order_feature_2026031715.parquet
    """
    auth = oss2.Auth(access_key_id, access_key_secret)
    bucket = oss2.Bucket(auth, endpoint, bucket_name)

    # 拆分路径
    parts = file_path.split('/')
    # parts[0] = camos（最上级目录）
    # parts[1] = user_feature（子目录）
    # parts[2:] = 文件名（可能有更深层级）

    if len(parts) < 3:
        print(f"✗ 路径层级不足: {file_path}")
        return None

    # 构建目标路径: 最上级目录/completed/剩余路径
    dest_path = f"{parts[0]}/completed/{'/'.join(parts[1:])}"

    print(f"源文件: {file_path}")
    print(f"目标路径: {dest_path}")

    # 复制文件到目标路径
    bucket.copy_object(bucket_name, file_path, dest_path)
    print("✓ 文件复制成功")

    # 删除源文件
    bucket.delete_object(file_path)
    print("✓ 源文件已删除")

    print(f"✓ 文件已移动到: {dest_path}")
    return dest_path


def main():
    parser = argparse.ArgumentParser(description='将 OSS 文件移动到 completed 目录')
    parser.add_argument('--access-id', default='YOUR_ACCESS_KEY', help='AccessKey ID')
    parser.add_argument('--secret-key', default='YOUR_SECRET_KEY', help='AccessKey Secret')
    parser.add_argument('--endpoint', default='oss-cn-shanghai-internal.aliyuncs.com', help='OSS endpoint')
    parser.add_argument('--bucket', default='kering-batch-data', help='Bucket 名称')
    parser.add_argument('--file-path', default='camos/user_feature/user_feature_2026031715.parquet', help='OSS 文件路径，如 camos/user_feature/xxx.parquet')

    args = parser.parse_args()

    move_to_completed(
        args.access_id,
        args.secret_key,
        args.endpoint,
        args.bucket,
        args.file_path
    )


def handler(event, context):
    # evt = json.loads(event)
    main()
    logger = logging.getLogger()
    logger.info('hello world')
    return 'hello world'
