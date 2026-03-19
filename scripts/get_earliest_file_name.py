# -*- coding: utf-8 -*-
import logging
import json
import oss2
import re
from datetime import datetime
import argparse


def get_earliest_parquet_file(access_key_id, access_key_secret, endpoint, bucket_name, prefix):
    """
    获取 OSS 目录下最早的 parquet 文件
    
    参数:
        access_key_id: AccessKey ID
        access_key_secret: AccessKey Secret
        endpoint: OSS endpoint
        bucket_name: Bucket 名称
        prefix: 目录前缀，如 'data/user_feature/'
        
    返回:
        最早文件的完整路径（包括文件名）
    """
    # 创建 OSS 客户端
    auth = oss2.Auth(access_key_id, access_key_secret)
    bucket = oss2.Bucket(auth, endpoint, bucket_name)
    
    # 存储文件信息
    parquet_files = []
    
    # 列出目录下的所有文件
    for obj in oss2.ObjectIterator(bucket, prefix=prefix):
        file_name = obj.key
        
        # 只处理 .parquet 文件
        if file_name.endswith('.parquet'):
            # 提取文件名（不含路径）
            base_name = file_name.split('/')[-1]
            
            # 使用正则提取时间戳：user_feature_yyyymmddHH.parquet
            match = re.search(r'_(\d{10})\.parquet$', base_name)
            
            if match:
                timestamp_str = match.group(1)  # 提取 yyyymmddHH
                
                try:
                    # 解析时间戳
                    timestamp = datetime.strptime(timestamp_str, '%Y%m%d%H')
                    
                    parquet_files.append({
                        'path': file_name,
                        'timestamp': timestamp,
                        'timestamp_str': timestamp_str
                    })
                    
                    print(f"找到文件: {file_name} (时间: {timestamp})")
                    
                except ValueError:
                    print(f"⚠ 跳过文件（时间格式错误）: {file_name}")
    
    # 检查是否找到文件
    if not parquet_files:
        print("✗ 未找到任何 parquet 文件")
        return None
    
    # 按时间排序，获取最早的文件
    earliest_file = min(parquet_files, key=lambda x: x['timestamp'])
    
    print(f"\n✓ 最早的文件:")
    print(f"  路径: {earliest_file['path']}")
    print(f"  时间: {earliest_file['timestamp']}")
    
    return earliest_file['path']

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='获取 OSS 目录下最早的 parquet 文件')
    parser.add_argument('--access-id', default='YOUR_ACCESS_KEY', help='AccessKey ID')
    parser.add_argument('--secret-key', default='YOUR_SECRET_KEY', help='AccessKey Secret')
    parser.add_argument('--endpoint', default="oss-cn-shanghai-internal.aliyuncs.com", help='OSS endpoint,如 oss-cn-shanghai.aliyuncs.com')
    parser.add_argument('--bucket', default="kering-batch-data", help='Bucket 名称')
    parser.add_argument('--prefix', default="camos/user_feature/", help='目录前缀，如 data/user_feature/')
    
    args = parser.parse_args()
    
    # 获取最早的文件
    earliest_file = get_earliest_parquet_file(
        args.access_id,
        args.secret_key,
        args.endpoint,
        args.bucket,
        args.prefix
    )
    
    if earliest_file:
        print(f"\n完整路径: {earliest_file}")
        return earliest_file
    else:
        return None

def handler(event, context):
    # evt = json.loads(event)
    main()
    logger = logging.getLogger()
    logger.info('hello world')
    return 'hello world'
