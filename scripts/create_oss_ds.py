# -*- coding: utf-8 -*-
"""
create_oss_ds.py
职责：根据项目下的 oss-datasource.json 创建 OSS 数据源。
"""

import argparse
import json
import sys
import os
from pathlib import Path

from alibabacloud_dataworks_public20240518 import models as dw_models
from alibabacloud_tea_util import models as util_models

# 引用之前封装的 SDK 初始化函数
from dataworks_client import create_client


from config_merger import load_merged_oss_ds_config

def main():
    # ── 解析命令行参数 ────────────────────────────────────────────
    parser = argparse.ArgumentParser(description="Create DataWorks OSS DataSource")
    parser.add_argument(
        "--project-dir", type=str, default="projects/Test",
        help="项目目录路径，该目录下必须有 oss-datasource.json 文件（全局配置同目录）"
    )
    args = parser.parse_args()

    # ── 读取 OSS 数据源配置（引入合并覆盖逻辑）───────────────
    try:
        config = load_merged_oss_ds_config(args.project_dir)
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    # 从环境变量获取 AK/SK 用于数据源鉴权
    ak = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_ID", "")
    sk = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "")
    if not ak or not sk:
        print("ERROR: ALIBABA_CLOUD_ACCESS_KEY_ID or ALIBABA_CLOUD_ACCESS_KEY_SECRET not set")
        sys.exit(1)

    # 从环境变量获取 Region (如 cn-shanghai)
    region = os.environ.get("ALIYUN_REGION", "cn-shanghai")

    # DataWorks 创建 OSS 数据源要求传入 Endpoint 和 AK/SK 以及 Region等必填信息
    connection_properties = {
        "endpoint": ds_config["endpoint"],
        "bucket": ds_config["bucket"],
        "authType": "Ak", # Ak 代表使用 AccessKey 认证
        "accessId": ak,
        "accessKey": sk,
        "envType": "Prod",  # 基础模式工作空间只支持 Prod 环境
        "regionId": region # DataWorks API 强制要求 regionId
    }

    project_id_str = os.environ.get("DATAWORKS_PROJECT_ID", "")
    if not project_id_str:
        print("ERROR: DATAWORKS_PROJECT_ID not set")
        sys.exit(1)
    project_id = int(project_id_str)

    print(f"Creating OSS DataSource '{config['name']}' in Project {project_id}...")
    
    client = create_client()
    # 构造请求体
    request = dw_models.CreateDataSourceRequest(
        project_id=project_id,
        name=config.get("name"),
        data_source_type="oss",
        description=config.get("description", ""),
        env_type=1,
        connection_properties_mode="UrlMode",
        url=f"{config.get('endpoint')}",
        # string 类型的 content 字段，存放 AK/SK 等凭证信息
        content=json.dumps({
            "accessId": ak,
            "accessKey": sk,
            "bucket": config.get("bucket"),
            "endpoint": config.get("endpoint")
        })
    )
    
    try:
        resp = client.create_data_source_with_options(request, util_models.RuntimeOptions())
        print(f"✅ OSS DataSource Created successfully. ID: {resp.body.id}")
    except Exception as e:
        print(f"Failed to create DataSource: {e.message if hasattr(e, 'message') else str(e)}")
        # 注: 如果数据源已存在，API 通常会抛错，此处不终止运行。

if __name__ == "__main__":
    main()
