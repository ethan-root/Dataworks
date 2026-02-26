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


def main():
    parser = argparse.ArgumentParser(description="Create OSS DataSource")
    parser.add_argument("--project-dir", type=str, default="projects/Test", help="项目目录路径")
    args = parser.parse_args()

    config_path = Path(args.project_dir) / "oss-datasource.json"
    if not config_path.exists():
        print(f"ERROR: {config_path} not found.")
        sys.exit(1)

    with open(config_path, "r", encoding="utf-8") as f:
        ds_config = json.load(f)

    # 从环境变量获取 AK/SK 用于数据源鉴权
    ak = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_ID", "")
    sk = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "")
    if not ak or not sk:
        print("ERROR: ALIBABA_CLOUD_ACCESS_KEY_ID or ALIBABA_CLOUD_ACCESS_KEY_SECRET not set")
        sys.exit(1)

    # DataWorks 创建 OSS 数据源要求传入 Endpoint 和 AK/SK
    connection_properties = {
        "endpoint": ds_config["endpoint"],
        "bucket": ds_config["bucket"],
        "accessId": ak,
        "accessKey": sk
    }

    project_id_str = os.environ.get("DATAWORKS_PROJECT_ID", "")
    if not project_id_str:
        print("ERROR: DATAWORKS_PROJECT_ID not set")
        sys.exit(1)
    project_id = int(project_id_str)

    print(f"Creating OSS DataSource '{ds_config['name']}' in Project {project_id}...")
    
    client = create_client()
    request = dw_models.CreateDataSourceRequest(
        project_id=project_id,
        name=ds_config["name"],
        type="oss",  # 数据源类型标识
        env_type="Dev",  # DataWorks 标准模式下需指定是开发(Dev)还是生产(Prod)环境数据源
        connection_properties_mode="UrlMode", # UrlMode 或 InstanceMode
        connection_properties=json.dumps(connection_properties, ensure_ascii=False),
        description=ds_config.get("description", "")
    )
    
    try:
        resp = client.create_data_source_with_options(request, util_models.RuntimeOptions())
        print(f"✅ OSS DataSource Created successfully. ID: {resp.body.id}")
    except Exception as e:
        print(f"Failed to create DataSource: {e.message if hasattr(e, 'message') else str(e)}")
        # 注: 如果数据源已存在，API 通常会抛错，此处不终止运行。

if __name__ == "__main__":
    main()
