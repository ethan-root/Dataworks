# -*- coding: utf-8 -*-
"""
create_mc_ds.py
职责：根据项目下的 maxcompute-datasource.json 创建 MaxCompute(odps) 数据源。
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
    parser = argparse.ArgumentParser(description="Create MaxCompute DataSource")
    parser.add_argument("--project-dir", type=str, default="projects/Test", help="项目目录路径")
    args = parser.parse_args()

    config_path = Path(args.project_dir) / "maxcompute-datasource.json"
    if not config_path.exists():
        print(f"ERROR: {config_path} not found.")
        sys.exit(1)

    with open(config_path, "r", encoding="utf-8") as f:
        ds_config = json.load(f)

    # 从环境变量获取 Region (如 cn-shanghai)
    region = os.environ.get("ALIYUN_REGION", "cn-shanghai")

    # DataWorks 标准/基础模式工作空间不允许 authType=Ak（禁止在数据源中存储 AK/SK）。
    # authType 为必填项，但要使用工作空间自身的 RAM 角色授权，需设置为字符串 "None"。
    # 此时数据源的访问权限由工作空间绑定的 RAM 角色统一接管，无需传入 accessId/accessKey。
    connection_properties = {
        "project": ds_config["project"],
        "endpoint": ds_config["endpoint"],
        "endpointMode": ds_config.get("endpointMode", "Public"),  # Public / Inner / VPC
        "authType": "None",  # 使用工作空间 RAM 角色，不单独注入 AK/SK
        "envType": "Prod",
        "regionId": region
    }

    project_id_str = os.environ.get("DATAWORKS_PROJECT_ID", "")
    if not project_id_str:
        print("ERROR: DATAWORKS_PROJECT_ID not set")
        sys.exit(1)
    project_id = int(project_id_str)

    print(f"Creating MaxCompute DataSource '{ds_config['name']}' in Project {project_id}...")
    
    client = create_client()
    request = dw_models.CreateDataSourceRequest(
        project_id=project_id,
        name=ds_config["name"],
        type="odps",  # DataWorks 里叫 odps
        connection_properties_mode="UrlMode", # UrlMode 或 InstanceMode
        connection_properties=json.dumps(connection_properties, ensure_ascii=False),
        description=ds_config.get("description", "")
    )
    
    try:
        resp = client.create_data_source_with_options(request, util_models.RuntimeOptions())
        print(f"✅ MaxCompute DataSource Created successfully. ID: {resp.body.id}")
    except Exception as e:
        msg = e.message if hasattr(e, 'message') else str(e)
        print(f"❌ Failed to create DataSource: {msg}")
        sys.exit(1)  # 数据源创建失败必须终止，否则后续 create_node 因无数据源而静默失败

if __name__ == "__main__":
    main()
