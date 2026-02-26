# -*- coding: utf-8 -*-
"""
create_mc_ds.py
职责：根据项目下的 maxcompute-datasource.json 创建 MaxCompute(odps) 数据源。

认证方式：AliyunAccount（阿里云账号及阿里云RAM角色）
  对应控制台：认证方式=阿里云账号及阿里云RAM角色，所属云账号=当前主账号，Endpoint=自动适配
  关键点：不传 endpoint / endpointMode，让 DataWorks 自动适配地域 endpoint

历史失败记录（供排查参考）：
  - subAccount 字段              → "无法被识别"
  - authType=AliyunAccount       → 之前同时传了 endpointMode 导致冲突
  - authType=Ak (无凭证)         → "Ak not allowed"
  - authType=ACCESS_KEY+凭证字段 → 各种字段名均被拒绝
  - InstanceMode                 → "odps不支持该ConnectionPropertiesMode"
"""

import argparse
import json
import sys
import os
from pathlib import Path

from alibabacloud_dataworks_public20240518 import models as dw_models
from alibabacloud_tea_util import models as util_models

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

    region = os.environ.get("ALIYUN_REGION", "cn-shanghai")

    project_id_str = os.environ.get("DATAWORKS_PROJECT_ID", "")
    if not project_id_str:
        print("ERROR: DATAWORKS_PROJECT_ID not set")
        sys.exit(1)
    project_id = int(project_id_str)

    # 对应控制台手动创建的参数：
    #   认证方式 = 阿里云账号及阿里云RAM角色
    #   所属云账号 = 当前阿里云主账号
    #   Endpoint = 自动适配（不传 endpoint/endpointMode，DataWorks 按 regionId 自动解析）
    connection_properties = {
        "project":  ds_config["project"],
        "authType": "AliyunAccount",
        "envType":  "Prod",
        "regionId": region,
    }

    print(f"Creating MaxCompute DataSource '{ds_config['name']}' in Project {project_id}...")
    print(f"  connection_properties: {json.dumps(connection_properties)}")

    client = create_client()
    request = dw_models.CreateDataSourceRequest(
        project_id=project_id,
        name=ds_config["name"],
        type="odps",
        connection_properties_mode="UrlMode",
        connection_properties=json.dumps(connection_properties, ensure_ascii=False),
        description=ds_config.get("description", "")
    )

    try:
        resp = client.create_data_source_with_options(request, util_models.RuntimeOptions())
        print(f"✅ MaxCompute DataSource Created successfully. ID: {resp.body.id}")
    except Exception as e:
        msg = e.message if hasattr(e, 'message') else str(e)
        print(f"❌ Failed to create DataSource: {msg}")
        sys.exit(1)

if __name__ == "__main__":
    main()
