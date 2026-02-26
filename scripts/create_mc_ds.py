# -*- coding: utf-8 -*-
"""
create_mc_ds.py
职责：根据项目下的 maxcompute-datasource.json 创建 MaxCompute(odps) 数据源。

认证方式：authType = "Ak"，与 OSS 数据源保持一致。
  accessId / accessKey 从环境变量注入，DataWorks 用这组凭证访问 MaxCompute。

历史尝试（均被 API 拒绝）：
  - subAccount 字段        → "无法被识别"
  - authType=AliyunAccount → "当前数据源不支持该authType"
  - authType=Ak 不带凭证  → "Ak not allowed"
  结论：必须使用 authType=Ak 并显式传入 accessId/accessKey
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

    # 从环境变量获取 AK/SK（与 OSS 数据源保持一致的认证方式）
    ak = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_ID", "")
    sk = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "")
    if not ak or not sk:
        print("ERROR: ALIBABA_CLOUD_ACCESS_KEY_ID or ALIBABA_CLOUD_ACCESS_KEY_SECRET not set")
        sys.exit(1)

    region = os.environ.get("ALIYUN_REGION", "cn-shanghai")

    # authType=ACCESS_KEY：DataWorks API 要求全大写，区分大小写
    # "Ak" / "AccessKey" / "AliyunAccount" 均被 API 拒绝
    connection_properties = {
        "project":         ds_config["project"],
        "authType":        "ACCESS_KEY",
        "accessKeyId":     ak,
        "accessKeySecret": sk,
        "envType":         "Prod",
        "regionId":        region,
        "endpointMode":    ds_config.get("endpointMode", "public"),  # 必填：public / vpc / intranet
    }
    # Endpoint（配置文件有则指定，否则由 DataWorks 自动适配）
    if ds_config.get("endpoint"):
        connection_properties["endpoint"] = ds_config["endpoint"]

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
