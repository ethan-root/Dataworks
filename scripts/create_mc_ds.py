# -*- coding: utf-8 -*-
"""
create_mc_ds.py
职责：根据项目下的 maxcompute-datasource.json 创建 MaxCompute(odps) 数据源。

认证方式：InstanceMode —— 不传 AK/SK，由 DataWorks 工作空间绑定的账号权限自动鉴权。

历史尝试（均被 API 拒绝，结论：connection_properties 不接受任何凭证字段）：
  - authType=AliyunAccount          → "不支持该authType"
  - authType=Ak (无凭证)            → "Ak not allowed"
  - authType=ACCESS_KEY+accessKeyId → "accessKeyId无法被识别"
  - authType=ACCESS_KEY+access_key_id → "access_key_id无法被识别"
  结论：切换 InstanceMode，让工作空间统一接管 MaxCompute 鉴权
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

    # InstanceMode：不传 AK/SK，DataWorks 通过工作空间绑定账号自动鉴权
    # 所有显式传入凭证字段的尝试均被 API 拒绝（见模块注释）
    connection_properties = {
        "project":  ds_config["project"],
        "envType":  "Prod",
        "regionId": region,
    }
    if ds_config.get("endpoint"):
        connection_properties["endpoint"] = ds_config["endpoint"]

    print(f"Creating MaxCompute DataSource '{ds_config['name']}' in Project {project_id}...")
    print(f"  connection_properties_mode: InstanceMode")
    print(f"  project: {ds_config['project']}")

    client = create_client()
    request = dw_models.CreateDataSourceRequest(
        project_id=project_id,
        name=ds_config["name"],
        type="odps",
        connection_properties_mode="InstanceMode",  # 工作空间实例模式，无需显式传凭证
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
