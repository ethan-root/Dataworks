# -*- coding: utf-8 -*-
"""
create_mc_ds.py
职责：根据项目下的 maxcompute-datasource.json 创建 MaxCompute(odps) 数据源。

认证方式对应 DataWorks 控制台：
  认证方式    : 阿里云账号及阿里云RAM角色  → authType = "AliyunAccount"
  所属云账号  : 当前阿里云主账号           → accountType 字段（不传默认为主账号）
  默认访问身份: 阿里云RAM子账号            → subAccount 字段
  Endpoint   : 自动适配                  → 不传 endpoint 或传空字符串
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

    # 认证方式：阿里云账号及阿里云RAM角色（对应 UI 中「当前阿里云主账号 + RAM子账号」模式）
    # authType = "AliyunAccount" 时不需要传入 AK/SK，由工作空间绑定的主账号权限接管。
    # subAccount 对应 UI 中「阿里云子账号」字段（如 kering-dataworks）。
    connection_properties = {
        "project":     ds_config["project"],
        "authType":    "AliyunAccount",
        "envType":     "Prod",
        "regionId":    region,
    }
    # 子账号由工作空间账号绑定关系自动确定，不需要在 connection_properties 中显式传入
    # if ds_config.get("subAccount"):
    #     connection_properties["subAccount"] = ds_config["subAccount"]
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
