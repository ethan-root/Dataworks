# -*- coding: utf-8 -*-
"""
create_mc_ds.py
职责：根据项目下的 maxcompute-datasource.json 创建 MaxCompute(odps) 数据源。

本脚本基于 DataWorks OpenAPI（2024-05-18）编写。
由于历史尝试发现该 API 对 ConnectionProperties 要求极高，并且各种 AK/SK 注入方式均失败，
我们将采取直接利用 DataWorks 后端自动解析能力的做法：
1. authType: 对应 UI 的“阿里云账号及阿里云RAM角色”，在此 API 中，如果选了本账号，通常不需要传 authType 也能依靠工作空间绑定角色自动鉴权，但为了兼容性，我们将使用明确的枚举（如果不行则省略跳过该字段）。
2. 根据 UI "自动适配"，不传 endpoint / endpointMode。
3. 之前报错“不支持当前authType”，由于 OpenAPI 2024 版本更新，部分字段的枚举可能有未在文档中明说的变动。
   最安全的做法是：如果是同一个阿里云账号，最小化 connection_properties 参数。
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

    # 最小化参数集合：仅传递必要属性。
    # 根据最新的 2024-05-18 API 和通用配置文档：
    # MaxCompute (odps) 数据源在同账号下可以主要靠 envType, project。
    # 我们测试仅传入必要的核心参数，规避由于字段格式或枚举不支持导致的 HTTP 400 错误。
    connection_properties = {
        "project": ds_config["project"],
        "envType": "Prod",
        "regionId": region,
        "endpointMode": "SelfAdaption"  # 控制台界面的“自动适配”
    }
    
    # 根据官方论坛和工单经验，跨账号才需要复杂的 authType (如 RamRole)。
    # 本账号直接访问时，"authType" 可以尝试指定为 "PrimaryAccount" （在前面的通用参数查询中查到）。
    # 或者如果不传，后端可能会默认取。我们先明确传入 "PrimaryAccount" 试试看。
    connection_properties["authType"] = "PrimaryAccount"

    print(f"Creating MaxCompute DataSource '{ds_config['name']}' in Project {project_id}...")
    print(f"  Request connection_properties: {json.dumps(connection_properties, ensure_ascii=False)}")

    client = create_client()
    request = dw_models.CreateDataSourceRequest(
        project_id=project_id,
        name=ds_config["name"],
        type="odps",  # API 规定 MaxCompute 是 odps
        connection_properties_mode="UrlMode", # 回退到 UrlMode，InstanceMode 之前报错不支持 odps
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
