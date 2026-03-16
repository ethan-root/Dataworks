# -*- coding: utf-8 -*-
"""
check_oss_ds.py
职责：检查指定的 OSS 数据源是否存在。
由于无法确定精确的 OpenAPI GetDataSource 方法参数，我们在本检查脚本中采用 List 尝试机制。
"""

import argparse
import sys
import os
from pathlib import Path

from alibabacloud_dataworks_public20240518 import models as dw_models
from alibabacloud_tea_util import models as util_models

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from dataworks_client import create_client
from config_merger import load_merged_oss_ds_config


def _extract_data_sources(resp_body):
    """兼容不同 SDK 返回结构，安全提取 data source 列表。"""
    if not resp_body:
        return []

    direct = getattr(resp_body, "data_sources", None)
    if isinstance(direct, list):
        return direct

    nested = getattr(resp_body, "data", None)
    if nested is not None:
        nested_sources = getattr(nested, "data_sources", None)
        if isinstance(nested_sources, list):
            return nested_sources

    # 最后回退到 map 结构兜底，避免模型字段名差异
    try:
        body_map = resp_body.to_map()
    except Exception:
        body_map = {}

    for key in ("dataSources", "data_sources"):
        val = body_map.get(key)
        if isinstance(val, list):
            return val

    data_map = body_map.get("data")
    if isinstance(data_map, dict):
        for key in ("dataSources", "data_sources"):
            val = data_map.get(key)
            if isinstance(val, list):
                return val

    return []

def main():
    parser = argparse.ArgumentParser(description="Check DataWorks OSS DataSource")
    parser.add_argument(
        "--project-dir", type=str, required=True,
        help="项目目录路径"
    )
    args = parser.parse_args()

    try:
        config = load_merged_oss_ds_config(args.project_dir)
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    ds_name = config.get("name")
    if not ds_name:
        print("ERROR: name not found in oss-datasource configuration.")
        sys.exit(1)

    project_id_str = os.environ.get("DATAWORKS_PROJECT_ID", "")
    if not project_id_str:
        print("ERROR: DATAWORKS_PROJECT_ID not set")
        sys.exit(1)
    project_id = int(project_id_str)

    print(f"Checking OSS DataSource '{ds_name}' in Project {project_id}...")
    
    client = create_client()
    request = dw_models.ListDataSourcesRequest(
        project_id=project_id,
        name=ds_name
    )
    
    try:
        resp = client.list_data_sources_with_options(request, util_models.RuntimeOptions())
        data_sources = _extract_data_sources(resp.body)
        
        found = False
        for ds in data_sources:
            # ds 可能是模型对象或 dict，统一兼容读取
            name = ds.get("name", "") if isinstance(ds, dict) else getattr(ds, "name", "")
            ds_id = ds.get("id", "Unknown") if isinstance(ds, dict) else getattr(ds, "id", "Unknown")
            if name == ds_name:
                print(f"✅ OSS DataSource '{ds_name}' exists. ID: {ds_id}")
                found = True
                break
                
        if not found:
            print(f"❌ OSS DataSource '{ds_name}' does not exist (not found in list).")
            sys.exit(1)
            
    except Exception as e:
        msg = e.message if hasattr(e, 'message') else str(e)
        if "NotFound" in msg or "Invalid" in msg:
            print(f"❌ Target Data Source '{ds_name}' might not exist. Error: {msg}")
        else:
            print(f"⚠️ Error while calling ListDataSources API: {msg}")
            print(f"For safety, treating as non-existent.")
        sys.exit(1)

if __name__ == "__main__":
    main()
