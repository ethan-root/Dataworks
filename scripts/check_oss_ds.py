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

    # 第二种可能：返回体就是一个字典（部分版本 SDK）
    try:
        body_map = resp_body.to_map()
    except Exception:
        body_map = resp_body if isinstance(resp_body, dict) else {}

    # 从 data 嵌套字典提取
    data_map = body_map.get("data", {})
    if isinstance(data_map, dict):
        for key in ("dataSources", "data_sources"):
            val = data_map.get(key)
            if isinstance(val, list):
                return val

    # 防止外层就有数据
    for key in ("dataSources", "data_sources"):
        val = body_map.get(key)
        if isinstance(val, list):
            return val

    return []


def _find_datasource(client, project_id: int, ds_name: str):
    """先按 name 查询，若未命中再走不带 name 的全量兜底查询。"""
    runtime = util_models.RuntimeOptions()

    req_by_name = dw_models.ListDataSourcesRequest(
        project_id=project_id,
        name=ds_name
    )
    resp = client.list_data_sources_with_options(req_by_name, runtime)
    sources = _extract_data_sources(resp.body)

    for ds in sources:
        name = ds.get("name", "") if isinstance(ds, dict) else getattr(ds, "name", "")
        if name == ds_name:
            return ds

    # 部分环境下 name 过滤可能漏检，兜底做一次无 filter 全量查询
    try:
        req_all = dw_models.ListDataSourcesRequest(project_id=project_id, page_size=100) # 这里加 page_size 防分页
        resp_all = client.list_data_sources_with_options(req_all, runtime)
        all_sources = _extract_data_sources(resp_all.body)
        for ds in all_sources:
            name = ds.get("name", "") if isinstance(ds, dict) else getattr(ds, "name", "")
            if name == ds_name:
                return ds
    except Exception as e:
        print(f"WARN: Fallback list all data sources failed: {e}")

    return None

def main():
    parser = argparse.ArgumentParser(description="Check DataWorks OSS DataSource")
    parser.add_argument(
        "--project-dir", type=str, required=True,
        help="项目目录路径"
    )
    parser.add_argument(
        "--env", type=str, required=True,
        help="环境名称"
    )
    args = parser.parse_args()

    try:
    # 获取配置
        config = load_merged_oss_ds_config(args.project_dir, args.env)
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
    
    try:
        ds = _find_datasource(client, project_id, ds_name)
        if ds is None:
            print(f"❌ OSS DataSource '{ds_name}' does not exist (not found in list).")
            sys.exit(1)
        ds_id = ds.get("id", "Unknown") if isinstance(ds, dict) else getattr(ds, "id", "Unknown")
        print(f"✅ OSS DataSource '{ds_name}' exists. ID: {ds_id}")
            
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
