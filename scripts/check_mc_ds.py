# -*- coding: utf-8 -*-
"""
check_mc_ds.py
职责：检查指定的 MaxCompute 数据源是否存在。
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
from config_merger import load_merged_mc_ds_config

def main():
    parser = argparse.ArgumentParser(description="Check DataWorks MaxCompute DataSource")
    parser.add_argument(
        "--project-dir", type=str, required=True,
        help="项目目录路径"
    )
    args = parser.parse_args()

    try:
        config = load_merged_mc_ds_config(args.project_dir)
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    ds_name = config.get("name")
    if not ds_name:
        print("ERROR: name not found in maxcompute-datasource configuration.")
        sys.exit(1)

    project_id_str = os.environ.get("DATAWORKS_PROJECT_ID", "")
    if not project_id_str:
        print("ERROR: DATAWORKS_PROJECT_ID not set")
        sys.exit(1)
    project_id = int(project_id_str)

    print(f"Checking MaxCompute DataSource '{ds_name}' in Project {project_id}...")
    
    client = create_client()
    request = dw_models.ListDataSourcesRequest(
        project_id=project_id,
        name=ds_name
    )
    
    try:
        resp = client.list_data_sources_with_options(request, util_models.RuntimeOptions())
        data_sources = resp.body.data_sources if hasattr(resp.body, "data_sources") else getattr(resp.body.data, "data_sources", [])
        
        found = False
        for ds in data_sources:
            if getattr(ds, "name", "") == ds_name:
                print(f"✅ MaxCompute DataSource '{ds_name}' exists. ID: {getattr(ds, 'id', 'Unknown')}")
                found = True
                break
                
        if not found:
            print(f"❌ MaxCompute DataSource '{ds_name}' does not exist (not found in list).")
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
