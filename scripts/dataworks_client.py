# -*- coding: utf-8 -*-
"""
dataworks_client.py

参考代码直接翻译版本：
  - create_client()  → DataWorksClient.__init__()，从环境变量读取 AK
  - build_spec()     → 与参考代码中的 di_job_content + spec_dict 完全对齐
  - create_node()    → client.create_node_with_options()，原样错误输出
"""

import json
import os
import sys

from alibabacloud_dataworks_public20240518.client import Client as DataWorksPublicClient
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_dataworks_public20240518 import models as dw_models
from alibabacloud_tea_util import models as util_models


def create_client() -> DataWorksPublicClient:
    """
    从环境变量初始化 DataWorks Client（对应参考代码 create_client()）。
    环境变量：
      ALIBABA_CLOUD_ACCESS_KEY_ID
      ALIBABA_CLOUD_ACCESS_KEY_SECRET
      ALIYUN_REGION
    """
    access_key_id     = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_ID", "")
    access_key_secret = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "")
    region            = os.environ.get("ALIYUN_REGION", "cn-shanghai")

    if not access_key_id or not access_key_secret:
        print("ERROR: ALIBABA_CLOUD_ACCESS_KEY_ID / ALIBABA_CLOUD_ACCESS_KEY_SECRET not set")
        sys.exit(1)

    config = open_api_models.Config(
        access_key_id=access_key_id,
        access_key_secret=access_key_secret,
    )
    config.endpoint = f"dataworks.{region}.aliyuncs.com"
    return DataWorksPublicClient(config)


def build_spec(config: dict) -> str:
    """
    根据 config.json 构建 CreateNode 所需的 spec JSON 字符串。

    与参考代码完全对齐：
      di_job_content → 来自 config["reader"] / config["writer"] / config["resource_group"]
      spec_dict      → 来自 config 其余字段
    """
    resource_group = config["resource_group"]

    # ---- di_job_content（对应参考代码中的同名变量）----
    di_job_content = {
        "extend": {
            "mode": "wizard",
            "resourceGroup": resource_group
        },
        "type": "job",
        "version": "2.0",
        "steps": [
            {
                "stepType": "oss",
                "parameter": {
                    "path":       config["reader"]["path"],
                    "datasource": config["reader"]["datasource"],
                    "column":     [],
                    "fileFormat": config["reader"]["fileFormat"]
                },
                "name": "Reader",
                "category": "reader"
            },
            {
                "stepType": "odps",
                "parameter": {
                    "partition":        config["writer"]["partition"],
                    "truncate":         False,
                    "datasource":       config["writer"]["datasource"],
                    "column":           [],
                    "emptyAsNull":      False,
                    "table":            config["writer"]["table"],
                    "consistencyCommit": True
                },
                "name": "Writer",
                "category": "writer"
            }
        ],
        "setting": {
            "errorLimit": {"record": "0"},
            "speed": {"throttle": False, "concurrent": 1}
        }
    }

    # ---- spec_dict（对应参考代码中的同名变量）----
    node_name = config["node_name"]
    spec_dict = {
        "version": "1.1.0",
        "kind": "CycleWorkflow",
        "spec": {
            "nodes": [
                {
                    "recurrence":    "Normal",
                    "timeout":        0,
                    "instanceMode": "T+1",
                    "rerunMode":    "Allowed",
                    "rerunTimes":    0,
                    "rerunInterval": 180000,
                    "script": {
                        "path":     node_name,
                        "language": "json",
                        "runtime":  {"command": "DI"},
                        "content":  json.dumps(di_job_content, ensure_ascii=False)
                    },
                    "trigger": {
                        "type":      "Scheduler",
                        "cron":       config["cron"],
                        "startTime": "1970-01-01 00:00:00",
                        "endTime":   "9999-01-01 00:00:00"
                    },
                    "runtimeResource": {"resourceGroup": resource_group},
                    "name":  node_name,
                    "owner": config["owner"]
                }
            ],
            "flow": []
        }
    }

    return json.dumps(spec_dict, ensure_ascii=False)


def create_node(client: DataWorksPublicClient, config: dict, project_id: int) -> None:
    """
    调用 CreateNode API（对应参考代码 main() 中的 create_node_with_options）。
    错误处理与参考代码完全一致。
    """
    spec_json = build_spec(config)

    create_node_request = dw_models.CreateNodeRequest(
        project_id=project_id,
        spec=spec_json,
        scene="DATAWORKS_PROJECT"
    )
    runtime = util_models.RuntimeOptions()

    try:
        resp = client.create_node_with_options(create_node_request, runtime)
        print(json.dumps(resp.body.to_map(), indent=2, ensure_ascii=False))
    except Exception as error:
        # 与参考代码完全一致的错误输出
        print(error.message)
        print(error.data.get("Recommend"))
        raise
