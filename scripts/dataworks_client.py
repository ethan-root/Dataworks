# -*- coding: utf-8 -*-
"""
dataworks_client.py
职责：封装阿里云 DataWorks API 的调用逻辑。

对外提供三个函数：
  - create_client()  : 初始化 DataWorks SDK 客户端
  - build_spec()     : 把 config.json 的配置转换成 DataWorks 节点所需的 JSON 格式
  - create_node()    : 调用 DataWorks API 创建定时同步节点
"""

import json
import os
import sys

# DataWorks 官方 Python SDK（2024-05-18 版本）
from alibabacloud_dataworks_public20240518.client import Client as DataWorksPublicClient
# SDK 通用配置（endpoint、AK 等）
from alibabacloud_tea_openapi import models as open_api_models
# DataWorks 请求/响应模型
from alibabacloud_dataworks_public20240518 import models as dw_models
# SDK 运行时参数（超时、重试等）
from alibabacloud_tea_util import models as util_models


# ─────────────────────────────────────────────────────
# 函数一：初始化 DataWorks 客户端
# ─────────────────────────────────────────────────────
def create_client() -> DataWorksPublicClient:
    """
    从环境变量读取访问凭证，初始化并返回 DataWorks SDK 客户端。

    所需环境变量（在 GitHub Actions 中通过 secrets 注入）：
      ALIBABA_CLOUD_ACCESS_KEY_ID      : 阿里云 AccessKey ID
      ALIBABA_CLOUD_ACCESS_KEY_SECRET  : 阿里云 AccessKey Secret
      ALIYUN_REGION                    : 阿里云地域（如 cn-shanghai）
    """
    access_key_id     = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_ID", "")
    access_key_secret = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "")
    region            = os.environ.get("ALIYUN_REGION", "cn-shanghai")

    # 如果凭证为空，立即报错退出，避免后续调用报奇怪的错误
    if not access_key_id or not access_key_secret:
        print("ERROR: ALIBABA_CLOUD_ACCESS_KEY_ID / ALIBABA_CLOUD_ACCESS_KEY_SECRET not set")
        sys.exit(1)

    # 构建 SDK 配置对象
    config = open_api_models.Config(
        access_key_id=access_key_id,
        access_key_secret=access_key_secret,
    )
    # DataWorks 的 API 地址格式固定为：dataworks.<region>.aliyuncs.com
    config.endpoint = f"dataworks.{region}.aliyuncs.com"

    return DataWorksPublicClient(config)


# ─────────────────────────────────────────────────────
# 函数二：构建 CreateNode 所需的 spec JSON
# ─────────────────────────────────────────────────────
def build_spec(config: dict) -> str:
    """
    把 config.json 的配置转换成 DataWorks CreateNode API 所要求的 spec JSON 字符串。

    DataWorks CreateNode 的 spec 是两层嵌套 JSON：
      外层（spec_dict）：描述节点调度配置（定时、重跑策略等）
      内层（di_job_content）：描述数据集成任务（读哪里、写哪里）

    Args:
        config: 从 config.json 读取的字典
    Returns:
        spec_json: JSON 字符串，直接传给 CreateNodeRequest.spec
    """
    resource_group = config.get("resource_group", "")

    # ── 内层：数据集成任务配置（di_job_content）──────────────────
    di_job_content = {
        "extend": {
            "mode": "wizard",
            "resourceGroup": resource_group,
            "oneStopPageNum": config.get("oneStopPageNum", 2),
            "cu": config.get("cu", 0.5)
        },
        "transform": config.get("transform", False),
        "type": "job",
        "version": "2.0",
        "steps": [
            {
                "stepType": "oss",
                "copies": 1,
                "parameter": {
                    "path":       config.get("reader", {}).get("path", ""),
                    "envType":    config.get("reader", {}).get("envType", 1),
                    "datasource": config.get("reader", {}).get("datasource", ""),
                    "column":     config.get("reader", {}).get("column", []),
                    "fileFormat": config.get("reader", {}).get("fileFormat", "parquet")
                },
                "name": "Reader",
                "category": "reader"
            },
            {
                "stepType": "odps",
                "copies": 1,
                "parameter": {
                    "partition":         config.get("writer", {}).get("partition", ""),
                    "truncate":          config.get("writer", {}).get("truncate", False),
                    "envType":           config.get("writer", {}).get("envType", 1),
                    "datasource":        config.get("writer", {}).get("datasource", ""),
                    "isSupportThreeModel": config.get("writer", {}).get("isSupportThreeModel", False),
                    "tunnelQuota":       config.get("writer", {}).get("tunnelQuota", "default"),
                    "column":            config.get("writer", {}).get("column", []),
                    "emptyAsNull":       config.get("writer", {}).get("emptyAsNull", False),
                    "tableComment":      config.get("writer", {}).get("tableComment", "null"),
                    "consistencyCommit": config.get("writer", {}).get("consistencyCommit", True),
                    "table":             config.get("writer", {}).get("table", "")
                },
                "name": "Writer",
                "category": "writer"
            }
        ],
        "order": {
            "hops": config.get("hops", [])
        },
        "setting": {
            "errorLimit": {"record": "0"},
            "locale": "zh_CN",
            "speed": {"throttle": False, "concurrent": 1}
        }
    }

    # ── 外层：DataWorks 节点调度配置（spec_dict）──────────────────
    node_name = config.get("node_name", "")
    
    script_content = {
        "path": node_name,
        "language": "json",
        "runtime": {
            "command": "DI",
            "commandTypeId": config.get("script_commandTypeId", 23),
            "cu": str(config.get("script_cu", "0.25"))
        },
        "content": json.dumps(di_job_content, ensure_ascii=False)
    }
    
    if "parameters" in config:
        script_content["parameters"] = config["parameters"]

    trigger_config = {
        "type": "Scheduler",
        "cron": config.get("cron", "00 00 00-23/1 * * ?"),
        "startTime": config.get("startTime", "1970-01-01 00:00:00"),
        "endTime": config.get("endTime", "9999-01-01 00:00:00"),
        "timezone": config.get("timezone", "Asia/Shanghai"),
        "delaySeconds": config.get("delaySeconds", 0)
    }
    if "cycleType" in config:
        trigger_config["cycleType"] = config["cycleType"]

    runtime_resource = {
        "resourceGroup": resource_group
    }
    if "resourceGroupId" in config:
        runtime_resource["resourceGroupId"] = config["resourceGroupId"]
    if "resourceGroupName" in config:
        runtime_resource["resourceGroupName"] = config["resourceGroupName"]

    node_def = {
        "recurrence": "Normal",
        "maxInternalConcurrency": config.get("maxInternalConcurrency", 0),
        "timeout": config.get("timeout", 0),
        "timeoutUnit": config.get("timeoutUnit", "HOURS"),
        "instanceMode": config.get("instanceMode", "Immediately"),
        "rerunMode": config.get("rerunMode", "Allowed"),
        "rerunTimes": config.get("rerunTimes", 0),
        "rerunInterval": config.get("rerunInterval", 180000),
        "autoParse": config.get("autoParse", False),
        "script": script_content,
        "trigger": trigger_config,
        "runtimeResource": runtime_resource,
        "name": node_name,
        "owner": config.get("owner", "")
    }
    
    if "metadata" in config:
        node_def["metadata"] = config["metadata"]

    spec_dict = {
        "version": "1.1.0",
        "kind": "CycleWorkflow",
        "spec": {
            "nodes": [node_def],
            "flow": [{"depends": config.get("depends", [])}] if config.get("depends") else []
        }
    }

    # 返回最终 JSON 字符串（ensure_ascii=False 保留中文字符）
    return json.dumps(spec_dict, ensure_ascii=False)


# ─────────────────────────────────────────────────────
# 函数三：调用 DataWorks API 创建节点
# ─────────────────────────────────────────────────────
def create_node(client: DataWorksPublicClient, config: dict, project_id: int) -> None:
    """
    调用 DataWorks CreateNode API，在指定工作空间中创建定时数据同步节点。

    Args:
        client:     由 create_client() 返回的 SDK 客户端
        config:     由 config.json 读取的配置字典
        project_id: DataWorks 工作空间 ID（对应环境变量 DATAWORKS_PROJECT_ID）
    """
    # 第一步：把配置转换成 spec JSON 字符串
    spec_json = build_spec(config)

    # 第二步：构建 API 请求对象
    create_node_request = dw_models.CreateNodeRequest(
        project_id=project_id,          # DataWorks 工作空间 ID
        spec=spec_json,                  # 上面生成的节点规格 JSON
        scene="DATAWORKS_PROJECT"        # 固定值：在 DataWorks 项目中创建
    )
    runtime = util_models.RuntimeOptions()   # 使用默认运行时参数（超时、重试）

    # 第三步：调用 API
    try:
        resp = client.create_node_with_options(create_node_request, runtime)
        # 成功：打印返回结果（包含 NodeId）
        print(json.dumps(resp.body.to_map(), indent=2, ensure_ascii=False))
    except Exception as error:
        # 失败：打印错误信息和阿里云故障排查链接
        print(error.message)
        print(error.data.get("Recommend"))
        raise   # 向上抛出，让 GitHub Actions 看到失败
