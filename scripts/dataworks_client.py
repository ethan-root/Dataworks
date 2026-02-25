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
    resource_group = config["resource_group"]   # 调度资源组标识符

    # ── 内层：数据集成任务配置（di_job_content）──────────────────
    # 这部分告诉 DataWorks 数据从哪个 OSS 路径读、写到哪个 MaxCompute 表
    di_job_content = {
        "extend": {
            "mode": "wizard",               # 向导模式（固定值）
            "resourceGroup": resource_group  # 数据集成使用的资源组
        },
        "type": "job",
        "version": "2.0",
        "steps": [
            {
                # === Reader：从 OSS 读取 Parquet 文件 ===
                "stepType": "oss",
                "parameter": {
                    "path":       config["reader"]["path"],        # OSS 文件路径（支持通配符）
                    "datasource": config["reader"]["datasource"],  # DataWorks 中配置的数据源名称
                    "column":     [],                              # [] 表示自动推断所有列
                    "fileFormat": config["reader"]["fileFormat"]   # 文件格式（parquet/csv 等）
                },
                "name": "Reader",
                "category": "reader"
            },
            {
                # === Writer：写入 MaxCompute 表 ===
                "stepType": "odps",
                "parameter": {
                    "partition":         config["writer"]["partition"],  # 分区，${bizdate} 是 DataWorks 调度日期变量
                    "truncate":          False,           # 不清空历史数据，追加写入
                    "datasource":        config["writer"]["datasource"],
                    "column":            [],              # [] 表示自动与目标表列对齐
                    "emptyAsNull":       False,           # 空字符串不转为 NULL
                    "table":             config["writer"]["table"],
                    "consistencyCommit": True             # 使用事务提交，保证写入一致性
                },
                "name": "Writer",
                "category": "writer"
            }
        ],
        "setting": {
            "errorLimit": {"record": "0"},           # 允许错误行数为 0（遇错即停）
            "speed": {"throttle": False, "concurrent": 1}  # 不限速，单并发
        }
    }

    # ── 外层：DataWorks 节点调度配置（spec_dict）──────────────────
    # 这部分告诉 DataWorks 节点的名字、定时规则、重跑策略等
    node_name = config["node_name"]
    spec_dict = {
        "version": "1.1.0",
        "kind": "CycleWorkflow",   # 周期调度类型（定时循环运行）
        "spec": {
            "nodes": [
                {
                    "recurrence":    "Normal",   # 正常调度（不暂停、不跳过）
                    "timeout":        0,          # 超时时间 0 = 不限制
                    "instanceMode": "T+1",        # T+1 模式：昨天的数据今天跑
                    "rerunMode":    "Allowed",    # 允许手动重跑
                    "rerunTimes":    0,           # 失败自动重试次数：0 = 不重试
                    "rerunInterval": 180000,      # 重试间隔（毫秒）= 3 分钟

                    # 脚本内容：语言为 json，运行时命令为 DI（数据集成）
                    "script": {
                        "path":     node_name,                                       # 节点在 DataWorks 中的路径
                        "language": "json",
                        "runtime":  {"command": "DI"},                               # DI = Data Integration
                        "content":  json.dumps(di_job_content, ensure_ascii=False)   # 内层 JSON 作为字符串嵌入
                    },

                    # 定时触发配置
                    "trigger": {
                        "type":      "Scheduler",
                        "cron":       config["cron"],               # Cron 表达式（如每天凌晨2点）
                        "startTime": "1970-01-01 00:00:00",         # 从 Unix 纪元开始，即立刻生效
                        "endTime":   "9999-01-01 00:00:00"          # 永不过期
                    },

                    "runtimeResource": {"resourceGroup": resource_group},  # 运行时使用的资源组
                    "name":  node_name,
                    "owner": config["owner"]    # 负责人 UID（DataWorks 账号 ID）
                }
            ],
            "flow": []   # 节点间依赖关系，当前只有一个节点，所以为空
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
