# -*- coding: utf-8 -*-
"""
dataworks_client.py
职责：封装阿里云 DataWorks API 的调用逻辑。

对外提供五个函数：
  - create_client()  : 初始化 DataWorks SDK 客户端
  - build_spec()     : 把 task-config.json 的配置转换成 DataWorks 节点所需的 JSON 格式
  - create_node()    : 调用 DataWorks API 创建定时同步节点
  - get_node_id()    : 通过节点名精确查找节点，返回 Data Studio 节点 ID
  - update_node()    : 调用 DataWorks API 增量更新已有节点
"""

import json
import os
import sys
import time
import random

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
        print("ERROR: 未设置 ALIBABA_CLOUD_ACCESS_KEY_ID 或 ALIBABA_CLOUD_ACCESS_KEY_SECRET 环境变量")
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
# 辅助函数：带退避重试的 API 调用器
# ─────────────────────────────────────────────────────
def _call_with_retry(func, *args, **kwargs):
    """
    包装 DataWorks API 调用，遇到 Throttling.Resource 限流时自动采用指数退避加随机抖动进行重试。
    最大重试 4 次，延迟分别为 ~1s, ~2s, ~4s, ~8s
    """
    max_retries = 4
    for i in range(max_retries):
        try:
            # 每次请求前默认停顿 0.5 秒缓解基础并发压力
            time.sleep(0.5)
            return func(*args, **kwargs)
        except Exception as error:
            msg = getattr(error, 'message', str(error))
            code = getattr(error, 'code', '')
            
            # 判断是否为限流错误 ("Throttling", "9990040003" 或 HTTP 429)
            is_throttled = ("Throttling" in msg) or ("9990040003" in msg) or ("429" in str(code))
            
            if is_throttled and i < max_retries - 1:
                wait_time = (2 ** i) + random.uniform(0.1, 1.0)
                print(f"   [WARN] API 限流 (Throttling.Resource)，等待 {wait_time:.2f}s 后进行第 {i+1} 次重试...")
                time.sleep(wait_time)
            else:
                raise


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
        resp = _call_with_retry(client.create_node_with_options, create_node_request, runtime)
        # 成功：打印返回结果（包含 NodeId）
        print(json.dumps(resp.body.to_map(), indent=2, ensure_ascii=False))
    except Exception as error:
        # 失败：打印错误信息和阿里云故障排查链接
        print(error.message)
        print(error.data.get("Recommend"))
        raise   # 向上抛出，让 GitHub Actions 看到失败


# ─────────────────────────────────────────────────────
# 函数四：通过节点名精确查找，返回数据开发节点 ID (Data Studio Node ID)
# ─────────────────────────────────────────────────────
def get_node_id(client: DataWorksPublicClient, project_id: int, node_name: str) -> int:
    """
    通过节点名在 DataWorks 工作空间中精确查找节点。

    使用 ListFiles API 的 ExactFileName 参数做精确匹配（非模糊搜索）。
    注意：在 DataWorks 2024-05-18 OpenAPI 中，数据开发（Data Studio）中的节点（Node）
    其唯一标识对应的是 ListFiles 接口返回的 file_id（而返回的 node_id 是发布后调度系统的 ID）。
    UpdateNode 和 GetNode 接口需要的 id 参数均为这个数据开发节点 ID（file_id）。

    Args:
        client:     由 create_client() 返回的 SDK 客户端
        project_id: DataWorks 工作空间 ID
        node_name:  精确节点名称（与 task-config.json 中的 node_name 一致）
    Returns:
        节点 ID（int）；未找到时返回 None
    """
    print(f"🔍 检查项目 {project_id} 中是否存在节点 '{node_name}'...")
    request = dw_models.ListFilesRequest(
        project_id=project_id,
        exact_file_name=node_name,
        page_size=10,
    )
    try:
        resp = _call_with_retry(client.list_files_with_options, request, util_models.RuntimeOptions())
        files = (
            resp.body.data.files
            if (resp.body and resp.body.data and resp.body.data.files)
            else []
        )
    except Exception as error:
        msg = error.message if hasattr(error, "message") else str(error)
        print(f"   ListFiles 查询失败: {msg}")
        return None

    if not files:
        print(f"   未找到节点 '{node_name}'。")
        return None

    f = files[0]
    # Data Studio 节点的唯一标识在 ListFiles 里对应 file_id
    ds_node_id = f.file_id
    print(f"   已找到 — DataStudio 节点 ID={ds_node_id} (调度节点 ID={f.node_id})")
    return ds_node_id


# ─────────────────────────────────────────────────────
# 辅助函数：通过 GetNode 拉取远端节点当前的 Spec
# ─────────────────────────────────────────────────────
def _get_remote_spec(client: DataWorksPublicClient, project_id: int, node_id: int) -> dict:
    """
    调用 GetNode API 获取远端节点的完整 FlowSpec，解析后返回 dict。
    获取失败时返回空 dict（不中断主流程）。
    """
    try:
        request = dw_models.GetNodeRequest(project_id=project_id, id=node_id)
        resp = _call_with_retry(client.get_node_with_options, request, util_models.RuntimeOptions())
        node = resp.body.node
        if node and node.spec:
            return json.loads(node.spec)
    except Exception as error:
        msg = error.message if hasattr(error, "message") else str(error)
        print(f"   ⚠️  获取节点信息失败（跳过差异对比）: {msg}")
    return {}


# ─────────────────────────────────────────────────────
# 辅助函数：递归扁平化 dict，生成 "a.b.c" → value 映射
# ─────────────────────────────────────────────────────
def _flatten(d, prefix=""):
    """
    将嵌套 dict/list 递归展开为扁平的 key→value 字典，方便逐字段对比。

    例如：{"spec": {"nodes": [{"name": "foo"}]}}
    展开为：{"spec.nodes[0].name": "foo"}
    """
    items = {}
    if isinstance(d, dict):
        for k, v in d.items():
            full_key = f"{prefix}.{k}" if prefix else k
            items.update(_flatten(v, full_key))
    elif isinstance(d, list):
        for i, v in enumerate(d):
            items.update(_flatten(v, f"{prefix}[{i}]"))
    else:
        items[prefix] = d
    return items


# ─────────────────────────────────────────────────────
# 辅助函数：比对本地与远端 Spec，打印差异
# ─────────────────────────────────────────────────────
def _print_diff(local_spec: dict, remote_spec: dict) -> int:
    """
    将本地 Spec 与远端 Spec 扁平化后做字段级比对，打印所有有差异的字段。

    Returns:
        diff_count: 差异字段数量（0 表示无差异）
    """
    if not remote_spec:
        print("   (远端节点配置不可用，跳过差异对比)")
        return -1   # -1 表示无法判断

    local_flat  = _flatten(local_spec)
    remote_flat = _flatten(remote_spec)

    all_keys = set(local_flat) | set(remote_flat)
    diffs = []

    for key in sorted(all_keys):
        local_val  = local_flat.get(key, "<missing>")
        remote_val = remote_flat.get(key, "<missing>")

        # content 字段是嵌套的 JSON 字符串，需要进一步解析后比对
        if key.endswith(".content") and isinstance(local_val, str) and isinstance(remote_val, str):
            try:
                local_inner  = json.loads(local_val)
                remote_inner = json.loads(remote_val)
                inner_diffs = _flatten(local_inner)
                inner_remote = _flatten(remote_inner)
                for ik in sorted(set(inner_diffs) | set(inner_remote)):
                    iv  = inner_diffs.get(ik,  "<missing>")
                    irv = inner_remote.get(ik, "<missing>")
                    if iv != irv:
                        diffs.append((f"{key} → {ik}", irv, iv))
                continue   # 跳过原始字符串比对
            except (json.JSONDecodeError, TypeError):
                pass   # 解析失败则降级为字符串比对

        if local_val != remote_val:
            diffs.append((key, remote_val, local_val))

    if not diffs:
        print("   ✅ 未检测到差异。节点已经是最新配置。")
        return 0

    print(f"   📋 发现 {len(diffs)} 个配置差异:\n")
    col_w = max(len(d[0]) for d in diffs) + 2
    print(f"   {'字段':<{col_w}}  {'远端 (当前)':<40}  {'本地 (最新)'}")
    print(f"   {'-'*col_w}  {'-'*40}  {'-'*40}")
    for field, old_val, new_val in diffs:
        old_str = str(old_val)[:38] + ".." if len(str(old_val)) > 40 else str(old_val)
        new_str = str(new_val)[:38] + ".." if len(str(new_val)) > 40 else str(new_val)
        print(f"   {field:<{col_w}}  {old_str:<40}  {new_str}")
    print()
    return len(diffs)


# ─────────────────────────────────────────────────────
# 函数五：增量更新已有节点（含 diff 输出）
# ─────────────────────────────────────────────────────
def update_node(client: DataWorksPublicClient, project_id: int, node_id: int, config: dict) -> None:
    """
    调用 DataWorks UpdateNode API，以增量方式更新节点配置。
    更新前会拉取远端当前 Spec，打印字段级 diff，有差异才执行更新。

    Args:
        client:     由 create_client() 返回的 SDK 客户端
        project_id: DataWorks 工作空间 ID（GetNode 需要）
        node_id:    由 get_node_id() 返回的 NodeId
        config:     由 task-config.json 读取的配置字典
    """
    if config.get("version") == "1.1.0" and "spec" in config:
        local_spec = config
    else:
        local_spec  = json.loads(build_spec(config))
    remote_spec = _get_remote_spec(client, project_id, node_id)

    print("\n   🔎 正在对比本地配置与远端节点配置...")
    diff_count = _print_diff(local_spec, remote_spec)

    if diff_count == 0:
        print("   配置无变化，跳过更新。")
        return

    # 有差异（或无法拉取远端）则执行更新
    update_request = dw_models.UpdateNodeRequest(
        project_id=project_id,
        id=node_id,
        spec=json.dumps(local_spec, ensure_ascii=False)
    )
    runtime = util_models.RuntimeOptions()

    try:
        resp = _call_with_retry(client.update_node_with_options, update_request, runtime)
        if resp.body.success:
            print(f"   ✅ 节点更新成功。 (节点 ID={node_id})")
        else:
            print(f"   ❌ 节点更新返回失败。RequestId={resp.body.request_id}")
            raise RuntimeError("UpdateNode returned success=False")
    except Exception as error:
        msg = error.message if hasattr(error, "message") else str(error)
        print(f"   ❌ 节点更新失败: {msg}")
        if hasattr(error, "data") and error.data:
            print(error.data.get("Recommend", ""))
        raise
