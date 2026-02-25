"""
dataworks_client.py — DataWorks API 封装类（2024-05-18 版本）

2024 SDK 规范（经实测验证）：
  - 所有 Request 构造参数使用 snake_case
  - ListNodesRequest:       project_id, name, page_size, page_number
  - ListDataSourcesRequest: project_id, name, page_size, page_number
  - CreateDataSourceRequest: project_id, name, type, connection_properties
  - CreateNodeRequest:       project_id, spec, scene   ← 已验证
"""

import json
import logging

from alibabacloud_dataworks_public20240518.client import Client
from alibabacloud_dataworks_public20240518 import models as dw
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_tea_util import models as util_models

logger = logging.getLogger(__name__)


class DataWorksClient:
    """DataWorks API 封装类（2024-05-18）"""

    def __init__(self, access_key_id: str, access_key_secret: str, region: str, project_id: int):
        self.project_id = project_id
        self.region = region

        config = open_api_models.Config(
            access_key_id=access_key_id,
            access_key_secret=access_key_secret,
        )
        config.endpoint = f"dataworks.{region}.aliyuncs.com"
        self.client = Client(config)
        self.runtime = util_models.RuntimeOptions()

        logger.info(f"DataWorksClient initialized (region={region}, project_id={project_id})")

    # =========================================================================
    # _safe_list — 通用分页列表调用，兼容不同响应结构
    # =========================================================================
    def _safe_list(self, api_call, request, *list_paths):
        """
        调用 list API，从响应 body 中按路径取列表。
        list_paths: 按优先级依次尝试的属性路径，如 ('data.nodes',) 或 ('data_sources',)
        """
        try:
            response = api_call(request, self.runtime)
            body = response.body
            for path in list_paths:
                obj = body
                for attr in path.split("."):
                    obj = getattr(obj, attr, None)
                    if obj is None:
                        break
                if obj is not None:
                    return list(obj)
            return []
        except Exception as e:
            raise e  # 交给调用方处理

    # =========================================================================
    # node_exists
    # =========================================================================
    def node_exists(self, node_name: str) -> bool:
        """检查节点是否已存在（ListNodes，按 name 过滤）"""
        logger.info(f"Checking if node '{node_name}' exists ...")
        try:
            # 2024 SDK: name 参数做关键字过滤；无 keyword 参数
            request = dw.ListNodesRequest(
                project_id=self.project_id,
                name=node_name,
                page_size=100,
                page_number=1,
            )
            nodes = self._safe_list(
                self.client.list_nodes_with_options,
                request,
                "data.nodes",   # 优先路径
                "nodes",        # 备选路径
            )
            matches = [n for n in nodes if getattr(n, "name", None) == node_name]
            if matches:
                logger.info(f"Node '{node_name}' already exists.")
                return True
            logger.info(f"Node '{node_name}' not found.")
            return False
        except Exception as e:
            logger.warning(f"ListNodes error: {e}. Treating as not found.")
            return False

    # =========================================================================
    # datasource_exists
    # =========================================================================
    def datasource_exists(self, ds_name: str) -> bool:
        """检查数据源是否已存在（ListDataSources）"""
        logger.info(f"Checking if datasource '{ds_name}' exists ...")
        try:
            request = dw.ListDataSourcesRequest(
                project_id=self.project_id,
                name=ds_name,
                page_size=20,
                page_number=1,
            )
            sources = self._safe_list(
                self.client.list_data_sources_with_options,
                request,
                "data.data_sources",   # 优先
                "data_sources",        # 备选
                "data",                # 再备选
            )
            matches = [s for s in sources if getattr(s, "name", None) == ds_name]
            if matches:
                logger.info(f"Datasource '{ds_name}' exists.")
                return True
            logger.info(f"Datasource '{ds_name}' not found.")
            return False
        except Exception as e:
            logger.warning(f"ListDataSources error: {e}. Treating as not found.")
            return False

    # =========================================================================
    # ensure_oss_datasource
    # =========================================================================
    def ensure_oss_datasource(self, config: dict) -> None:
        """确保 OSS 数据源存在，不存在则创建"""
        ds_name = config["OSS"]["DataSourceName"]
        if self.datasource_exists(ds_name):
            return

        logger.info(f"Creating OSS datasource '{ds_name}' ...")
        conn_props = json.dumps({
            "envType": "Prod",
            "endpoint": config["OSS"]["Endpoint"],
            "bucket": config["OSS"]["Bucket"],
        })
        request = dw.CreateDataSourceRequest(
            project_id=self.project_id,
            name=ds_name,
            type="oss",
            connection_properties_mode="AccessKey",
            connection_properties=conn_props,
        )
        response = self.client.create_data_source_with_options(request, self.runtime)
        body = response.body
        success = getattr(body, "success", None)
        if success is False:
            raise RuntimeError(f"Failed to create OSS datasource '{ds_name}': {body}")
        logger.info(f"OSS datasource '{ds_name}' created.")

    # =========================================================================
    # ensure_odps_datasource
    # =========================================================================
    def ensure_odps_datasource(self, config: dict) -> None:
        """确保 MaxCompute 数据源存在，不存在则创建"""
        ds_name = config["MaxCompute"]["DataSourceName"]
        if self.datasource_exists(ds_name):
            return

        logger.info(f"Creating MaxCompute datasource '{ds_name}' ...")
        conn_props = json.dumps({
            "envType": "Prod",
            "projectName": config["MaxCompute"]["ProjectName"],
            "endpoint": config["MaxCompute"]["Endpoint"],
        })
        request = dw.CreateDataSourceRequest(
            project_id=self.project_id,
            name=ds_name,
            type="odps",
            connection_properties_mode="UserName",
            connection_properties=conn_props,
        )
        response = self.client.create_data_source_with_options(request, self.runtime)
        body = response.body
        success = getattr(body, "success", None)
        if success is False:
            raise RuntimeError(f"Failed to create MaxCompute datasource '{ds_name}': {body}")
        logger.info(f"MaxCompute datasource '{ds_name}' created.")

    # =========================================================================
    # generate_di_job_content
    # =========================================================================
    def generate_di_job_content(self, config: dict, table_idx: int) -> dict:
        """生成 di_job_content（嵌入 spec 内）"""
        table = config["Tables"][table_idx]
        oss_ds = config["OSS"]["DataSourceName"]
        odps_ds = config["MaxCompute"]["DataSourceName"]
        resource_group = config["ResourceGroupIdentifier"]

        base_path = config["OSS"].get("BasePath", "")
        oss_path = f"{base_path}{table['OSS_Object']}"
        file_format = table["FileFormat"]

        reader_param = {
            "path": oss_path,
            "datasource": oss_ds,
            "column": [],
            "fileFormat": file_format,
        }
        if file_format != "parquet":
            reader_param["fieldDelimiter"] = table.get("FieldDelimiter", ",")
            reader_param["encoding"] = table.get("Encoding", "UTF-8")

        writer_param = {
            "truncate": False,
            "datasource": odps_ds,
            "column": [],
            "emptyAsNull": False,
            "table": table["Name"],
            "consistencyCommit": True,
        }
        partition = table.get("Partition")
        if partition:
            writer_param["partition"] = f"{partition}='${{bizdate}}'"

        return {
            "extend": {"mode": "wizard", "resourceGroup": resource_group},
            "type": "job",
            "version": "2.0",
            "steps": [
                {"stepType": "oss",  "parameter": reader_param, "name": "Reader", "category": "reader"},
                {"stepType": "odps", "parameter": writer_param, "name": "Writer", "category": "writer"},
            ],
            "setting": {
                "errorLimit": {"record": "0"},
                "speed": {"throttle": False, "concurrent": 1},
            },
        }

    # =========================================================================
    # build_node_spec
    # =========================================================================
    def build_node_spec(self, config: dict, table_idx: int, node_name: str) -> str:
        """构建 CreateNode 的 spec JSON 字符串"""
        resource_group = config["ResourceGroupIdentifier"]
        schedule = config.get("Schedule", {})
        cron = schedule.get("CronExpress", "00 00 02 * * ?")
        owner = config.get("Owner", "")

        di_job_content = self.generate_di_job_content(config, table_idx)

        spec_dict = {
            "version": "1.1.0",
            "kind": "CycleWorkflow",
            "spec": {
                "nodes": [{
                    "recurrence": "Normal",
                    "timeout": 0,
                    "instanceMode": "T+1",
                    "rerunMode": "Allowed",
                    "rerunTimes": 0,
                    "rerunInterval": 180000,
                    "script": {
                        "path": node_name,
                        "language": "json",
                        "runtime": {"command": "DI"},
                        "content": json.dumps(di_job_content, ensure_ascii=False),
                    },
                    "trigger": {
                        "type": "Scheduler",
                        "cron": cron,
                        "startTime": "1970-01-01 00:00:00",
                        "endTime": "9999-01-01 00:00:00",
                    },
                    "runtimeResource": {"resourceGroup": resource_group},
                    "name": node_name,
                    "owner": owner,
                }],
                "flow": [],
            },
        }
        return json.dumps(spec_dict, ensure_ascii=False)

    # =========================================================================
    # create_node  ← 已验证：snake_case 正确
    # =========================================================================
    def create_node(self, node_name: str, config: dict, table_idx: int) -> int:
        """调用 CreateNode API，一步创建定时节点"""
        logger.info(f"Creating node '{node_name}' ...")
        spec_json = self.build_node_spec(config, table_idx, node_name)

        request = dw.CreateNodeRequest(
            project_id=self.project_id,
            spec=spec_json,
            scene="DATAWORKS_PROJECT",
        )
        try:
            response = self.client.create_node_with_options(request, self.runtime)
        except Exception as e:
            raise RuntimeError(f"CreateNode failed for '{node_name}': {e}")

        node_id = response.body.data
        if not node_id:
            raise RuntimeError(f"CreateNode returned no node_id for '{node_name}'")

        logger.info(f"Node '{node_name}' created (NodeId: {node_id}).")
        return node_id

    # =========================================================================
    # list_resource_groups
    # =========================================================================
    def list_resource_groups(self) -> list:
        """列出数据集成资源组"""
        logger.info("Listing Data Integration resource groups ...")
        # resource_group_type=4 → 数据集成资源组
        for kwargs in [{"resource_group_type": 4}, {}]:
            try:
                request = dw.ListResourceGroupsRequest(**kwargs)
                response = self.client.list_resource_groups_with_options(request, self.runtime)
                groups = (response.body.data or []) if response.body else []
                for g in groups:
                    logger.info(f"  Identifier: {getattr(g, 'identifier', '?')} | Status: {getattr(g, 'status', '?')}")
                return groups
            except Exception as e:
                logger.warning(f"ListResourceGroups {kwargs}: {e}")
        return []
