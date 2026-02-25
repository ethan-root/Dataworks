"""
dataworks_client.py — DataWorks API 封装类（2024-05-18 版本）

使用 CreateNode API 一次性创建并调度数据集成节点，无需单独的 Submit + Deploy 步骤。

API 文档: https://api.aliyun.com/product/dataworks-public (版本: 2024-05-18)
"""

import json
import logging
from typing import Optional

from alibabacloud_dataworks_public20240518.client import Client
from alibabacloud_dataworks_public20240518 import models as dataworks_models
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
    # node_exists — 检查同名节点是否已存在
    # =========================================================================
    def node_exists(self, node_name: str) -> bool:
        """检查 DataWorks 中是否已存在同名节点"""
        logger.info(f"Checking if node '{node_name}' exists ...")
        try:
            request = dataworks_models.ListNodesRequest(
                project_id=self.project_id,
                keyword=node_name,
                page_size=100,
                page_number=1,
            )
            response = self.client.list_nodes_with_options(request, self.runtime)
            nodes = (response.body.data.nodes or []) if response.body.data else []
            matches = [n for n in nodes if n.name == node_name]
            if matches:
                logger.info(f"Node '{node_name}' already exists ({len(matches)} match(es)).")
                return True
            logger.info(f"Node '{node_name}' does not exist.")
            return False
        except Exception as e:
            logger.warning(f"ListNodes API error: {e}, treating as not found.")
            return False

    # =========================================================================
    # datasource_exists — 检查数据源是否已存在
    # =========================================================================
    def datasource_exists(self, ds_name: str) -> bool:
        """检查数据源是否已存在"""
        logger.info(f"Checking if datasource '{ds_name}' exists ...")
        try:
            request = dataworks_models.ListDataSourcesRequest(
                project_id=self.project_id,
                name=ds_name,
                page_size=20,
                page_number=1,
            )
            response = self.client.list_data_sources_with_options(request, self.runtime)
            sources = (response.body.data.data_sources or []) if response.body.data else []
            matches = [s for s in sources if s.name == ds_name]
            if matches:
                logger.info(f"Datasource '{ds_name}' exists.")
                return True
            logger.info(f"Datasource '{ds_name}' does not exist.")
            return False
        except Exception as e:
            logger.warning(f"ListDataSources API error: {e}, treating as not found.")
            return False

    # =========================================================================
    # ensure_oss_datasource — 确保 OSS 数据源存在
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
        request = dataworks_models.CreateDataSourceRequest(
            project_id=self.project_id,
            name=ds_name,
            data_source_type="oss",
            connection_properties=conn_props,
        )
        response = self.client.create_data_source_with_options(request, self.runtime)
        if not response.body.success:
            raise RuntimeError(f"Failed to create OSS datasource '{ds_name}': {response.body}")
        logger.info(f"OSS datasource '{ds_name}' created successfully.")

    # =========================================================================
    # ensure_odps_datasource — 确保 MaxCompute 数据源存在
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
        request = dataworks_models.CreateDataSourceRequest(
            project_id=self.project_id,
            name=ds_name,
            data_source_type="odps",
            connection_properties=conn_props,
        )
        response = self.client.create_data_source_with_options(request, self.runtime)
        if not response.body.success:
            raise RuntimeError(f"Failed to create MaxCompute datasource '{ds_name}': {response.body}")
        logger.info(f"MaxCompute datasource '{ds_name}' created successfully.")

    # =========================================================================
    # generate_di_job_content — 生成 DI Job Content（嵌入 spec 中的数据集成配置）
    # =========================================================================
    def generate_di_job_content(self, config: dict, table_idx: int) -> dict:
        """
        生成 di_job_content，支持 parquet/csv 格式 + 分区配置。

        - Parquet: column=[] 让 DataWorks 自动推断 schema
        - CSV: column=[] 同样支持（或可显式指定）
        - Partition: 从 config 读取，若无则不写 partition 字段
        """
        table = config["Tables"][table_idx]
        oss_ds = config["OSS"]["DataSourceName"]
        odps_ds = config["MaxCompute"]["DataSourceName"]
        resource_group = config["ResourceGroupIdentifier"]

        base_path = config["OSS"].get("BasePath", "")
        oss_path = f"{base_path}{table['OSS_Object']}"
        file_format = table["FileFormat"]

        # ---- Reader ----
        reader_param = {
            "path": oss_path,
            "datasource": oss_ds,
            "column": [],          # 空列表 = 自动推断 schema（parquet 推荐方式）
            "fileFormat": file_format,
        }
        # CSV 额外参数
        if file_format != "parquet":
            reader_param["fieldDelimiter"] = table.get("FieldDelimiter", ",")
            reader_param["encoding"] = table.get("Encoding", "UTF-8")

        # ---- Writer ----
        writer_param = {
            "truncate": False,
            "datasource": odps_ds,
            "column": [],          # 空列表 = 与来源列自动对应
            "emptyAsNull": False,
            "table": table["Name"],
            "consistencyCommit": True,
        }
        # 分区配置
        partition = table.get("Partition")
        if partition:
            # 格式: "pt='${bizdate}'" — DataWorks 调度变量
            writer_param["partition"] = f"{partition}='${{bizdate}}'"

        di_job_content = {
            "extend": {
                "mode": "wizard",
                "resourceGroup": resource_group,
            },
            "type": "job",
            "version": "2.0",
            "steps": [
                {
                    "stepType": "oss",
                    "parameter": reader_param,
                    "name": "Reader",
                    "category": "reader",
                },
                {
                    "stepType": "odps",
                    "parameter": writer_param,
                    "name": "Writer",
                    "category": "writer",
                },
            ],
            "setting": {
                "errorLimit": {"record": "0"},
                "speed": {"throttle": False, "concurrent": 1},
            },
        }
        return di_job_content

    # =========================================================================
    # build_node_spec — 构建 CreateNode 的 spec JSON
    # =========================================================================
    def build_node_spec(self, config: dict, table_idx: int, node_name: str) -> str:
        """
        构建完整的 CycleWorkflow spec JSON 字符串，传给 CreateNode API。

        spec 包含：节点名称、调度 cron、资源组、DI 内容
        """
        table = config["Tables"][table_idx]
        resource_group = config["ResourceGroupIdentifier"]
        schedule = config.get("Schedule", {})
        cron = schedule.get("CronExpress", "00 00 02 * * ?")
        owner = config.get("Owner", "")

        di_job_content = self.generate_di_job_content(config, table_idx)

        spec_dict = {
            "version": "1.1.0",
            "kind": "CycleWorkflow",
            "spec": {
                "nodes": [
                    {
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
                        "runtimeResource": {
                            "resourceGroup": resource_group,
                        },
                        "name": node_name,
                        "owner": owner,
                    }
                ],
                "flow": [],
            },
        }
        return json.dumps(spec_dict, ensure_ascii=False)

    # =========================================================================
    # create_node — 创建数据集成节点（一步完成，无需 Submit + Deploy）
    # =========================================================================
    def create_node(self, node_name: str, config: dict, table_idx: int) -> int:
        """
        调用 CreateNode API 创建定时数据集成节点。
        新 API 创建即发布，无需单独 Submit + Deploy。

        Returns:
            node_id (int)
        """
        logger.info(f"Creating node '{node_name}' ...")

        spec_json = self.build_node_spec(config, table_idx, node_name)
        logger.debug(f"Node spec:\n{spec_json}")

        request = dataworks_models.CreateNodeRequest(
            project_id=self.project_id,
            spec=spec_json,
            scene="DATAWORKS_PROJECT",
        )

        try:
            response = self.client.create_node_with_options(request, self.runtime)
        except Exception as e:
            raise RuntimeError(f"CreateNode API error for '{node_name}': {e}")

        node_id = response.body.data
        if not node_id:
            raise RuntimeError(f"CreateNode returned no node_id for '{node_name}'")

        logger.info(f"Node '{node_name}' created successfully (NodeId: {node_id}).")
        return node_id

    # =========================================================================
    # list_resource_groups — 列出数据集成资源组
    # =========================================================================
    def list_resource_groups(self) -> list:
        """列出数据集成资源组"""
        logger.info("Listing Data Integration resource groups ...")
        # 2024 SDK 用 ResourceGroupType（PascalCase），先尝试带过滤，失败则不带参数重试
        for kwargs in [{"ResourceGroupType": 4}, {}]:
            try:
                request = dataworks_models.ListResourceGroupsRequest(**kwargs)
                response = self.client.list_resource_groups_with_options(request, self.runtime)
                groups = (response.body.data or []) if response.body else []
                for g in groups:
                    logger.info(f"  Identifier: {g.identifier} | Status: {g.status}")
                return groups
            except Exception as e:
                logger.warning(f"ListResourceGroups attempt failed ({kwargs}): {e}")
        return []
