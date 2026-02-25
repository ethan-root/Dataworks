"""
dataworks_client.py — DataWorks API 封装类
使用阿里云 Python SDK 实现所有 DataWorks OpenAPI 调用
"""

import json
import logging
from typing import Optional

from alibabacloud_dataworks_public20200518.client import Client
from alibabacloud_dataworks_public20200518 import models as dataworks_models
from alibabacloud_tea_openapi.models import Config

logger = logging.getLogger(__name__)


class DataWorksClient:
    """DataWorks API 封装类"""

    def __init__(self, access_key_id: str, access_key_secret: str, region: str, project_id: int):
        self.project_id = project_id
        self.region = region

        config = Config(
            access_key_id=access_key_id,
            access_key_secret=access_key_secret,
            region_id=region,
            endpoint=f"dataworks.{region}.aliyuncs.com",
        )
        self.client = Client(config)
        logger.info(f"DataWorksClient initialized (region={region}, project_id={project_id})")

    # =========================================================================
    # job_exists — 检查同名 Job 是否已存在
    # =========================================================================
    def job_exists(self, job_name: str) -> bool:
        """检查 DataWorks 中是否已存在同名 Job"""
        logger.info(f"Checking if job '{job_name}' exists ...")
        try:
            request = dataworks_models.ListFilesRequest(
                project_id=self.project_id,
                keyword=job_name,
                page_size=100,
                page_number=1,
            )
            response = self.client.list_files(request)
            files = response.body.data.files or []
            matches = [f for f in files if f.file_name == job_name]
            if matches:
                logger.info(f"Job '{job_name}' already exists ({len(matches)} match(es)).")
                return True
            logger.info(f"Job '{job_name}' does not exist.")
            return False
        except Exception as e:
            logger.warning(f"ListFiles API error: {e}, treating as not found.")
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
            response = self.client.list_data_sources(request)
            sources = response.body.data.data_sources or []
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
        response = self.client.create_data_source(request)
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
        response = self.client.create_data_source(request)
        if not response.body.success:
            raise RuntimeError(f"Failed to create MaxCompute datasource '{ds_name}': {response.body}")
        logger.info(f"MaxCompute datasource '{ds_name}' created successfully.")

    # =========================================================================
    # generate_task_content — 生成 TaskContent JSON
    # =========================================================================
    def generate_task_content(self, config: dict, table_idx: int) -> dict:
        """根据 config 和 table 索引生成 TaskContent，支持 parquet/csv + 分区"""
        table = config["Tables"][table_idx]
        oss_ds = config["OSS"]["DataSourceName"]
        odps_ds = config["MaxCompute"]["DataSourceName"]

        base_path = config["OSS"].get("BasePath", "")
        full_oss_object = f"{base_path}{table['OSS_Object']}"
        file_format = table["FileFormat"]

        # ---- Reader parameter ----
        if file_format == "parquet":
            # Parquet: 列名直接映射
            reader_columns = [{"type": col["type"], "value": col["name"]} for col in table["Columns"]]
            reader_param = {
                "datasource": oss_ds,
                "object": [full_oss_object],
                "column": reader_columns,
                "fileFormat": file_format,
            }
        else:
            # CSV/Text: 按索引映射
            reader_columns = [{"type": col["type"], "value": str(i)} for i, col in enumerate(table["Columns"])]
            reader_param = {
                "datasource": oss_ds,
                "object": [full_oss_object],
                "column": reader_columns,
                "fieldDelimiter": table.get("FieldDelimiter", ","),
                "encoding": table.get("Encoding", "UTF-8"),
                "fileFormat": file_format,
            }

        # ---- Writer parameter ----
        writer_columns = [col["name"] for col in table["Columns"]]
        writer_param = {
            "datasource": odps_ds,
            "table": table["Name"],
            "column": writer_columns,
            "truncate": True,
        }

        # 分区支持
        partition = table.get("Partition")
        if partition:
            writer_param["partition"] = f"{partition}=${{bizdate}}"

        # ---- 完整 TaskContent ----
        task_content = {
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
                "speed": {"channel": 1, "throttle": False},
                "errorLimit": {"record": 0},
            },
            "order": {
                "hops": [{"from": "Reader", "to": "Writer"}],
            },
        }
        return task_content

    # =========================================================================
    # create_sync_task — 创建同步任务
    # =========================================================================
    def create_sync_task(self, job_name: str, task_content: dict, resource_group: str) -> int:
        """创建 DI 同步任务，返回 FileId"""
        logger.info(f"Creating DI sync task '{job_name}' ...")

        task_param = json.dumps({
            "FileFolderPath": "/",
            "ResourceGroup": resource_group,
        })

        request = dataworks_models.CreateDISyncTaskRequest(
            project_id=self.project_id,
            task_type="DI_OFFLINE",
            task_name=job_name,
            task_param=task_param,
            task_content=json.dumps(task_content),
        )
        response = self.client.create_disync_task(request)

        data = response.body.data
        if data.status != "success":
            message = getattr(data, "message", "Unknown error")
            raise RuntimeError(f"Failed to create task '{job_name}': {message}")

        file_id = data.file_id
        logger.info(f"Task '{job_name}' created with FileId: {file_id}")
        return file_id

    # =========================================================================
    # submit_file — 提交任务
    # =========================================================================
    def submit_file(self, file_id: int) -> None:
        """提交文件到调度系统"""
        logger.info(f"Submitting file {file_id} ...")
        request = dataworks_models.SubmitFileRequest(
            project_id=self.project_id,
            file_id=file_id,
        )
        response = self.client.submit_file(request)
        if not response.body.success:
            raise RuntimeError(f"Failed to submit file {file_id}: {response.body}")
        logger.info(f"File {file_id} submitted successfully.")

    # =========================================================================
    # deploy_file — 发布到生产
    # =========================================================================
    def deploy_file(self, file_id: int) -> None:
        """发布文件到生产环境"""
        logger.info(f"Deploying file {file_id} to production ...")
        request = dataworks_models.DeployFileRequest(
            project_id=self.project_id,
            file_id=file_id,
        )
        response = self.client.deploy_file(request)
        if not response.body.success:
            raise RuntimeError(f"Failed to deploy file {file_id}: {response.body}")
        logger.info(f"File {file_id} deployed to production successfully.")

    # =========================================================================
    # list_resource_groups — 列出资源组
    # =========================================================================
    def list_resource_groups(self) -> list:
        """列出数据集成资源组"""
        logger.info("Listing Data Integration resource groups ...")
        request = dataworks_models.ListResourceGroupsRequest(
            resource_group_type=4,  # 4 = Data Integration
        )
        response = self.client.list_resource_groups(request)
        groups = response.body.data or []
        for g in groups:
            logger.info(f"  ResourceGroup: {g.identifier} | Status: {g.status}")
        return groups
