"""
dataworks_client.py — DataWorks API 封装（2024-05-18）

设计原则（参考官方示例代码）：
  - 只负责 CreateNode，数据源由 DataWorks 控制台预先配置
  - 不做 ListNodes / ListDataSources / CreateDataSource 等管理操作
  - 错误信息直接打印 error.message + error.data["Recommend"]
"""

import json
import logging

from alibabacloud_dataworks_public20240518.client import Client
from alibabacloud_dataworks_public20240518 import models as dw_models
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
    # generate_di_job_content — 生成 DI Job Content
    # =========================================================================
    def generate_di_job_content(self, config: dict, table_idx: int) -> dict:
        """
        生成写入 spec.nodes[].script.content 的数据集成配置 JSON。

        参考官方示例中的 di_job_content 结构：
          - extend.mode = "wizard"
          - extend.resourceGroup = 资源组标识
          - steps: reader(oss) + writer(odps)
          - column: [] 自动推断 schema（parquet 推荐）
        """
        table = config["Tables"][table_idx]
        oss_ds = config["OSS"]["DataSourceName"]
        odps_ds = config["MaxCompute"]["DataSourceName"]
        resource_group = config["ResourceGroupIdentifier"]

        base_path = config["OSS"].get("BasePath", "")
        oss_path = f"{base_path}{table['OSS_Object']}"
        file_format = table["FileFormat"]

        # ---- Reader (OSS) ----
        reader_param = {
            "path": oss_path,
            "datasource": oss_ds,
            "column": [],           # 空列表 = 自动推断 schema
            "fileFormat": file_format,
        }
        if file_format != "parquet":
            reader_param["fieldDelimiter"] = table.get("FieldDelimiter", ",")
            reader_param["encoding"] = table.get("Encoding", "UTF-8")

        # ---- Writer (ODPS/MaxCompute) ----
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
            # DataWorks 调度变量：${bizdate}
            writer_param["partition"] = f"{partition}='${{bizdate}}'"

        return {
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

    # =========================================================================
    # build_node_spec — 构建 CreateNode 的 spec JSON
    # =========================================================================
    def build_node_spec(self, config: dict, table_idx: int, node_name: str) -> str:
        """
        构建完整的 CycleWorkflow spec JSON 字符串。

        结构与官方示例完全对齐：
          version / kind / spec.nodes[] / spec.flow
        """
        resource_group = config["ResourceGroupIdentifier"]
        schedule = config.get("Schedule", {})
        cron = schedule.get("CronExpress", "00 00 00 * * ?")
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
                        "runtimeResource": {"resourceGroup": resource_group},
                        "name": node_name,
                        "owner": owner,
                    }
                ],
                "flow": [],
            },
        }
        return json.dumps(spec_dict, ensure_ascii=False)

    # =========================================================================
    # create_node — 创建定时数据集成节点（官方示例对齐版本）
    # =========================================================================
    def create_node(self, node_name: str, config: dict, table_idx: int) -> int:
        """
        调用 CreateNode API 创建节点。
        与官方示例一致：
          - CreateNodeRequest(project_id, spec, scene)
          - client.create_node_with_options(request, runtime)
          - 异常：打印 error.message + error.data["Recommend"]
        Returns: node_id (int)
        """
        logger.info(f"Creating node '{node_name}' ...")
        spec_json = self.build_node_spec(config, table_idx, node_name)

        request = dw_models.CreateNodeRequest(
            project_id=self.project_id,
            spec=spec_json,
            scene="DATAWORKS_PROJECT",
        )
        try:
            response = self.client.create_node_with_options(request, self.runtime)
        except Exception as error:
            # 与官方示例保持一致的错误处理方式
            msg = getattr(error, "message", str(error))
            recommend = ""
            if hasattr(error, "data") and error.data:
                recommend = error.data.get("Recommend", "")
            raise RuntimeError(
                f"CreateNode failed for '{node_name}': {msg}"
                + (f"\n  Recommend: {recommend}" if recommend else "")
            )

        node_id = response.body.data
        if not node_id:
            raise RuntimeError(f"CreateNode returned no node_id for '{node_name}'")

        logger.info(f"✅ Node '{node_name}' created successfully (NodeId: {node_id}).")
        return node_id

    # =========================================================================
    # list_resource_groups — 连接测试用
    # =========================================================================
    def list_resource_groups(self) -> list:
        """列出数据集成资源组，用于验证 AK 连接"""
        logger.info("Listing resource groups ...")
        try:
            from alibabacloud_dataworks_public20240518 import models as dw_models2
            request = dw_models2.ListResourceGroupsRequest(resource_group_type=4)
            response = self.client.list_resource_groups_with_options(request, self.runtime)
            groups = (response.body.data or []) if response.body else []
            for g in groups:
                logger.info(f"  └ {getattr(g, 'identifier', '?')} | {getattr(g, 'status', '?')}")
            return groups
        except Exception as e:
            logger.warning(f"ListResourceGroups: {e}")
            return []
