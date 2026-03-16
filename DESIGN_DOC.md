# DataWorks PoC 项目设计文档（现状深度梳理）

## 1. 文档目标

本文针对当前 PoC 项目代码进行“以代码为准”的全量梳理，覆盖以下范围：

- 所有流水线（GitHub Actions + 本地脚本执行链）
- 所有逻辑关系（模块依赖、数据流、配置流、API 调用流）
- 每个代码文件和配置文件职责
- 现状一致性评估（文档与代码、命名与实现、可维护性）

说明：本文以仓库当前文件内容为准，而非仅以 README 叙述为准。

---

## 2. 项目总体架构

项目采用“底板模板 + 业务轻配置 + Python 脚本编排 + DataWorks OpenAPI 调用”的模式。

### 2.1 分层架构

1. 模板层（default-setting）
- 存放系统级默认配置（DataWorks 节点模板、数据源模板）
- 由脚本读取后按业务配置覆盖

2. 业务层（feature）
- 每个 feature 下按环境目录维护轻量参数（当前主链路是 setting.json）
- create-table.sql 同目录存放业务表结构，供自动映射解析

3. 执行层（scripts）
- config_merger.py 负责模板读取与动态覆盖
- dataworks_client.py 封装核心 DataWorks SDK 调用
- 其他脚本作为不同场景入口（创建、更新、发布、检查、清理、校验）

4. 编排层（.github/workflows）
- 通过 workflow_dispatch 输入动作类型和环境
- 调用 scripts 对应入口完成任务

### 2.2 核心思想

- Schema-Driven：从 create-table.sql 自动抽取字段，生成 Reader/Writer 列映射
- Upsert Node：节点存在则更新，不存在则创建
- 模板与业务参数解耦：default-setting 放系统参数，feature/env 放业务参数

---

## 3. 端到端流水线梳理

## 3.1 GitHub Actions 流水线一览

1. .github/workflows/test_dataworks.yml
- 名称：[TEST] DataWorks Integration
- 手动触发，支持 action 选择：
  - create_table
  - create_node
  - publish_node
  - clean_mc_tables
  - create_oss_ds
  - create_mc_ds
  - validate_row_count
- 支持 project_dir + env_name 组合定位 feature 路径
- 是最完整、最通用的测试编排入口

2. .github/workflows/datawork_node_create-dev.yml
3. .github/workflows/datawork_node_create-qa.yml
4. .github/workflows/datawork_node_create-preprod.yml
5. .github/workflows/datawork_node_create-prod.yml
- 四套环境拆分工作流
- action 支持与 test 类似（不含 validate_row_count）
- 通过固定后缀 /dev /qa /pre-prod /prod 传给脚本

6. .github/workflows/dataworks_sync.yml
- 名称：DataWorks Sync Management
- 支持 projects、env_name 输入，但当前 run 命令固定为：
  - python scripts/deploy.py --project-dir feature/test-feature/${env_name}
- 现状是“单路径硬编码部署”而非多项目循环

## 3.2 单次主流程（create_node）

触发路径示例：test_dataworks.yml action=create_node

1. Checkout
2. 安装 Python 3.11
3. pip install -r scripts/requirements.txt
4. 执行：python scripts/deploy.py --project-dir feature/<feature>/<env>
5. deploy.py 调用 create_integration_node.process_project
6. process_project 调 config_merger.load_merged_node_config
7. merge 后调用 dataworks_client.get_node_id 判定是否存在
8. 已存在：update_node；不存在：create_node
9. DataWorks API 完成节点落库

## 3.3 发布流程（publish_node）

publish_node.py 执行阶段：

1. 读取 merged config 获取 node_name
2. get_node_id 获取 Data Studio file_id
3. CreatePipelineRun(type=Online)
4. 顺序执行阶段：
  - BUILD_PACKAGE
  - PROD_CHECK
  - PROD
5. 每阶段带简单重试与等待

## 3.4 建表、数据源、校验、清理流程

1. create_table.py
- 读取 create-table.sql
- 用 pyodps 执行 DDL（幂等依赖 SQL 自身 IF NOT EXISTS）

2. create_oss_ds.py / create_mc_ds.py
- 读取 merged datasource 配置
- 调用 CreateDataSource API

3. check_oss_ds.py / check_mc_ds.py / check_integration_node.py
- 分别检查 OSS/MC 数据源和节点是否存在

4. clean_mc_tables.py
- 枚举表并按创建时间阈值清理（支持 dry-run）

5. validate_row_count.py
- 比对外部表与内部表行数
- 打印少量样本行

---

## 4. 逻辑关系总图

## 4.1 模块调用关系（脚本级）

- deploy.py -> create_integration_node.py -> config_merger.py + dataworks_client.py
- update_integration_node.py -> config_merger.py + dataworks_client.py
- publish_node.py -> config_merger.py + dataworks_client.py + Pipeline API
- create_oss_ds.py -> config_merger.py + dataworks_client.py
- create_mc_ds.py -> config_merger.py + dataworks_client.py
- check_oss_ds.py / check_mc_ds.py -> config_merger.py + dataworks_client.py
- check_integration_node.py -> config_merger.py + dataworks_client.py
- create_table.py / clean_mc_tables.py / validate_row_count.py -> pyodps 直连 MaxCompute

## 4.2 配置流关系

输入源：

1. default-setting/integration-config.json
2. default-setting/oss-datasource.json
3. default-setting/maxcompute-datasource.json
4. feature/<name>/<env>/setting.json
5. feature/<name>/<env>/create-table.sql

merge 规则（config_merger.py）：

1. 读取对应底板
2. 读取 setting.json
3. 覆盖 task/datasource 的关键字段
4. 解析 SQL 字段，注入 reader.column 和 writer.column

输出：

- create/update node 的最终 config dict
- create datasource 的最终 config dict

## 4.3 API 调用流（DataWorks）

1. 客户端初始化：create_client
- 环境变量 AK/SK/Region -> DataWorksPublicClient

2. 节点相关
- get_node_id：ListFiles(exact_file_name)
- create_node：CreateNode(spec)
- update_node：GetNode + diff + UpdateNode(spec)

3. 发布相关
- create_pipeline_run_with_options
- exec_pipeline_run_stage_with_options

4. 数据源相关
- create_data_source_with_options
- list_data_sources_with_options

---

## 5. 配置模型与字段映射

## 5.1 setting.json 当前生效结构（以 test-feature/dev 为例）

- datasource.oss.name/bucket/endpoint
- datasource.mc.name/project/endpoint
- task.node_name/cron/reader_datasource/reader_path/writer_datasource/writer_table/writer_partition

## 5.2 SQL 自动映射规则（_parse_columns_from_sql）

1. 优先匹配 CREATE TABLE 括号区块到 COMMENT/PARTITIONED 之间
2. 行级解析字段名：
- 优先反引号包裹字段
- 否则取首个 token
3. 忽略注释行和部分关键字
4. 生成：
- reader.column: [{name,type=BINARY,index,originalType=UTF8,repetition=OPTIONAL}, ...]
- writer.column: [col1, col2, ...]

## 5.3 integration-config 底板与覆盖边界

会被 setting.json 覆盖的字段：

- node_name
- cron
- reader.datasource
- reader.path
- writer.datasource
- writer.table
- writer.partition

保持底板默认的字段（示例）：

- owner/resource_group/resourceGroupId
- script/runtime 参数
- 调度控制参数（instanceMode/rerun 等）
- metadata 等

---

## 6. 各文件职责清单（逐文件）

## 6.1 根目录

1. over.README.md
- 架构说明与使用指南
- 叙述方向正确，但部分术语仍有历史命名（global.json/task-config）

2. sdk_methods.txt
- 空文件，当前无功能

3. DESIGN_DOC.md
- 本文档（本次梳理产物）

## 6.2 default-setting

1. integration-config.json
- DataWorks 节点底板（调度参数 + Reader/Writer 基础结构）

2. oss-datasource.json
- OSS 数据源底板

3. maxcompute-datasource.json
- MaxCompute 数据源底板

4. python_cp_node.json
- 空 JSON，扩展占位

5. downstream.json
- 空 JSON，扩展占位

6. data_clean.json
- 空 JSON，扩展占位

## 6.3 feature

1. feature/test-feature/dev/setting.json
- 当前主流程生效业务配置

2. feature/test-feature/dev/create-table.sql
- 当前主流程生效 SQL，驱动字段映射

3. feature/guest-feature/dev/config.json
4. feature/user-feature/dev/config.json
- 旧版配置结构样例（ProjectName/OSS/MaxCompute/Tables）
- 当前 scripts 主链路不直接消费这两份 config.json

## 6.4 scripts

1. dataworks_client.py
- DataWorks API 客户端与核心调用封装
- 包含 spec 组装、节点创建/更新、节点查询、diff 打印

2. config_merger.py
- 模板加载、setting 覆盖、SQL 字段解析注入
- 是配置体系核心枢纽

3. deploy.py
- 节点部署入口（单项目或扫描 feature 全量）

4. create_integration_node.py
- process_project（Upsert）与 create_project（仅创建）

5. update_integration_node.py
- 仅更新入口

6. publish_node.py
- Pipeline 三阶段发布入口

7. create_table.py
- MaxCompute 执行建表 SQL

8. create_oss_ds.py
- 创建 OSS 数据源

9. create_mc_ds.py
- 创建 MaxCompute 数据源

10. check_integration_node.py
- 检查节点是否存在

11. check_oss_ds.py
- 检查 OSS 数据源是否存在

12. check_mc_ds.py
- 检查 MaxCompute 数据源是否存在

13. clean_mc_tables.py
- 过期表清理（dry-run/执行）

14. validate_row_count.py
- 外部表与内部表行数比对

15. requirements.txt
- 依赖声明（DataWorks SDK + Tea + pyodps）

## 6.5 .github/workflows

1. test_dataworks.yml
- 全动作测试入口

2. datawork_node_create-dev.yml
3. datawork_node_create-qa.yml
4. datawork_node_create-preprod.yml
5. datawork_node_create-prod.yml
- 环境拆分入口

6. dataworks_sync.yml
- 同步管理入口（当前命令固定 test-feature）

---

## 7. 环境变量与外部依赖

## 7.1 必需环境变量

DataWorks 相关：

- ALIBABA_CLOUD_ACCESS_KEY_ID
- ALIBABA_CLOUD_ACCESS_KEY_SECRET
- ALIYUN_REGION
- DATAWORKS_PROJECT_ID

MaxCompute 相关（建表/清理/校验用）：

- MAXCOMPUTE_PROJECT
- MAXCOMPUTE_ENDPOINT

## 7.2 第三方依赖

- alibabacloud-dataworks-public20240518
- alibabacloud-tea-openapi
- alibabacloud-tea-util
- alibabacloud-credentials
- pyodps

---

## 8. 现状一致性评估（以代码真实行为为准）

以下是当前代码中的关键一致性点与偏差点：

1. 新旧命名并存
- 注释/文档中仍出现 global.json、task-config.json、config.json
- 实际主链路使用 setting.json + integration-config.json

2. 双配置体系并存
- feature 下既有 setting.json（新）也有 config.json（旧样例）
- 当前 deploy 扫描逻辑仅识别 setting.json

3. dataworks_sync.yml 输入未完全利用
- 定义了 projects 输入，但 run 命令固定 test-feature 路径

4. create_oss_ds.py 与 create_mc_ds.py 中存在变量引用不一致
- 准备 connection_properties 时引用 ds_config，但脚本实际变量为 config
- connection_properties 最终也未进入 CreateDataSourceRequest 的关键字段使用链路（当前主要依赖其他参数）

5. 更新逻辑较完整
- update_node 先 GetNode 并打印字段差异，再决定是否调用 UpdateNode
- 对 PoC 迭代调试非常友好

---

## 9. 设计优点与技术债

## 9.1 优点

1. 分层清晰
- 模板与业务配置分离，降低重复配置成本

2. 自动字段映射
- SQL 作为单一事实源，减少手工维护列映射错误

3. Upsert 与 Diff 能力
- 减少重复创建风险，更新过程可观测

4. 脚本粒度清晰
- 创建、更新、发布、检查、清理、校验职责明确

## 9.2 技术债

1. 命名与注释历史包袱较重
2. 工作流存在重复（dev/qa/preprod/prod）
3. 旧配置样例未明确标记“弃用/兼容”状态
4. 部分脚本有未生效变量或变量名残留

---

## 10. 推荐重构路线（不影响当前可跑通链路）

1. 统一术语与注释
- 全仓将 global.json/task-config/config.json 旧术语替换为 setting.json/integration-config

2. 固化唯一配置模型
- 明确 feature 目录只保留 setting.json + create-table.sql
- 旧 config.json 放入 examples/legacy 或删除

3. 合并四套环境工作流
- 使用单一 workflow + env_name 输入，减少重复维护

4. 修正数据源脚本变量一致性
- 统一 config 命名，移除未使用 connection_properties 或改为真实使用

5. dataworks_sync.yml 实现 projects 循环
- 按输入 split 后逐个调用 deploy.py

---

## 11. 典型运行时序（以 create_node 为例）

1. GitHub Actions 接收 action=create_node
2. 执行 deploy.py
3. deploy.py 调 process_project
4. process_project 调 load_merged_node_config
5. load_merged_node_config:
- 读 default-setting/integration-config.json
- 读 feature/<x>/<env>/setting.json
- 覆盖关键字段
- 解析 create-table.sql 注入 column
6. process_project 调 get_node_id
7. 若存在则 update_node（含 diff），否则 create_node
8. DataWorks 工作空间生成或更新节点

---

## 12. 结论

当前项目已具备完整 PoC 闭环能力：

- 配置合并 -> 节点创建/更新 -> 节点发布 -> 数据质量校验/运维清理

并且主链路（test-feature/dev）设计方向正确，可持续演进为生产化版本。后续主要工作不在“重做架构”，而在“清理历史兼容痕迹 + 统一配置标准 + 收敛工作流重复”。
