# DataWorks DataOps — OSS to MaxCompute

基于 **GitHub Actions + Python** 自动化编排 Aliyun DataWorks 数据集成任务，实现配置驱动、多环境串行部署与代码化管理的 DataOps CI/CD 体系。

---

## 📐 整体架构

```text
Dataworks/
├── .github/workflows/               # CI/CD 流水线定义
│   ├── datawork_node_create_update.yml  # 主流水线（Push/手动触发，全量节点组装与创建/更新）
│   ├── _deploy_env.yml              # 可复用工作流（单环境部署，调用 ci_runner.py）
│   ├── database_update.yml          # DDL 执行流水线
│   └── pr_validation.yml            # PR 安全扫描（SonarCloud/GitGuardian）
│
├── configuration/                   # 全局底板配置（系统级参数，无需业务人员修改）
│   ├── integration-config.json      # 数据集成节点系统级底板
│   ├── oss-datasource.json          # OSS 数据源系统级底板
│   ├── maxcompute-datasource.json   # MaxCompute 数据源系统级底板
│   ├── get_earliest_file_name.py    # 上游节点底层参考源码
│   └── move_parquet_to_completed.py # 下游归档底层参考源码
│
├── features/                        # 业务工作区（开发人员每次只改这里）
│   ├── shared/
│   │   └── ddl-metadata.sql         # 全局元数据表、Changelog 跟踪表定义
│   └── {feature-name}/              # 以集成节点业务名为目录名
│       ├── setting-dev.json         # DEV 环境业务参数
│       ├── setting-qa.json          # QA 环境业务参数
│       ├── setting-preprod.json     # PRE-PROD 环境业务参数
│       ├── setting-prod.json        # PROD 环境业务参数
│       └── ddl/
│           ├── YYYYMMDDHHMI_create.sql  # MaxCompute 建表 SQL
│           └── YYYYMMDDHHMI_alter.sql   # MaxCompute 改表 SQL（支持增量演进）
│
└── scripts/                         # Python 部署引擎
    ├── ci_runner.py                 # ★ 编排层：发起上下游全链路部署流程的总指挥
    ├── config_merger.py             # 配置引擎（底板 + 业务参数 + DDL 聚合解析 → 完整配置）
    ├── dataworks_client.py          # DataWorks SDK 封装（内置 Throttling 指数退避重试保护）
    ├── check/create_*_ds.py         # 数据源探查与创建工具
    ├── create_table.py              # MaxCompute 增量 DDL 迁移执行器（基于 Changelog）
    ├── create_upstream_node.py      # 上游 Python 节点生成器（拉取处理）
    ├── create_integration_node.py   # 核心 DI 数据集成节点生成/对比器
    ├── create_downstream_node.py    # 下游归档移动节点生成器
    ├── create_python_cp_node.py     # 分区自动清理节点生成器
    └── publish_node.py              # 三阶段打样与真实环境发布工具
```

---

## 🚀 CI/CD 流水线说明

### 1. 节点全链路部署流水线（`datawork_node_create_update.yml`）

**触发条件**：
- 向 `main` 分支 push，且 `features/**` 目录下有文件变更时自动触发。
- 支持 `workflow_dispatch` 手动触发网页 UI 按钮（填写 `all` 部署全量或填写 `模块名` 如 `test-feature` 部署单节点）。

**执行流程（高智能 Upsert）**：
采用全链路编排，对于给定的 feature，依次执行探测对比，如果节点已存在且有变化则走修改 `UPDATE`，不存在则全新 `CREATE`，完全跳过未改动文件无损发布：
1. ☁️ **确保 OSS 和 MC 数据源可用**
2. 📄 **增量 DDL 迁移**：按时间顺序增量执行所有未执行的 `ddl/*.sql` 文件，已执行的 SQL 会记录到变更表避免重复跑。
3. 🐍 **上游赋值节点更新**：拼装用于探测最早文件的 Python 工作包推送云端。
4. 🔗 **中心数据集成节点更新**：合并配置形成主血缘 DI 任务。
5. 📤 **下游及清理节点更新**：挂接文件移动归档与旧历史分区清理操作。
6. 🚀 **触发流水线自动化发布工作**。

**多环境串行流转**：`dev → qa → preprod → prod`，每个环境使用对应的 `setting-<env>.json` 注入环境独立配方参数，`prod` 需 GitHub Environment 人工审批介入。

---

## ✍️ 业务开发指南

### 新建一个 Feature（数据集成任务流）

**第一步**：在 `features/` 下创建目录（目录名 = 你的业务名）

```bash
features/
└── my-new-feature/
    ├── setting-dev.json      # 最少只需填 10 行业务参数
    ├── setting-qa.json
    ├── setting-preprod.json
    ├── setting-prod.json
    └── ddl/
        └── 202602141515_create.sql
```

**第二步**：填写 `setting-dev.json`（各环境仅端点/库名参数值不同，脚本会自动拼装进上下游任务）

```json
{
    "datasource": {
        "oss": {
            "name": "oss_your_ds_name",
            "bucket": "your-bucket",
            "endpoint": "https://oss-cn-shanghai-internal.aliyuncs.com"
        },
        "mc": {
            "name": "mc_your_ds_name",
            "project": "your_mc_project",
            "endpoint": "http://service.cn-shanghai.maxcompute.aliyun.com/api"
        }
    },
    "task": {
        "node_name": "your_integration_node_name",
        "cron": "00 10 00-23/1 * * ?",
        "reader_datasource": "oss_your_ds_name",
        "reader_prefix": "camos/new_project/",
        "writer_datasource": "mc_your_ds_name",
        "writer_table": "your_target_table",
        "mc_partition_retention": 30
    }
}
```

**第三步**：在 `ddl/` 下放建表 SQL（Schema-Driven：此处写多少列，DataWorks 底层就会自动拉出来双向映射拼接，且随 `alter table` 持续自动增量叠加扩展）

```sql
CREATE TABLE IF NOT EXISTS your_target_table(
    `id` STRING COMMENT '用户ID',
    `age` STRING COMMENT '年龄'
)
PARTITIONED BY (pt STRING);
```

**第四步**：Commit & Push to main → GitHub Actions 自动触发全链路 5+ 节点的自动化重组与部署发布 ✅

---

## ⚙️ 核心引擎剖析

### 1. 配置超级合并映射（`config_merger.py`）
每次部署时按顺序深度混淆参数：
1. **获取底板**：从 `configuration/` 拾取无生命的原型 JSON 或 python 源码。
2. **叠加环境字典**：把 `setting-<env>.json` 中的定制参数暴力注入原型的血肉中。
3. **DDL 增量词法透视 (`_parse_all_columns_from_sqls`)**：脚本会自动按时间戳遍历这个环境自古至今所有跑过的 DDL 文件，利用正则提取所有的 `CREATE` 和 `ALTER ADD COLUMNS` 列记录，智能像滚雪球一般拼装出当前时刻这张表完整的“表结构画像”，然后自动转化填装至集成节点的数组映射关系里！无须人为维护繁琐 Mapping！

### 2. DataWorks API 万能外壳（`dataworks_client.py`）
该文件不单单是 SDK 调用器，更是包含了极具韧性的防御设施：
* **内置抗限流防护 (`_call_with_retry`)**：DataWorks 公共云拥有极低阈值的 QPS OpenAPI 请求频率。当大批量全量部署或创建时极易触发 `Throttling.Resource 400` 错误。此客户端拦截了所有查/改/写流量，一旦触碰警报，自适应执行 0.5s~8s 的指数安全退避 (Exponential Backoff)，保障流水线面对万级任务依旧固若金汤不崩溃。
* **差异对比更新工具**：内置了远端拉取 JSON Schema 的扁平拍平对比函数。在 Upsert 时能精确打印本地与远端究竟相差哪几个字符，只对实质变更文件下达指令，0 差异则强制忽略避免无谓耗费版本发布资源。

### 3. DDL 时序跟踪表机制（`create_table.py`）
依靠事先创建位于共享元数据目录 `shared/ddl-metadata.sql` 里的跟踪表 `database_changelog`。每次读取前获取已加载文件名：
- 未应用过的增量记录执行真实 MaxCompute 连接并执行 `pyodps`。
- 回写文件名入库确保极高幂等性 (Idempotent)。 

### 4. 发布与提交指令（`publish_node.py`）
底层借助 DataWorks Node Deploy 接口完成三阶段标准下发动作：
- `BUILD_PACKAGE` 打包发版。
- `PROD_CHECK` 强制触发质量、发布规约校验规则检测防泄漏。
- `PROD` 切流生效发布上线。
