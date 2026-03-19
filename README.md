# DataWorks DataOps — OSS to MaxCompute

基于 **GitHub Actions + Python** 自动化编排 Aliyun DataWorks 数据集成任务，实现配置驱动、多环境串行部署的 DataOps CI/CD 体系。

---

## 📐 整体架构

```text
Dataworks/
├── .github/workflows/               # CI/CD 流水线定义
│   ├── datawork_node_create_update.yml  # 主流水线（push 触发，节点创建/更新）
│   ├── _deploy_env.yml              # 可复用工作流（单环境部署，调用 ci_runner.py）
│   ├── database_update.yml          # DDL 执行流水线（plan / apply / apply_all）
│   └── pr_validation.yml            # PR 安全扫描（SonarCloud + GitGuardian + DDL 报告）
│
├── configuration/                   # 全局底板配置（系统级参数，无需业务人员修改）
│   ├── integration-config.json      # 数据集成节点系统级底板
│   ├── oss-datasource.json          # OSS 数据源系统级底板
│   ├── maxcompute-datasource.json   # MaxCompute 数据源系统级底板
│   ├── parquet-name-get.json        # 上游 Parquet 节点模板
│   ├── parquet-file-completed.json  # 下游 Parquet 归档节点模板
│   └── data-clean.json              # MaxCompute 数据清理节点模板
│
├── features/                        # 业务工作区（开发人员每次只改这里）
│   ├── shared/
│   │   └── ddl-metadata.sql         # 全局元数据表 DDL（跨 feature 共享）
│   └── {feature-name}/              # 以集成节点业务名为目录名
│       ├── setting-dev.json          # DEV 环境业务参数
│       ├── setting-qa.json           # QA 环境业务参数
│       ├── setting-preprod.json      # PRE-PROD 环境业务参数
│       ├── setting-prod.json         # PROD 环境业务参数
│       └── ddl/
│           └── YYYYMMDDHHMI_create.sql  # MaxCompute 建表 SQL（带时间戳版本）
│
└── scripts/                         # Python 脚本（三层架构）
    ├── ci_runner.py                 # ★ 编排层：发起部署流程的总指挥
    ├── config_merger.py             # 配置合并引擎（底板 + 业务参数 → 完整配置）
    ├── dataworks_client.py          # DataWorks SDK 封装（get/create/update/publish）
    ├── check_integration_node.py    # 检查节点是否存在
    ├── create_integration_node.py   # 创建数据集成节点
    ├── update_integration_node.py   # 更新数据集成节点（含 remote diff 对比）
    ├── check_oss_ds.py              # 检查 OSS 数据源
    ├── create_oss_ds.py             # 创建 OSS 数据源
    ├── check_mc_ds.py               # 检查 MaxCompute 数据源
    ├── create_mc_ds.py              # 创建 MaxCompute 数据源
    ├── create_table.py              # 在 MaxCompute 中执行建表 SQL（幂等）
    ├── create_downstream_node.py    # 创建下游节点（Parquet → completed）
    ├── create_python_cp_node.py     # 创建 Python 节点（cp / delete 两种类型）
    ├── publish_node.py              # 三阶段发布（BUILD → PROD_CHECK → PROD）
    └── requirements.txt             # Python 依赖
```

---

## 🚀 CI/CD 流水线说明

### 1. 节点部署流水线（`datawork_node_create_update.yml`）

**触发条件**：向 `main` 分支 push，且 `features/**` 目录下有文件变更。

**执行流程（三层架构）**：

```
YAML（协调层）          Python 编排层              Python 执行层
─────────────          ────────────               ────────────
detect 变更 feature  →  ci_runner.py            →  check_*.py
        │               ├── 节点已存在 → UPDATE      create_*.py
        ↓               │   └── update → publish    update_*.py
deploy-dev              └── 节点不存在 → CREATE     publish_node.py
deploy-qa                   ├── 确保 OSS 数据源
deploy-preprod              ├── 确保 MC 数据源
deploy-prod 🔒             ├── 创建 MC 表
```

**多环境串行**：`dev → qa → preprod → prod`，每个环境使用对应的 `setting-<env>.json`，`prod` 需 GitHub Environment 人工审批。

---

### 2. DDL 执行流水线（`database_update.yml`）

**触发条件**：手动触发（`workflow_dispatch`）或 PR 到 main（plan 模式）。

| 模式 | 说明 |
|---|---|
| `plan` | 打印将要变更的 DDL 文件，供 Review，不执行 |
| `apply` | 对指定单一环境执行 DDL（紧急修复使用）|
| `apply_all` | 串行执行所有环境 `dev → qa → preprod → prod`，`prod` 需审批 |

---

### 3. PR 安全验证（`pr_validation.yml`）

**触发条件**：向 `main` 发起 Pull Request。

| Job | 工具 | 状态 |
|---|---|---|
| 🔍 SonarCloud 代码质量扫描 | `SonarSource/sonarcloud-github-action` | 待接入 `SONAR_TOKEN` |
| 🔐 GitGuardian 密钥泄露检测 | `GitGuardian/ggshield-action` | 待接入 `GITGUARDIAN_API_KEY` |
| 📋 DDL 变更报告 | git diff | 立即生效 |

> 接入账号后，删除 `continue-on-error: true` 即可开启阻断 PR 的强校验。

---

## ✍️ 业务开发指南

### 新建一个 Feature（数据集成任务）

**第一步**：在 `features/` 下创建目录（目录名 = DataWorks 节点名前缀）

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

**第二步**：填写 `setting-dev.json`（各环境仅参数值不同）

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
        "reader_path": "parquet/*.parquet",
        "writer_datasource": "mc_your_ds_name",
        "writer_table": "your_target_table",
        "writer_partition": "pt='${bizdate}'"
    }
}
```

**第三步**：在 `ddl/` 下放建表 SQL（Schema-Driven，字段自动映射到集成任务）

```sql
CREATE TABLE IF NOT EXISTS your_target_table(
    `col1` STRING COMMENT '',
    `col2` STRING COMMENT ''
)
PARTITIONED BY (pt STRING)
lifecycle 36500;
```

**第四步**：Commit & Push to main → GitHub Actions 自动触发全流程部署 ✅

---

## ⚙️ 配置合并引擎（`config_merger.py`）

每次部署时，`config_merger.py` 按以下顺序组装完整配置：

1. **读取底板**：从 `configuration/` 加载全局系统级 JSON 模板
2. **叠加业务参数**：用 `setting-<env>.json` 中的业务字段覆写底板
3. **Schema-Driven 字段映射**：从 `ddl/*.sql` 正则提取列名，自动生成 OSS Reader 和 MaxCompute Writer 的 Column Array
4. **投递**：向 DataWorks OpenAPI 发起 Create / Update 请求

---

## 🔑 环境变量（GitHub Secrets）

| Secret 名称 | 说明 |
|---|---|
| `DATAWORKS_PROJECT_ID` | DataWorks 工作空间 ID |
| `ALIYUN_ACCESS_KEY_ID` | 阿里云 AccessKey ID |
| `ALIYUN_ACCESS_KEY_SECRET` | 阿里云 AccessKey Secret |
| `ALIYUN_REGION` | 地域（如 `cn-shanghai`）|
| `MAXCOMPUTE_PROJECT` | MaxCompute 项目名 |
| `MAXCOMPUTE_ENDPOINT` | MaxCompute API Endpoint |
| `SONAR_TOKEN` | （可选）SonarCloud 扫描令牌 |
| `GITGUARDIAN_API_KEY` | （可选）GitGuardian API Key |

**本地开发调试**：

```bash
export ALIBABA_CLOUD_ACCESS_KEY_ID="xxx"
export ALIBABA_CLOUD_ACCESS_KEY_SECRET="xxx"
export ALIYUN_REGION="cn-shanghai"
export DATAWORKS_PROJECT_ID="xxxxxx"
export MAXCOMPUTE_PROJECT="your_mc_project"
export MAXCOMPUTE_ENDPOINT="http://service.cn-shanghai.maxcompute.aliyun.com/api"

# 本地模拟一次 dev 环境部署（无需触发 GitHub Actions）
python scripts/ci_runner.py --feature-list my-new-feature --env dev
```
