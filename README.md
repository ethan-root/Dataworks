# DataWorks DataOps — OSS to MaxCompute

基于 **GitHub Actions + Python** 自动化编排 Aliyun DataWorks 数据集成任务，实现配置驱动、多环境串行部署与代码化管理的 DataOps CI/CD 体系。

---

## 📐 整体架构

```text
Dataworks/
├── .github/workflows/               # CI/CD 流水线定义
│   ├── pr_validation.yml            # ★ PR合并前安全检查（历史 SQL 防篡改拦截）
│   ├── datawork_node_create_update.yml  # 主流水线（Push/手动触发，全量节点组装与创建/更新）
│   ├── _deploy_env.yml              # 可复用工作流（单环境部署，调用 ci_runner.py）
│   └── database_update.yml          # DDL 执行流水线
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

### 2. PR 安全防篡改代码审查流水线 (`pr_validation.yml`)

**触发条件**：
- 开发人员发起向 `main` 或 `dev` 的 Pull Request (合并请求) 时自动触发。

**核心拦截能力**：
- 🛡️ **Git-based DDL Mutability Check (历史 DDL 防篡改校验)**：通过纯 Git 原生特性 (`git diff --name-status`)，毫秒级侦测 PR 中是否恶意修改或删除了 `features/*/ddl/` 下已经发布过的历史老 `.sql` 文件。
- 零数据库网络依赖、零 AK/SK 密文泄漏风险。
- **强制规范规约**：强制打断企图覆盖老历史表结构的动作（Cancel PR），逼迫开发者必须经过**“新建增量 `.sql` 文件”**才是演进表结构的唯一合法途径，100% 捍卫生产数据基线安全。

---

## 🐣 快速上手 (Quick Start)

如果你是 0 基础的新手，想要把这个项目套用在自己的真实业务上，只需跟着以下 4 步走：

### 第一步：配置 GitHub 密钥 (Secrets)
项目必须拥有操作你阿里云 DataWorks 的权限。请进入你这个 GitHub 仓库的 `Settings -> Secrets and variables -> Actions`，添加以下机密参数：
- `ALIBABA_CLOUD_ACCESS_KEY_ID`: 你的阿里云 AccessKey ID
- `ALIBABA_CLOUD_ACCESS_KEY_SECRET`: 你的阿里云 AccessKey Secret
- `ALIYUN_REGION`: 你的 DataWorks 所在地域 (如 `cn-shanghai`)
- `DATAWORKS_PROJECT_ID`: 你 DataWorks 真实工作空间的纯数字 ID

### 第二步：修改项目的全局“底板”参数
换了新账号或新项目，必须先修改底层网关参数。
打开 `configuration/integration-config.json` 文件（这是所有集成节点的模板引擎），修改以下核心字段为**你自己的专属值**：
- `"owner"`: 填你自己的阿里云账号纯数字 UID。
- `"resource_group"`: 填你实际购买的 DataWorks 独享数据集成资源组标识（Serverless_res_group_xxx）。
- `"metadata"` 下的 `"owner"` 和 `"project.projectIdentifier"` 等参数也要对应修改为你自己的空间信息。
*(注：`upstream-node-config.json` 等其他底板文件中的 owner 最好也一并顺手改掉)*

### 第三步：配置你的真实业务参数
在这个项目中，你几乎永远不需要去碰 `scripts/` 下的 Python 代码，所有开发只需改配置！
1. 复制现有的 `features/test-feature` 文件夹，将其重命名为你的业务名（比如 `features/my-first-job`）。
2. 打开里面的 `setting-dev.json`，你会看到业务参数：
   - 把 `datasource.oss.bucket` 改为你真正的 OSS 桶名称。
   - 把 `task.reader_prefix` 改为你要读取并同步的真实 OSS 路径。
   - 把 `task.writer_table` 改为目的端的真实表名。
3. 打开 `ddl/` 目录下的 `.sql` 文件，按你实际需求编写真实的 `CREATE TABLE` 表结构。表结构中的字段，系统会自动解析并跟你的 JSON 配置融合成精准的同步映射！

### 第四步：一键推送，自动建成！
把上面的修改保存，执行提交并将代码 Push 到 `main` 分支：
```bash
git add .
git commit -m "feat: run my first dataworks job"
git push origin main
```
接下来，只要打开 GitHub 仓库的 **Actions** 页面，你就能看到一个机器人正在自动帮你执行建表、拼接节点、连线关联等工作，并最终把完整的链路原封不动地发布到你的 DataWorks 开发环境中去！

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
4. **节点变量透传编织**：自动扫描上游节点（如拉取最新 Parquet 文件的 Python 节点）产生的临时变量输出，自动捕获并在下层核心的数据集成甚至下游归档组件中，用 `${变量名}` 做全链路的无缝参数传递与串联。

### 2. DataWorks API 万能外壳（`dataworks_client.py`）
该文件不单单是 SDK 调用器，更是包含了极具韧性的防御设施：
* **内置抗限流防护 (`_call_with_retry`)**：DataWorks 公共云拥有极低阈值的 QPS OpenAPI 请求频率。当大批量全量部署或创建时极易触发 `Throttling.Resource 400` 错误。此客户端拦截了所有查/改/写流量，施加了严格的串行限制锁并配置了安全封顶的指数退避 (Exponential Backoff)，保障流水线面对部署洪峰依旧固若金汤不崩溃。
* **智能 Diff 防损耗阀（保护每日 API 余额护城河）**：除了 QPS，非企业版 DataWorks 每日有 100~1000 次死线限额。客户端内置了极其深度的远端配置拉取与跨层级扁平拍平对比函数 (AST Level Diff)。在发起 `UpdateNode` 前，能自动脱敏规避系统生成的随机 UUID/时间戳区别。只针对有效业务修改（如字段映射变化、脚本语言替换、资源组换绑）才下达 API 请求。若完全一致则直接 `Return` 斩断无意义更新，**每日为公司主账号节省数以百计的珍贵 API 调用计费配额**。

### 3. DDL 时序跟踪表机制（`create_table.py`）
依靠事先创建位于共享元数据目录 `shared/ddl-metadata.sql` 里的跟踪表 `database_changelog`。每次读取前获取已加载文件名：
- 未应用过的增量记录执行真实 MaxCompute 连接并执行 `pyodps`。
- 回写文件名入库确保极高幂等性 (Idempotent)。 

### 4. 发布与提交指令（`publish_node.py`）
### 4. 发布与提交指令（`publish_node.py`）
底层借助 DataWorks Node Deploy 接口完成三阶段标准下发动作：
- `BUILD_PACKAGE` 打包发版。
- `PROD_CHECK` 强制触发质量、发布规约校验规则检测防泄漏。
- `PROD` 切流生效发布上线。

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

---

## 📜 Scripts 脚本核心逻辑

### 编排层

#### `ci_runner.py` — 部署总指挥

被 `_deploy_env.yml` 中的一行调用，封装了完整的创建/更新决策逻辑：

```text
接收 --feature-list 和 --env 参数
  └── 遍历每个 feature
       ├── 校验目录和 setting-<env>.json 是否存在
       ├── 调用 check_integration_node.py → 判断节点存在与否
       │    ├── 节点已存在 → UPDATE 分支 (全流程同步)
       │    │    ├── 运行 create_table 更新 Schema
       │    │    ├── 运行 create_upstream_node 全量检查
       │    │    ├── 运行 update_integration_node 注入最新参数
       │    │    └── 运行下游/清理节点同步 + publish_node.py 下发生产
       │    └── 节点不存在 → CREATE 分支 (全流程创建)
       │         ├── check/create OSS/MC 数据源
       │         └── 循序递进完成：表创建 -> 节点依序创建 -> 发布上线
```

---

### 节点辅助操作脚本群

#### `check_integration_node.py`
1. 从 `config_merger` 读取 `node_name`
2. 调用 `get_node_id()` 精确查找
3. **退出码约定**：找到 → `exit 0`；未找到 → `exit 1`（`ci_runner.py` 依赖此做 create/update 分支判断）

#### `create_integration_node.py` / `update_integration_node.py`
1. `ci_runner` 各自调用的主要钩子。合并完整配置（含 Schema-Driven 字段映射与最新血缘依赖）
2. 借用客户端的内置差异化阻断控制台打印对比情况，只有当发生参数内容跳变时，才会借由 `UpdateNode` 切流重组。

#### 数据源探活与补偿（`check_xx_ds` / `create_xx_ds`）
- 采取主动侦测机制，遇到 OpenAPI 报错 "400 名称重复" 等已存在特性时静默吞噬返回真值，确保无状态集群能够极速放行流水线工作车间。

---

### 辅助与治理工具

#### `validate_row_count.py` — 行数对比验证（部署卡点测试工具）
- 利用 `pyodps` 双重并发比对 OSS 外表映射层与实际落库层的行数。不一致时拦截部署管道防止脏数据混入。


