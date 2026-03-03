# DataWorks Data Integration — OSS to MaxCompute

依靠 GitHub Actions + Python 自动化编排管理 Aliyun DataWorks 数据集成任务，全面采用中心化架构与 Schema-Driven（Schema 驱动）的最佳实践。

## 🏆 最新架构理念 (中心化配置池)

为了避免每个项目的环境文件夹中存在大量底层“八股文” JSON 配置文件的复制粘贴，系统采用了**“配置（底板）与业务（扩展）完全分离”**的架构模式。

### 目录结构

```text
Dataworks/
├── .github/workflows/               # GitHub Actions 流水线定义
├── default-setting/                 # 💥 全局底层模板池 (八股文统一存放地)
│   ├── integration-config.json      # 数据集成节点底层系统级参数 (原 task-config.json)
│   ├── oss-datasource.json          # OSS 数据源底层系统级参数
│   ├── maxcompute-datasource.json   # MC 数据源底层系统级参数
│   ├── python_cp_node.json          # Python 拷贝节点 (扩展预留)
│   ├── downstream.json              # 下游清理节点 (扩展预留)
│   └── data_clean.json              # MC 清理节点 (扩展预留)
├── feature/                         # ✨ 开发业务工作区 (真正干活的地方)
│   └── user-feature/                
│       ├── dev/                     
│       │   ├── setting.json         # 核心业务参数大本营 (只需十行)
│       │   └── create-table.sql     # 建表 SQL 
│       ├── qa/                      # QA 环境同理，只需存放一份 setting.json + sql
│       └── prod/                    # Prod 环境...
└── scripts/
    ├── config_merger.py             # 核心配置组装与合并引擎
    ├── create_integration_node.py   # 创建离线同步节点 (读取 integration-config)
    ├── create_oss_ds.py             # 创建 OSS 数据源
    ├── create_mc_ds.py              # 创建 MC 数据源
    ├── publish_node.py              # 执行在线发布 Pipeline 到生产环境
    └── dataworks_client.py          # 底层 API Client 封装
```

## 🚀 核心工作流：如何新建/修改一个任务？

在全新的架构下，普通开发人员（数据研发）**绝对不需要**去修改 `default-setting` 下的任何模板。

你只需要在 `feature/项目名称/开发环境/` 目录下，维护以下两个文件：

### 1. `create-table.sql` (业务表结构)
该系统全自动实行 **Schema-Driven** 映射！你在 SQL 里定义的列名（如 `name`、`age`），将在 Python 脚本执行时，使用正则表达式被提取，并**自动推导出数据集成节点中的源端 (Reader) 与目标端 (Writer) 的 1对1 字段映射**，彻底消灭手动维护庞大 JSON Columns 结构的泥潭。

```sql
CREATE TABLE IF NOT EXISTS feature_demo(
`name`                          STRING COMMENT '',
`age`                           STRING COMMENT '',
`location`                      STRING COMMENT ''
)
COMMENT 'null'
PARTITIONED BY (pt STRING) 
lifecycle 36500;
```

### 2. `setting.json` (轻量级业务配置)
抛开了繁重的底层参数（如 `resourceGroup`, `envType`, `uuid` 等），你只需要填写当前任务最核心的名字、频率、数据源绑定流向：

```json
{
    "datasource": {
        "oss": {
            "name": "oss_demo",
            "bucket": "your-bucket-name",
            "endpoint": "oss-cn-shanghai.aliyuncs.com"
        },
        "mc": {
            "name": "mc_demo",
            "project": "your_mc_project",
            "endpoint": "http://service.cn-shanghai.maxcompute.aliyun.com/api"
        }
    },
    "task": {
        "node_name": "update_task_3",
        "cron": "00 10 00-23/1 * * ?",
        "reader_datasource": "oss_demo",
        "reader_path": "parquet/*.parquet",
        "writer_datasource": "mc_demo",
        "writer_table": "feature_demo",
        "writer_partition": "pt='${bizdate}'"
    }
}
```

## ⚙️ CI/CD 引擎 (`config_merger.py`) 是如何工作的？

当 GitHub Actions （或者本地）触发部署脚本时，底层处理链路如下：
1. **寻找底板**：Python 去项目根目录下的 `default-setting/` 下寻找对应的基础模板（如 `integration-config.json`）。
2. **读取业务设定**：读取目标环境下的 `setting.json`。
3. **强覆盖**：将 `setting.json` 中的关键属性提取，“暴力且精准地”覆写进底板中。
4. **解析注入映射**：打开同名目录下的 `create-table.sql`，提取出字段 List。将这个 List 渲染成 OpenAPI 规定的 Reader / Writer Column Arrays。
5. **投递**：向 DataWorks OpenAPI 投递这份集 大成、完美且合法 的巨型配置对象。

## 🔧 GitHub Actions 使用指南

进入项目 GitHub 页面的 **Actions** 选项卡，选择 `[TEST] DataWorks Integration` 工作流并发起 `Run workflow`：

1. **`create_oss_ds` / `create_mc_ds`**：基于指定环境的 `setting.json` 自动配置系统数据源。
2. **`create_integration_node`**：在 DataWorks 开发环境建立/更新（Upsert）离线同步节点。
3. **`publish_node`**：使用官方 2024-05-18 `Pipeline API` 进行工业级上线投产（包含 `BUILD_PACKAGE`, `PROD_CHECK` 生产合规安全校验, `PROD` 三阶段流转）。

*系统自动通过跨目录查询锁定指定环境的 `setting.json` 来作为部署上下文！*

## 🔑 本地开发运行 (环境变量要求)

如果要在本地跑 Python 测试脚本，必须在环境变量中装载：

```bash
export ALIBABA_CLOUD_ACCESS_KEY_ID="xxx"
export ALIBABA_CLOUD_ACCESS_KEY_SECRET="xxx"
export ALIYUN_REGION="cn-shanghai"
export DATAWORKS_PROJECT_ID="1186017"
```

## 未来扩展设计 (Adding New Node Types)
有了这套 `default-setting` 模型，如果团队欲扩展比如 “Python 任务节点”，仅需：
1. 在 `default-setting/` 新建一个 `python_node.json` 底板。
2. 在 `scripts/` 下新写一个 `create_python_node.py`。
3. script 中调用 `load_base_config` 抓底板、拿 `setting.json` 里 `[python]` block 下的属性覆写，即可无缝加入自动化家族！
