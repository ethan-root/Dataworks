# DataWorks Data Integration — OSS to MaxCompute

使用 GitHub Actions + Python 自动化管理 Aliyun DataWorks 数据集成定时任务。

## 架构

```
GitHub Actions YAML  →  Python 脚本  →  DataWorks API
  (流程编排)           (控制逻辑)       (创建定时任务)
                           ↑
                    projects/*/config.json
                      (任务参数配置)
```

## 项目结构

```
Dataworks/
├── .github/workflows/
│   ├── dataworks_sync.yml         # 生产部署流水线
│   └── test_dataworks.yml         # 分步测试流水线
├── projects/                       # 品牌项目目录
│   ├── Test/config.json
│   ├── Gucci/config.json
│   └── Balenciaga/config.json
├── scripts/
│   ├── dataworks_client.py        # DataWorks API 封装类
│   ├── process_project.py         # 单项目处理逻辑
│   ├── deploy.py                  # CLI 主入口
│   └── requirements.txt           # Python 依赖
└── README.md
```

## 快速开始

### 1. 配置 GitHub Secrets

| Secret | 说明 |
|---|---|
| `ALIYUN_ACCESS_KEY_ID` | 阿里云 AK ID |
| `ALIYUN_ACCESS_KEY_SECRET` | 阿里云 AK Secret |
| `ALIYUN_REGION` | 区域（如 `cn-shanghai`）|
| `DATAWORKS_PROJECT_ID` | DataWorks 工作空间 ID |

### 2. 新增品牌项目

在 `projects/` 下创建新目录，添加 `config.json`：

```json
{
  "ProjectName": "BrandName",
  "OSS": {
    "DataSourceName": "oss_brand",
    "Endpoint": "https://oss-cn-shanghai-internal.aliyuncs.com",
    "Bucket": "brand-data-bucket",
    "BasePath": ""
  },
  "MaxCompute": {
    "DataSourceName": "odps_brand",
    "ProjectName": "brand_mc_project",
    "Endpoint": "http://service.cn-shanghai.maxcompute.aliyun.com/api"
  },
  "Tables": [
    {
      "Name": "table1",
      "OSS_Object": "path/*.parquet",
      "FileFormat": "parquet",
      "Columns": [
        {"name": "col1", "type": "string"},
        {"name": "col2", "type": "double"}
      ],
      "Partition": "pt"
    }
  ],
  "ResourceGroupIdentifier": "dataworks_default_resource_group",
  "Schedule": { "CronExpress": "00 00 02 * * ?", "CycleType": "DAY" }
}
```

### 3. 触发部署

- **自动触发**：Push `config.json` 到 `main` 分支
- **手动触发**：GitHub Actions → `workflow_dispatch`

### 4. 分步测试

使用 `[TEST] DataWorks Integration` 工作流，选择步骤：

| Step | 说明 |
|---|---|
| `check_cli` | 验证 SDK 连接 & 资源组列表 |
| `check_datasources` | 检查数据源是否存在 |
| `create_oss_ds` | 创建 OSS 数据源 |
| `create_odps_ds` | 创建 MaxCompute 数据源 |
| `create_job` | 创建同步 Job（输出 FileId）|
| `submit` | 提交任务 |
| `deploy` | 发布到生产 |

## 本地执行

```bash
pip install -r scripts/requirements.txt

# 设置环境变量
export ALIBABA_CLOUD_ACCESS_KEY_ID=xxx
export ALIBABA_CLOUD_ACCESS_KEY_SECRET=xxx
export ALIYUN_REGION=cn-shanghai
export DATAWORKS_PROJECT_ID=1186017

# 全量部署
python scripts/deploy.py

# 指定项目
python scripts/deploy.py --projects Gucci

# 分步测试
python scripts/deploy.py --step check_cli
python scripts/deploy.py --step create_job --project-dir projects/Test
```
