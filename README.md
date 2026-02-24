# DataWorks Data Integration — OSS to MaxCompute

使用 GitHub Actions 自动化管理 Aliyun DataWorks 数据集成任务，支持多品牌项目并行管理。

## 项目结构

```
Dataworks/
├── .github/workflows/
│   └── dataworks_sync.yml        # GitHub Actions 工作流
├── projects/                      # 品牌项目目录
│   ├── Gucci/config.json
│   ├── Balenciaga/config.json
│   └── .../config.json
├── scripts/
│   ├── deploy.sh                  # 主部署入口
│   ├── process_project.sh         # 单项目处理逻辑
│   └── utils.sh                   # DataWorks API 工具函数
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
    "BasePath": "data/export/"
  },
  "MaxCompute": {
    "DataSourceName": "odps_brand",
    "ProjectName": "brand_mc_project",
    "Endpoint": "http://service.cn-shanghai.maxcompute.aliyun.com/api"
  },
  "Tables": [
    {
      "Name": "TableName",
      "OSS_Object": "table_path/",
      "FileFormat": "csv",
      "FieldDelimiter": ",",
      "Encoding": "UTF-8",
      "Columns": [
        {"name": "col1", "type": "string"},
        {"name": "col2", "type": "double"}
      ]
    }
  ],
  "ResourceGroupIdentifier": "S_res_group_xxx",
  "Schedule": {
    "CronExpress": "00 00 02 * * ?",
    "CycleType": "DAY"
  }
}
```

### 3. 触发部署

- **自动触发**：Push `config.json` 变更到 `main` 分支
- **手动触发**：GitHub Actions → `workflow_dispatch`，可指定项目名

## JobName 命名规范

```
JobName = {ProjectName}_{TableName}
例: Gucci_Item, Gucci_User, Balenciaga_Item
```

## 处理逻辑

1. 检查 Job 是否存在 → 存在则跳过
2. 检查/创建 OSS 数据源
3. 检查/创建 MaxCompute 数据源
4. 创建同步任务（`CreateDISyncTask`）
5. 提交（`SubmitFile`）→ 发布到生产（`DeployFile`）
