#!/bin/bash
# ==============================================================================
# utils.sh — DataWorks API 工具函数
# 封装所有 DataWorks OpenAPI 调用
# ==============================================================================

set -euo pipefail

# ---- 颜色输出 ----
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

# ==============================================================================
# job_exists — 检查同名 Job 是否已存在于 DataWorks
# 参数: $1 = ProjectId, $2 = JobName
# 返回: 0=存在, 1=不存在
# ==============================================================================
job_exists() {
    local project_id="$1"
    local job_name="$2"

    log_info "Checking if job '$job_name' exists in project $project_id ..."

    local result
    result=$(aliyun dataworks-public ListFiles \
        --ProjectId "$project_id" \
        --Keyword "$job_name" \
        --PageSize 100 \
        --PageNumber 1 \
        2>&1) || {
        log_warn "ListFiles API call returned non-zero, treating as not found."
        return 1
    }

    # 检查返回的文件列表中是否有精确匹配的文件名
    local match_count
    match_count=$(echo "$result" | jq -r "[.Data.Files[]? | select(.FileName == \"$job_name\")] | length" 2>/dev/null || echo "0")

    if [ "$match_count" -gt 0 ]; then
        log_info "Job '$job_name' already exists (found $match_count match(es))."
        return 0
    else
        log_info "Job '$job_name' does not exist."
        return 1
    fi
}

# ==============================================================================
# datasource_exists — 检查数据源是否已存在
# 参数: $1 = ProjectId, $2 = DataSourceName
# 返回: 0=存在, 1=不存在
# ==============================================================================
datasource_exists() {
    local project_id="$1"
    local ds_name="$2"

    log_info "Checking if datasource '$ds_name' exists ..."

    local result
    result=$(aliyun dataworks-public ListDataSources \
        --ProjectId "$project_id" \
        --Name "$ds_name" \
        --PageSize 20 \
        --PageNumber 1 \
        2>&1) || {
        log_warn "ListDataSources API call failed, treating as not found."
        return 1
    }

    local match_count
    match_count=$(echo "$result" | jq -r "[.Data.DataSources[]? | select(.Name == \"$ds_name\")] | length" 2>/dev/null || echo "0")

    if [ "$match_count" -gt 0 ]; then
        log_info "Datasource '$ds_name' exists."
        return 0
    else
        log_info "Datasource '$ds_name' does not exist."
        return 1
    fi
}

# ==============================================================================
# ensure_oss_datasource — 确保 OSS 数据源存在，不存在则创建
# 参数: $1 = ProjectId, $2 = config.json 路径
# ==============================================================================
ensure_oss_datasource() {
    local project_id="$1"
    local config="$2"

    local ds_name
    ds_name=$(jq -r '.OSS.DataSourceName' "$config")

    if datasource_exists "$project_id" "$ds_name"; then
        return 0
    fi

    log_info "Creating OSS datasource '$ds_name' ..."

    local endpoint bucket
    endpoint=$(jq -r '.OSS.Endpoint' "$config")
    bucket=$(jq -r '.OSS.Bucket' "$config")

    local conn_props
    conn_props=$(jq -n \
        --arg endpoint "$endpoint" \
        --arg bucket "$bucket" \
        '{
            "envType": "Prod",
            "endpoint": $endpoint,
            "bucket": $bucket
        }')

    local result
    result=$(aliyun dataworks-public CreateDataSource \
        --ProjectId "$project_id" \
        --Name "$ds_name" \
        --DataSourceType "oss" \
        --ConnectionProperties "$conn_props" \
        2>&1)

    log_info "CreateDataSource (OSS) result: $result"

    # 验证创建成功
    local success
    success=$(echo "$result" | jq -r '.Success' 2>/dev/null || echo "false")
    if [ "$success" != "true" ]; then
        log_error "Failed to create OSS datasource '$ds_name'"
        log_error "Response: $result"
        return 1
    fi

    log_info "OSS datasource '$ds_name' created successfully."
}

# ==============================================================================
# ensure_odps_datasource — 确保 MaxCompute 数据源存在，不存在则创建
# 参数: $1 = ProjectId, $2 = config.json 路径
# ==============================================================================
ensure_odps_datasource() {
    local project_id="$1"
    local config="$2"

    local ds_name
    ds_name=$(jq -r '.MaxCompute.DataSourceName' "$config")

    if datasource_exists "$project_id" "$ds_name"; then
        return 0
    fi

    log_info "Creating MaxCompute datasource '$ds_name' ..."

    local mc_project mc_endpoint
    mc_project=$(jq -r '.MaxCompute.ProjectName' "$config")
    mc_endpoint=$(jq -r '.MaxCompute.Endpoint' "$config")

    local conn_props
    conn_props=$(jq -n \
        --arg project "$mc_project" \
        --arg endpoint "$mc_endpoint" \
        '{
            "envType": "Prod",
            "projectName": $project,
            "endpoint": $endpoint
        }')

    local result
    result=$(aliyun dataworks-public CreateDataSource \
        --ProjectId "$project_id" \
        --Name "$ds_name" \
        --DataSourceType "odps" \
        --ConnectionProperties "$conn_props" \
        2>&1)

    log_info "CreateDataSource (MaxCompute) result: $result"

    local success
    success=$(echo "$result" | jq -r '.Success' 2>/dev/null || echo "false")
    if [ "$success" != "true" ]; then
        log_error "Failed to create MaxCompute datasource '$ds_name'"
        log_error "Response: $result"
        return 1
    fi

    log_info "MaxCompute datasource '$ds_name' created successfully."
}

# ==============================================================================
# generate_task_content — 根据 config.json 和 table index 生成 TaskContent JSON
# 参数: $1 = config.json 路径, $2 = table index (0-based)
# 输出: 打印 JSON 字符串到 stdout
# ==============================================================================
generate_task_content() {
    local config="$1"
    local table_idx="$2"

    local oss_ds_name oss_base_path
    oss_ds_name=$(jq -r '.OSS.DataSourceName' "$config")
    oss_base_path=$(jq -r '.OSS.BasePath' "$config")

    local odps_ds_name
    odps_ds_name=$(jq -r '.MaxCompute.DataSourceName' "$config")

    local table_name oss_object file_format field_delimiter encoding
    table_name=$(jq -r ".Tables[$table_idx].Name" "$config")
    oss_object=$(jq -r ".Tables[$table_idx].OSS_Object" "$config")
    file_format=$(jq -r ".Tables[$table_idx].FileFormat" "$config")
    field_delimiter=$(jq -r ".Tables[$table_idx].FieldDelimiter" "$config")
    encoding=$(jq -r ".Tables[$table_idx].Encoding // \"UTF-8\"" "$config")

    # 构建 Reader columns (按 index 索引)
    local reader_columns
    reader_columns=$(jq -c "[.Tables[$table_idx].Columns | to_entries[] | {type: .value.type, value: (.key | tostring)}]" "$config")

    # 构建 Writer columns (列名列表)
    local writer_columns
    writer_columns=$(jq -c "[.Tables[$table_idx].Columns[].name]" "$config")

    # 拼接完整 OSS object 路径
    local full_oss_object="${oss_base_path}${oss_object}"

    # 生成 TaskContent JSON
    jq -n \
        --arg oss_ds "$oss_ds_name" \
        --arg oss_obj "$full_oss_object" \
        --argjson reader_cols "$reader_columns" \
        --arg delimiter "$field_delimiter" \
        --arg enc "$encoding" \
        --arg fmt "$file_format" \
        --arg odps_ds "$odps_ds_name" \
        --arg table "$table_name" \
        --argjson writer_cols "$writer_columns" \
        '{
            "type": "job",
            "version": "2.0",
            "steps": [
                {
                    "stepType": "oss",
                    "parameter": {
                        "datasource": $oss_ds,
                        "object": [$oss_obj],
                        "column": $reader_cols,
                        "fieldDelimiter": $delimiter,
                        "encoding": $enc,
                        "fileFormat": $fmt
                    },
                    "name": "Reader",
                    "category": "reader"
                },
                {
                    "stepType": "odps",
                    "parameter": {
                        "datasource": $odps_ds,
                        "table": $table,
                        "column": $writer_cols,
                        "truncate": true
                    },
                    "name": "Writer",
                    "category": "writer"
                }
            ],
            "setting": {
                "speed": {
                    "channel": 1,
                    "throttle": false
                },
                "errorLimit": {
                    "record": 0
                }
            },
            "order": {
                "hops": [
                    {
                        "from": "Reader",
                        "to": "Writer"
                    }
                ]
            }
        }'
}

# ==============================================================================
# create_sync_task — 创建数据集成同步任务
# 参数: $1 = ProjectId, $2 = JobName, $3 = TaskContent, $4 = config.json 路径
# 输出: 打印 FileId 到 stdout
# ==============================================================================
create_sync_task() {
    local project_id="$1"
    local job_name="$2"
    local task_content="$3"
    local config="$4"

    local resource_group
    resource_group=$(jq -r '.ResourceGroupIdentifier' "$config")

    local task_param
    task_param=$(jq -n \
        --arg rg "$resource_group" \
        '{
            "FileFolderPath": "/",
            "ResourceGroup": $rg
        }')

    log_info "Creating DI sync task '$job_name' ..."

    local result
    result=$(aliyun dataworks-public CreateDISyncTask \
        --ProjectId "$project_id" \
        --TaskType "DI_OFFLINE" \
        --TaskName "$job_name" \
        --TaskParam "$task_param" \
        --TaskContent "$task_content" \
        2>&1)

    log_info "CreateDISyncTask result: $result"

    local status
    status=$(echo "$result" | jq -r '.Data.Status' 2>/dev/null || echo "fail")
    if [ "$status" != "success" ]; then
        local message
        message=$(echo "$result" | jq -r '.Data.Message // "Unknown error"' 2>/dev/null)
        log_error "Failed to create task '$job_name': $message"
        return 1
    fi

    local file_id
    file_id=$(echo "$result" | jq -r '.Data.FileId')
    log_info "Task '$job_name' created with FileId: $file_id"
    echo "$file_id"
}

# ==============================================================================
# submit_file — 提交文件到调度系统
# 参数: $1 = ProjectId, $2 = FileId
# ==============================================================================
submit_file() {
    local project_id="$1"
    local file_id="$2"

    log_info "Submitting file $file_id ..."

    local result
    result=$(aliyun dataworks-public SubmitFile \
        --ProjectId "$project_id" \
        --FileId "$file_id" \
        2>&1)

    log_info "SubmitFile result: $result"

    local success
    success=$(echo "$result" | jq -r '.Success' 2>/dev/null || echo "false")
    if [ "$success" != "true" ]; then
        log_error "Failed to submit file $file_id"
        log_error "Response: $result"
        return 1
    fi

    log_info "File $file_id submitted successfully."
}

# ==============================================================================
# deploy_file — 发布文件到生产环境
# 参数: $1 = ProjectId, $2 = FileId
# ==============================================================================
deploy_file() {
    local project_id="$1"
    local file_id="$2"

    log_info "Deploying file $file_id to production ..."

    local result
    result=$(aliyun dataworks-public DeployFile \
        --ProjectId "$project_id" \
        --FileId "$file_id" \
        2>&1)

    log_info "DeployFile result: $result"

    local success
    success=$(echo "$result" | jq -r '.Success' 2>/dev/null || echo "false")
    if [ "$success" != "true" ]; then
        log_error "Failed to deploy file $file_id"
        log_error "Response: $result"
        return 1
    fi

    log_info "File $file_id deployed to production successfully."
}
