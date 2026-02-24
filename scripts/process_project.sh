#!/bin/bash
# ==============================================================================
# process_project.sh — 处理单个品牌项目
# 读取 config.json → 遍历 Tables → 检查/创建 Job
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/utils.sh"

# ==============================================================================
# process_project — 处理单个品牌项目的所有 Table
# 参数: $1 = 项目目录路径, $2 = DataWorks ProjectId
# ==============================================================================
process_project() {
    local project_dir="$1"
    local dataworks_project_id="$2"
    local config="${project_dir}/config.json"

    # 验证 config.json 存在
    if [ ! -f "$config" ]; then
        log_error "config.json not found in $project_dir"
        return 1
    fi

    # 读取项目基本信息
    local project_name
    project_name=$(jq -r '.ProjectName' "$config")

    local table_count
    table_count=$(jq '.Tables | length' "$config")

    echo ""
    echo "================================================================="
    log_info "Processing Project: $project_name ($table_count tables)"
    echo "================================================================="

    # 跟踪统计
    local created=0
    local skipped=0
    local failed=0

    # 遍历所有 Tables
    for i in $(seq 0 $((table_count - 1))); do
        local table_name
        table_name=$(jq -r ".Tables[$i].Name" "$config")

        # 生成 JobName: {ProjectName}_{TableName}
        local job_name="${project_name}_${table_name}"

        echo ""
        echo "-----------------------------------------------------------------"
        log_info "[$((i+1))/$table_count] Processing: $job_name"
        echo "-----------------------------------------------------------------"

        # ---- Step 1: 检查 Job 是否已存在 ----
        if job_exists "$dataworks_project_id" "$job_name"; then
            log_info "SKIP: Job '$job_name' already exists."
            skipped=$((skipped + 1))
            continue
        fi

        # ---- Step 2: 确保数据源存在 ----
        log_info "Ensuring data sources exist ..."

        if ! ensure_oss_datasource "$dataworks_project_id" "$config"; then
            log_error "FAIL: Cannot ensure OSS datasource for '$job_name'"
            failed=$((failed + 1))
            continue
        fi

        if ! ensure_odps_datasource "$dataworks_project_id" "$config"; then
            log_error "FAIL: Cannot ensure MaxCompute datasource for '$job_name'"
            failed=$((failed + 1))
            continue
        fi

        # ---- Step 3: 生成 TaskContent 并创建同步任务 ----
        log_info "Generating task content ..."
        local task_content
        task_content=$(generate_task_content "$config" "$i")

        local file_id
        file_id=$(create_sync_task "$dataworks_project_id" "$job_name" "$task_content" "$config")

        if [ -z "$file_id" ] || [ "$file_id" == "null" ]; then
            log_error "FAIL: Failed to create sync task '$job_name'"
            failed=$((failed + 1))
            continue
        fi

        # ---- Step 4: 提交任务 ----
        if ! submit_file "$dataworks_project_id" "$file_id"; then
            log_error "FAIL: Failed to submit task '$job_name' (FileId: $file_id)"
            failed=$((failed + 1))
            continue
        fi

        # ---- Step 5: 发布到生产 ----
        if ! deploy_file "$dataworks_project_id" "$file_id"; then
            log_error "FAIL: Failed to deploy task '$job_name' (FileId: $file_id)"
            failed=$((failed + 1))
            continue
        fi

        log_info "SUCCESS: Job '$job_name' created and deployed."
        created=$((created + 1))
    done

    # 输出统计
    echo ""
    echo "================================================================="
    log_info "Project '$project_name' Summary:"
    log_info "  Created: $created | Skipped: $skipped | Failed: $failed"
    echo "================================================================="

    # 如果有失败的任务，返回非零
    if [ "$failed" -gt 0 ]; then
        return 1
    fi
}
