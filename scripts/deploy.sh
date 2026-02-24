#!/bin/bash
# ==============================================================================
# deploy.sh — 主部署入口
# 遍历 projects/ 下所有品牌项目，逐一处理
# 用法:
#   bash scripts/deploy.sh                     # 处理所有项目
#   bash scripts/deploy.sh Gucci               # 仅处理 Gucci 项目
#   bash scripts/deploy.sh Gucci,Balenciaga    # 处理多个指定项目
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECTS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)/projects"

source "$SCRIPT_DIR/process_project.sh"

# ---- 读取参数 ----
TARGET_PROJECTS="${1:-}"
DATAWORKS_PROJECT_ID="${DATAWORKS_PROJECT_ID:?'Error: DATAWORKS_PROJECT_ID environment variable is required'}"

echo ""
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║        DataWorks Data Integration — Deployment Tool          ║"
echo "╠═══════════════════════════════════════════════════════════════╣"
echo "║  DataWorks Project ID: $DATAWORKS_PROJECT_ID"
echo "║  Projects Dir:        $PROJECTS_DIR"
echo "║  Target Projects:     ${TARGET_PROJECTS:-ALL}"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

# ---- 构建项目列表 ----
PROJECT_DIRS=()

if [ -n "$TARGET_PROJECTS" ]; then
    # 按逗号分割指定的项目
    IFS=',' read -ra PROJECT_NAMES <<< "$TARGET_PROJECTS"
    for name in "${PROJECT_NAMES[@]}"; do
        name=$(echo "$name" | xargs)  # trim spaces
        dir="$PROJECTS_DIR/$name"
        if [ -d "$dir" ]; then
            PROJECT_DIRS+=("$dir")
        else
            log_error "Project directory not found: $dir"
            exit 1
        fi
    done
else
    # 遍历所有项目目录
    for dir in "$PROJECTS_DIR"/*/; do
        if [ -f "$dir/config.json" ]; then
            PROJECT_DIRS+=("$dir")
        else
            log_warn "Skipping $dir — no config.json found"
        fi
    done
fi

if [ ${#PROJECT_DIRS[@]} -eq 0 ]; then
    log_error "No project directories found in $PROJECTS_DIR"
    exit 1
fi

log_info "Found ${#PROJECT_DIRS[@]} project(s) to process."

# ---- 遍历处理每个项目 ----
TOTAL_SUCCESS=0
TOTAL_FAILED=0

for project_dir in "${PROJECT_DIRS[@]}"; do
    if process_project "$project_dir" "$DATAWORKS_PROJECT_ID"; then
        TOTAL_SUCCESS=$((TOTAL_SUCCESS + 1))
    else
        TOTAL_FAILED=$((TOTAL_FAILED + 1))
    fi
done

# ---- 最终汇总 ----
echo ""
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║                    Deployment Complete                       ║"
echo "╠═══════════════════════════════════════════════════════════════╣"
echo "║  Projects Succeeded: $TOTAL_SUCCESS"
echo "║  Projects Failed:    $TOTAL_FAILED"
echo "╚═══════════════════════════════════════════════════════════════╝"

if [ "$TOTAL_FAILED" -gt 0 ]; then
    log_error "Some projects failed. Please check the logs above."
    exit 1
fi

log_info "All projects deployed successfully!"
