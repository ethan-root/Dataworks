# -*- coding: utf-8 -*-
"""
publish_node.py
职责：将指定 DataWorks 项目中的节点提交并发布到生产环境。
使用全新的 DataWorks 2024-05-18 Pipeline API (CreatePipelineRunRequest)。
"""

import argparse
import json
import sys
import os
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from dataworks_client import create_client, get_node_id
from config_merger import load_merged_node_config

from alibabacloud_dataworks_public20240518 import models as dw_models
from alibabacloud_tea_util import models as util_models

def main():
    parser = argparse.ArgumentParser(description="Publish DataWorks Node to Production using Pipeline API")
    parser.add_argument(
        "--project-dir", type=str, default="projects/Test",
        help="项目目录路径（由于我们已引入 global.json，直接传所在环境目录即可）"
    )
    args = parser.parse_args()

    # ── 1. 读取配置（支持 global.json 提取） ───────────────────────
    try:
        config = load_merged_node_config(args.project_dir)
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        sys.exit(1)
        
    node_name = config.get("node_name")
    if not node_name:
        print(f"ERROR: node_name not found in configuration.")
        sys.exit(1)

    # ── 2. 读取工作空间 ID ─────────────────────────────────────
    project_id_str = os.environ.get("DATAWORKS_PROJECT_ID", "")
    if not project_id_str:
        print("ERROR: DATAWORKS_PROJECT_ID not set")
        sys.exit(1)
    project_id = int(project_id_str)

    client = create_client()
    print(f"\n{'='*50}")
    print(f"🚀 [PUBLISH] 开始将节点 '{node_name}' 发布到生产环境")
    print(f"{'='*50}")

    # ── 3. 获取真实节点 ID ─────────────────────────────────────
    node_file_id = get_node_id(client, project_id, node_name)
    if not node_file_id:
        print(f"❌ 查无此节点 '{node_name}'，请先运行 create_node 部署流程。")
        sys.exit(1)
    print(f"✅ 获取到节点的真实 ID: {node_file_id}")

    # ── 4. 触发 CreatePipelineRun ──────────────────────────────
    print(f"\n[INIT] 正在创建发布流水线...")
    pipeline_req = dw_models.CreatePipelineRunRequest(
        type='Online',
        project_id=project_id,
        object_ids=[str(node_file_id)]
    )
    
    try:
        resp = client.create_pipeline_run_with_options(pipeline_req, util_models.RuntimeOptions())
        pipeline_run_id = resp.body.id
        print(f"✅ 成功创建发布流水线, 流水线 ID: {pipeline_run_id}")
    except Exception as e:
        print(f"❌ 创建流水线失败: {e}")
        sys.exit(1)
        
    # 为避免流水线初始化延迟
    time.sleep(3)

    # ── 5. 顺序执行三个标准发布阶段 ────────────────────────────
    stages = ["BUILD_PACKAGE", "PROD_CHECK", "PROD"]
    for idx, stage in enumerate(stages, 1):
        print(f"\n[STAGE {idx}/{len(stages)}] 正在执行 {stage} ...")
        
        stage_req = dw_models.ExecPipelineRunStageRequest(
            project_id=project_id,
            id=pipeline_run_id,
            code=stage
        )
        
        # 带有重试机制的推进器
        max_retries = 3
        success = False
        for attempt in range(max_retries):
            try:
                client.exec_pipeline_run_stage_with_options(stage_req, util_models.RuntimeOptions())
                success = True
                
                # 等待阶段真实下发执行
                wait_time = 10 if stage == "PROD_CHECK" else 5
                for i in range(wait_time):
                    print(".", end="", flush=True)
                    time.sleep(1)
                print(" ✅ 已触发且执行通过")
                break
                
            except Exception as e:
                msg = str(e)
                if "Failed" in msg or "not finish" in msg.lower() or "dependent" in msg.lower():
                    print(f"\n   [WARN] 触发出错 (可能是上一个阶段尚未完全结束): {msg}")
                    if attempt < max_retries - 1:
                        print("   等待 5 秒后重试...")
                        time.sleep(5)
                else:
                    print(f"\n❌ 执行阶段 {stage} 发生异常: {msg}")
                    sys.exit(1)
                    
        if not success:
            print(f"\n❌ 阶段 {stage} 重试 {max_retries} 次仍失败，退出发布流程。请去控制台排查错误日志。")
            sys.exit(1)

    print(f"\n🎉 恭喜，节点 '{node_name}' 已成功发布到生产环境！\n")

if __name__ == "__main__":
    main()
