# -*- coding: utf-8 -*-
"""
ci_runner.py — DataWorks 部署编排脚本（CI/CD 流程核心）

职责：
  接收 feature 列表和目标环境，按序执行「检查 → 创建/更新 → 发布」
  完整流程。每个具体操作委托给 scripts/ 下对应的 Python 脚本执行。

设计原则：
  本脚本是「编排层」，不直接调用 DataWorks SDK，只负责串联各子脚本。
  各子脚本保持独立可执行（仍可单独 CLI 调用），ci_runner 通过 subprocess
  调用它们，确保彼此解耦。

本地调试：
  python scripts/ci_runner.py --feature-list test-feature --env dev

使用方式（GitHub Actions 中）：
  python scripts/ci_runner.py \\
    --feature-list "$FEATURE_LIST" \\
    --env "$ENV_NAME"
"""

import argparse
import subprocess
import sys
from pathlib import Path


# ─────────────────────────────────────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────────────────────────────────────

def _banner(feature: str, env: str) -> None:
    """打印 feature 部署开始的日志横幅。"""
    line = "═" * 62
    print(f"\n╔{line}╗")
    print(f"║  🚀 DataWorks Deploy   feature: {feature:<20} env: {env:<8}║")
    print(f"╚{line}╝")


def _log_step(step: int, total: int, desc: str) -> None:
    print(f"  ├─ [{step}/{total}] {desc}")


def _log_done() -> None:
    print("  └─ ✅ done")


def _run(script: str, args: list[str], *, check: bool = True) -> int:
    """
    执行 scripts/<script> 并传入参数。
    check=True  → 非零返回码时直接 sys.exit 中断流水线
    check=False → 仅返回返回码，不抛出（用于 'check_*' 探测类脚本）
    """
    cmd = ["python", f"scripts/{script}"] + args
    result = subprocess.run(cmd, check=False)
    if check and result.returncode != 0:
        print(f"  └─ ❌ 脚本 {script} 执行失败（退出码 {result.returncode}）")
        sys.exit(result.returncode)
    return result.returncode


def _exists(script: str, args: list[str]) -> bool:
    """调用 check_* 脚本，返回 True 表示「已存在」（exit 0）。"""
    return _run(script, args, check=False) == 0


# ─────────────────────────────────────────────────────────────────────────────
# 核心：单 feature 全量部署
# ─────────────────────────────────────────────────────────────────────────────

def deploy_feature(feature_name: str, env: str) -> None:
    """
    对一个 feature 执行完整的 create-or-update 部署流程。
    """
    project_dir = f"features/{feature_name}"
    setting_file = f"{project_dir}/setting-{env}.json"

    # ── 前置校验 ──────────────────────────────────────────────────────
    if not Path(project_dir).is_dir():
        print(f"  ⚠️  目录不存在: {project_dir}，跳过此 feature。")
        return
    if not Path(setting_file).is_file():
        print(f"  ⚠️  缺少环境配置文件: {setting_file}，跳过此 feature。")
        return

    common_args = ["--project-dir", project_dir, "--env", env]

    # ── 步骤 0：检查集成节点是否已存在 ──────────────────────────────
    print("\n  ┌─ 判断节点是否已存在...")
    node_exists = _exists("check_integration_node.py", common_args)

    if node_exists:
        # ━━━ UPDATE 分支 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        print("  ┌─ [UPDATE WORKFLOW] 节点已存在，执行全部节点的更新流程")
        
        _log_step(1, 4, "🔄 更新目标表 DDL (新增字段等)")
        _run("create_table.py", common_args)
        
        _log_step(2, 4, "🔄 更新上游节点")
        _run("create_upstream_node.py", common_args)
        
        _log_step(3, 4, "🔄 更新数据集成节点")
        _run("update_integration_node.py", common_args)
        
        _log_step(4, 4, "🔄 更新下游节点")
        _run("create_downstream_node.py", common_args)
        _log_done()

    else:
        # ━━━ CREATE 分支 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        print("  ┌─ [CREATION WORKFLOW] 节点不存在，执行完整创建流程")

        # 3.1.1 检查/创建 OSS 数据源
        _log_step(1, 6, "☁️  确保 OSS 数据源存在")
        if not _exists("check_oss_ds.py", common_args):
            _run("create_oss_ds.py", common_args)
        _log_done()

        # 3.1.2 检查/创建 MaxCompute 数据源
        _log_step(2, 6, "🗄️  确保 MaxCompute 数据源存在")
        if not _exists("check_mc_ds.py", common_args):
            _run("create_mc_ds.py", common_args)
        _log_done()

        # 3.1.3 创建 MaxCompute 目标表（DDL Migration）
        _log_step(3, 6, "📄 创建 MaxCompute 目标表（使用新版迁移逻辑）")
        _run("create_table.py", common_args)
        _log_done()

        # 3.1.4 创建上游节点（获取最早 parquet 文件名并注入 CI/CD 环境）
        _log_step(4, 6, "🐍 创建上游节点（获取最早 parquet 文件名）")
        _run("create_upstream_node.py", common_args)
        _log_done()

        # 3.1.5 创建数据集成节点
        _log_step(5, 6, "🔗 创建数据集成节点（注入 parquet 文件名）")
        _run("create_integration_node.py", common_args)
        _log_done()

        # 3.1.6 创建下游节点 (Parquet 移动到 completed)
        _log_step(6, 6, "📤 创建下游节点（移动 parquet 文件）")
        _run("create_downstream_node.py", common_args)
        _log_done()

    # ── 最终：发布节点（创建和更新分支均需执行）──────────────────────
    print("\n  ┌─ 🚀 发布节点...")
    # 3.1.7 / 3.2.3 发布节点
    _run("publish_node.py", common_args)
    _log_done()

    print(f"\n  ✅ feature [{feature_name}] 在 [{env}] 环境部署完成。")
    print("─" * 66)


# ─────────────────────────────────────────────────────────────────────────────
# 入口
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="DataWorks CI/CD 部署编排脚本"
    )
    parser.add_argument(
        "--feature-list",
        required=True,
        help="逗号分隔的 feature 目录名称列表，例如: test-feature,user-feature",
    )
    parser.add_argument(
        "--env",
        required=True,
        help="目标环境名称，例如: dev / qa / preprod / prod",
    )
    args = parser.parse_args()

    features = [f.strip() for f in args.feature_list.split(",") if f.strip()]
    if not features:
        print("⚠️  feature-list 为空，无需部署。")
        sys.exit(0)

    print(f"\n🎯 开始部署 {len(features)} 个 feature → 环境: {args.env}")
    print(f"   features: {', '.join(features)}")

    for feature in features:
        _banner(feature, args.env)
        deploy_feature(feature, args.env)

    print(f"\n🎉 所有 feature 在 [{args.env}] 环境部署完成！")


if __name__ == "__main__":
    main()
