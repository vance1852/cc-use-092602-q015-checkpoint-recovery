"""测绘校核分片执行的离线验收：覆盖重启恢复、租约接管、重试与取消。"""

from __future__ import annotations

import argparse
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .clock import FrozenClock
from .errors import Conflict
from .jsonio import load_json
from .service import SurveyCheckoutService
from .storage import connect, inspect_schema


def _drain_ready(service: SurveyCheckoutService, worker_id: str, job_id: str) -> list[str]:
    """领取并完成作业内当前全部可执行分片，返回完成顺序。"""

    finished: list[str] = []
    while True:
        claim = service.claim_shard(worker_id, lease_seconds=300, job_id=job_id)
        if claim is None:
            return finished
        service.complete_shard(
            worker_id, claim["job_id"], claim["shard_id"],
            claim["input_sha256"], claim["rule_set_sha256"],
        )
        finished.append(claim["shard_id"])


def run(workspace: Path) -> dict[str, object]:
    fixtures = workspace / "fixtures"
    rule_set = load_json(fixtures / "demo_survey_rules.json")
    manifest = load_json(fixtures / "demo_survey_manifest.json")
    clock = FrozenClock(datetime(2026, 9, 26, 8, 0, tzinfo=timezone.utc))
    with tempfile.TemporaryDirectory(prefix="survey-checkout-") as temporary:
        database = Path(temporary) / "survey.sqlite3"
        connection = connect(database)
        service = SurveyCheckoutService(connection, clock)
        service.create_user("planner-1", "自然资源所经办", "planner")
        service.create_user("reviewer-1", "测绘复核员", "reviewer")
        service.create_user("auditor-1", "审计人员", "auditor")
        rules = service.publish_rule_set("planner-1", rule_set)

        # 1. 依赖链：summary 分片等待两个地块分片，完成后被原子解锁
        service.create_job("planner-1", "job-demo", manifest)
        initial = service.job_status("auditor-1", "job-demo")
        summary_waiting = next(s for s in initial["shards"] if s["shard_id"] == "village-summary")
        first = service.claim_shard("worker-a", 300, "job-demo")
        service.complete_shard("worker-a", "job-demo", first["shard_id"], first["input_sha256"], first["rule_set_sha256"])
        second = service.claim_shard("worker-a", 300, "job-demo")
        unlocked = service.complete_shard(
            "worker-a", "job-demo", second["shard_id"], second["input_sha256"], second["rule_set_sha256"]
        )
        connection.close()

        # 2. 模拟服务重启：新连接、新服务实例，状态全部由 SQLite 重建
        connection = connect(database)
        service = SurveyCheckoutService(connection, clock)
        rebuilt = service.job_status("auditor-1", "job-demo")
        rebuilt_sources = {s["shard_id"]: s["state_source"] for s in rebuilt["shards"]}
        finished = _drain_ready(service, "worker-b", "job-demo")
        final = service.job_status("auditor-1", "job-demo")

        # 3. 租约过期被接管，迟到结果不能覆盖新持有者
        service.create_job("planner-1", "job-lease", manifest)
        stale = service.claim_shard("worker-a", 30, "job-lease")
        clock.advance(seconds=31)
        takeover = service.claim_shard("worker-b", 30, "job-lease")
        late_result_rejected = False
        try:
            service.complete_shard(
                "worker-a", "job-lease", stale["shard_id"], stale["input_sha256"], stale["rule_set_sha256"]
            )
        except Conflict:
            late_result_rejected = True
        service.complete_shard(
            "worker-b", "job-lease", takeover["shard_id"], takeover["input_sha256"], takeover["rule_set_sha256"]
        )

        # 4. 失败按策略重试，次数用尽进入人工处置，人工决定后重新入队
        service.create_job("planner-1", "job-retry", manifest)
        attempt = service.claim_shard("worker-a", 300, "job-retry")
        service.fail_shard("worker-a", "job-retry", attempt["shard_id"], "全站仪断电")
        attempt = service.claim_shard("worker-a", 300, "job-retry")
        exhausted = service.fail_shard("worker-a", "job-retry", attempt["shard_id"], "数据存储卡损坏")
        service.manual_decision("reviewer-1", "job-retry", attempt["shard_id"], "retry", "更换设备后重测")
        recovered = service.claim_shard("worker-c", 300, "job-retry")
        service.complete_shard(
            "worker-c", "job-retry", recovered["shard_id"], recovered["input_sha256"], recovered["rule_set_sha256"]
        )

        # 5. 取消：阻止新领取，已完成成果与审计证据保留
        service.cancel_job("planner-1", "job-retry", "测区规划调整，暂停校核")
        claim_after_cancel = service.claim_shard("worker-a", 300, "job-retry")
        cancelled = service.job_status("auditor-1", "job-retry")
        audit = service.audit_trail("auditor-1", "job-retry")
        schema = inspect_schema(connection)
        connection.close()

    if schema["missing_tables"] or schema["schema_version"] != "1":
        raise RuntimeError("SQLite 基础结构检查失败")
    if summary_waiting["state"] != "waiting" or summary_waiting["blocked_by"] != ["north-parcels", "south-parcels"]:
        raise RuntimeError("依赖分片未按预期阻塞")
    if unlocked["unlocked_shards"] != ["village-summary"]:
        raise RuntimeError("后续分片未被原子解锁")
    if rebuilt_sources["village-summary"] != "dependency_unlocked":
        raise RuntimeError("重启后未能重建依赖解锁状态")
    if not late_result_rejected or not takeover["lease"]["takeover"]:
        raise RuntimeError("迟到结果未被拒绝或租约未被接管")
    if exhausted["state"] != "manual" or claim_after_cancel is not None:
        raise RuntimeError("重试转人工或取消阻止领取未生效")
    if cancelled["counts"]["succeeded"] != 1:
        raise RuntimeError("取消后已完成成果未保留")
    return {
        "status": "ok",
        "rule_set": f"{rules['rule_set_id']}@{rules['version']}",
        "manifest_sha256": final["job"]["manifest_sha256"],
        "job_demo": {
            "state": final["job"]["state"],
            "counts": final["counts"],
            "state_sources": {s["shard_id"]: s["state_source"] for s in final["shards"]},
            "finished_after_restart": finished,
        },
        "lease": {
            "takeover": takeover["lease"]["takeover"],
            "epoch": takeover["lease"]["epoch"],
            "late_result_rejected": late_result_rejected,
        },
        "retry": {"exhausted_state": exhausted["state"], "recovered_shard": recovered["shard_id"]},
        "cancelled_job": {
            "state": cancelled["job"]["state"],
            "counts": cancelled["counts"],
            "claim_after_cancel": claim_after_cancel,
            "audit_events": len(audit["events"]),
        },
        "schema": schema,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="执行测绘校核分片执行的离线自检")
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    result = run(args.workspace.resolve())
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
