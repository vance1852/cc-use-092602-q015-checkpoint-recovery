"""测绘校核可恢复分片执行的离线验收。

在同一临时数据库上依次演练：依赖解锁、租约过期接管、迟到结果拒绝、
输入清单更正导致的在途结果作废、失败重试进入人工处置、进程重启后的
状态重建，以及取消任务后证据保留。全程使用冻结时钟，不访问外部网络。
"""

from __future__ import annotations

import argparse
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .clock import FrozenClock
from .compute import run_shard
from .errors import Conflict, InvalidState
from .service import SurveyCheckService
from .storage import connect, inspect_schema


def demo_job_definition(job_id: str) -> dict[str, object]:
    return {
        "job_id": job_id,
        "title": "城东乡安置区测绘校核",
        "rule": {
            "rule_version": "survey-rules-2026-09",
            "boundary_tolerance_m": "0.05",
            "area_tolerance_percent": "0.5",
            "lease_seconds": 30,
            "max_attempts": 2,
            "retry_delay_seconds": 5,
        },
        "parcels": [
            {
                "parcel_id": "p-e1",
                "zone": "east",
                "declared_area_mu": "7.5",
                "declared_boundary": [[0, 0], [100, 0], [100, 50], [0, 50]],
                "surveyed_boundary": [[0.01, 0], [100.01, 0], [100.01, 50], [0.01, 50]],
                "source_revision": "cadastre-2026-09-01",
            },
            {
                "parcel_id": "p-e2",
                "zone": "east",
                "declared_area_mu": "14.4",
                "declared_boundary": [[200, 0], [320, 0], [320, 80], [200, 80]],
                "surveyed_boundary": [[200.02, 0.01], [320.02, 0.01], [320.02, 80.01], [200.02, 80.01]],
                "source_revision": "cadastre-2026-09-01",
            },
            {
                "parcel_id": "p-w1",
                "zone": "west",
                "declared_area_mu": "4.5",
                "declared_boundary": [[0, 100], [60, 100], [60, 150], [0, 150]],
                "surveyed_boundary": [[0, 100], [60.03, 100], [60.03, 150], [0, 150]],
                "source_revision": "cadastre-2026-09-01",
            },
            {
                "parcel_id": "p-w2",
                "zone": "west",
                "declared_area_mu": "7.2",
                "declared_boundary": [[100, 100], [180, 100], [180, 160], [100, 160]],
                "surveyed_boundary": [[100, 100], [180, 100], [180, 160], [100, 160]],
                "source_revision": "cadastre-2026-09-01",
            },
        ],
        "shards": [
            {"shard_key": "boundary:east", "kind": "boundary-compare", "zone": "east", "depends_on": []},
            {"shard_key": "boundary:west", "kind": "boundary-compare", "zone": "west", "depends_on": []},
            {"shard_key": "area:east", "kind": "area-stats", "zone": "east", "depends_on": ["boundary:east"]},
            {"shard_key": "area:west", "kind": "area-stats", "zone": "west", "depends_on": ["boundary:west"]},
            {"shard_key": "summary", "kind": "summary", "depends_on": ["area:east", "area:west"]},
        ],
    }


def _work_once(service: SurveyCheckService, worker: str, job_id: str) -> dict[str, object]:
    """工作进程标准循环：领取分片、确定性计算、提交结果。"""

    claim = service.claim_shard(worker, job_id)
    if claim is None:
        raise RuntimeError("没有可领取的分片")
    result = run_shard(claim["kind"], claim["input"])
    return service.complete_shard(worker, claim["shard_id"], claim["fencing_token"], result)


def run() -> dict[str, object]:
    clock = FrozenClock(datetime(2026, 9, 26, 8, 0, tzinfo=timezone.utc))
    evidence: dict[str, object] = {}
    with tempfile.TemporaryDirectory(prefix="survey-check-") as temporary:
        database = Path(temporary) / "survey.sqlite3"
        connection = connect(database)
        service = SurveyCheckService(connection, clock)
        service.create_user("coord-1", "县自然资源协调员", "coordinator")
        service.create_user("review-1", "测绘成果复核员", "reviewer")
        service.create_user("audit-1", "审计人员", "auditor")

        created = service.create_job("coord-1", demo_job_definition("survey-demo"))
        evidence["manifest_sha256"] = created["manifest_sha256"]
        evidence["rule_version"] = created["rule_version"]

        # 边界比对：东片区直接完成，原子解锁面积统计分片。
        first = _work_once(service, "worker-a", "survey-demo")
        evidence["first_unlocked"] = first["unlocked"]

        # 西片区租约过期被接管，迟到结果不能覆盖新持有者。
        stalled = service.claim_shard("worker-b", "survey-demo")
        clock.advance(seconds=31)
        takeover = service.claim_shard("worker-c", "survey-demo")
        evidence["takeover"] = {"shard_key": takeover["shard_key"], "fencing_token": takeover["fencing_token"]}
        late_result = run_shard(stalled["kind"], stalled["input"])
        try:
            service.complete_shard("worker-b", stalled["shard_id"], stalled["fencing_token"], late_result)
            raise RuntimeError("迟到结果不应被接受")
        except Conflict as exc:
            evidence["late_result_rejected"] = str(exc)
        result = run_shard(takeover["kind"], takeover["input"])
        service.complete_shard("worker-c", takeover["shard_id"], takeover["fencing_token"], result)

        # 面积统计（东）：失败一次后按退避重试。
        east = service.claim_shard("worker-a", "survey-demo")
        failed = service.fail_shard("worker-a", east["shard_id"], east["fencing_token"], "计算节点内存不足")
        evidence["retry_scheduled_at"] = failed["available_at"]
        clock.advance(seconds=5)

        # 面积统计（西）：领取后输入清单被更正，完成时验证不通过并重新排队。
        west = service.claim_shard("worker-a", "survey-demo")
        corrected = dict(demo_job_definition("survey-demo")["parcels"][2])
        corrected["surveyed_boundary"] = [[0, 100], [60.04, 100], [60.04, 150], [0, 150]]
        corrected["source_revision"] = "cadastre-2026-09-02"
        service.correct_parcel("coord-1", "survey-demo", "p-w1", corrected)
        stale_result = run_shard(west["kind"], west["input"])
        try:
            service.complete_shard("worker-a", west["shard_id"], west["fencing_token"], stale_result)
            raise RuntimeError("基于旧输入的结果不应被登记")
        except Conflict as exc:
            evidence["stale_result_rejected"] = str(exc)

        # 面积统计（东）：再次失败，重试次数用尽进入人工处置；复核员重置后完成。
        east_retry = service.claim_shard("worker-a", "survey-demo")
        exhausted = service.fail_shard(
            "worker-a", east_retry["shard_id"], east_retry["fencing_token"], "再次内存不足"
        )
        evidence["needs_review"] = exhausted["state"]
        service.resolve_shard("review-1", east_retry["shard_id"], "retry", "扩容后重新计算")
        _work_once(service, "worker-a", "survey-demo")

        # 面积统计（西）：按更正后的输入重新领取并完成。
        _work_once(service, "worker-a", "survey-demo")

        # 汇总分片：依赖全部就绪后解锁，完成后任务自动办结。
        summary_done = _work_once(service, "worker-a", "survey-demo")
        evidence["job_state_after_summary"] = summary_done["job_state"]
        connection.close()

        # 进程重启：仅从 SQLite 重建各分片状态并说明来源。
        reopened = connect(database)
        recovered = SurveyCheckService(reopened, clock)
        status = recovered.job_status("audit-1", "survey-demo")
        evidence["restart_status"] = {
            item["shard_key"]: {
                "effective_state": item["effective_state"],
                "basis": item["source"]["basis"],
            }
            for item in status["shards"]
        }
        evidence["restart_progress"] = status["progress"]
        audit = recovered.audit_trail("audit-1", "survey-demo")
        evidence["audit_valid"] = audit["valid"]
        evidence["audit_events"] = audit["event_count"]

        # 取消：阻止新领取，但保留已完成分片证据。
        service2 = recovered
        service2.create_job("coord-1", demo_job_definition("survey-cancel"))
        _work_once(service2, "worker-a", "survey-cancel")
        cancelled = service2.cancel_job("coord-1", "survey-cancel", "上级通知暂停本轮校核")
        evidence["cancelled"] = cancelled
        try:
            service2.claim_shard("worker-b", "survey-cancel")
            raise RuntimeError("取消后不应允许新领取")
        except InvalidState as exc:
            evidence["claim_after_cancel_rejected"] = str(exc)
        cancelled_status = service2.job_status("audit-1", "survey-cancel")
        evidence["cancelled_evidence"] = {
            "claimable": cancelled_status["claimable"],
            "notes": cancelled_status["notes"],
            "succeeded_basis": [
                item["source"]["basis"]
                for item in cancelled_status["shards"]
                if item["effective_state"] == "succeeded"
            ],
        }
        schema = inspect_schema(reopened)
        reopened.close()
    if schema["missing_tables"] or schema["schema_version"] != "1":
        raise RuntimeError("SQLite 基础结构检查失败")
    return {
        "status": "ok",
        "schema": schema,
        **evidence,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="执行测绘校核可恢复分片执行的离线自检")
    parser.parse_args(argv)
    result = run()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
