from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from survey_checkout.api import JsonApplication
from survey_checkout.checks import ALGORITHM_VERSION, boundary_offset, evaluate_shard, ring_area
from survey_checkout.clock import FrozenClock
from survey_checkout.contracts import parse_manifest
from survey_checkout.errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from survey_checkout.jsonio import canonical_json
from survey_checkout.service import SurveyCheckoutService
from survey_checkout.storage import connect


RULE_SET = {
    "rule_set_id": "rs",
    "version": 1,
    "title": "测绘校核规则",
    "area_tolerance_m2": "1.0",
    "area_tolerance_ratio": "0.01",
    "boundary_offset_tolerance_m": "0.5",
}

SQUARE = [[0, 0], [10, 0], [10, 10], [0, 10]]


def _parcel(parcel_id: str, ring=None, cadastral=None) -> dict:
    return {
        "parcel_id": parcel_id,
        "surveyed_ring": ring if ring is not None else SQUARE,
        "cadastral_ring": cadastral if cadastral is not None else SQUARE,
    }


def make_manifest(**overrides) -> dict:
    manifest = {
        "rule_set_id": "rs",
        "rule_set_version": 1,
        "retry_policy": {"max_attempts": 2, "backoff_seconds": [0, 0]},
        "shards": [
            {"shard_id": "a", "parcels": [_parcel("A-1")]},
            {"shard_id": "b", "parcels": [_parcel("B-1")]},
            {"shard_id": "c", "depends_on": ["a", "b"], "parcels": [_parcel("C-1")]},
        ],
    }
    manifest.update(overrides)
    return manifest


class ServiceTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 26, 8, 0, tzinfo=timezone.utc))
        self.service = SurveyCheckoutService(self.connection, self.clock)
        self.service.create_user("planner", "经办", "planner")
        self.service.create_user("reviewer", "复核", "reviewer")
        self.service.create_user("auditor", "审计", "auditor")
        self.rules = self.service.publish_rule_set("planner", RULE_SET)

    def tearDown(self) -> None:
        self.connection.close()

    def _create_job(self, job_id: str = "job-1", **overrides) -> dict:
        return self.service.create_job("planner", job_id, make_manifest(**overrides))

    def _claim(self, worker: str = "w1", job_id: str = "job-1", lease_seconds: int = 60) -> dict:
        claim = self.service.claim_shard(worker, lease_seconds, job_id)
        self.assertIsNotNone(claim)
        return claim

    def _complete(self, claim: dict, worker: str = "w1") -> dict:
        return self.service.complete_shard(
            worker, claim["job_id"], claim["shard_id"], claim["input_sha256"], claim["rule_set_sha256"]
        )

    def _shard(self, status: dict, shard_id: str) -> dict:
        return next(item for item in status["shards"] if item["shard_id"] == shard_id)


class DependencyChainTests(ServiceTestBase):
    def test_dependency_chain_and_atomic_unlock(self) -> None:
        created = self._create_job()
        self.assertEqual(created["initial_ready"], ["a", "b"])
        self.assertEqual(created["waiting"], ["c"])
        self.assertEqual(len(created["manifest_sha256"]), 64)

        status = self.service.job_status("auditor", "job-1")
        shard_c = self._shard(status, "c")
        self.assertEqual(shard_c["state"], "waiting")
        self.assertEqual(shard_c["state_source"], "dependency_pending")
        self.assertEqual(shard_c["blocked_by"], ["a", "b"])

        claim_a = self._claim()
        self.assertEqual(claim_a["shard_id"], "a")
        result = self._complete(claim_a)
        self.assertEqual(result["unlocked_shards"], [])
        self.assertEqual(result["verdict"], "pass")
        status = self.service.job_status("auditor", "job-1")
        self.assertEqual(self._shard(status, "c")["blocked_by"], ["b"])

        claim_b = self._claim()
        result = self._complete(claim_b)
        self.assertEqual(result["unlocked_shards"], ["c"])
        status = self.service.job_status("auditor", "job-1")
        shard_c = self._shard(status, "c")
        self.assertEqual(shard_c["state"], "ready")
        self.assertEqual(shard_c["state_source"], "dependency_unlocked")

        claim_c = self._claim()
        result = self._complete(claim_c)
        self.assertEqual(result["job_state"], "completed")
        job = self.service.get_job("job-1")
        self.assertEqual(job["state"], "completed")
        self.assertIsNotNone(job["completed_at"])
        status = self.service.job_status("auditor", "job-1")
        self.assertEqual(status["counts"]["succeeded"], 3)
        self.assertIsNone(self.service.claim_shard("w1", 60, "job-1"))

    def test_output_checksum_recorded(self) -> None:
        self._create_job()
        claim = self._claim()
        result = self._complete(claim)
        self.assertEqual(len(result["output_sha256"]), 64)
        status = self.service.job_status("auditor", "job-1")
        shard_a = self._shard(status, "a")
        self.assertEqual(shard_a["state_source"], "output_recorded")
        self.assertEqual(shard_a["output"]["sha256"], result["output_sha256"])
        self.assertEqual(shard_a["output"]["completed_by"], "w1")


class RestartRebuildTests(unittest.TestCase):
    def test_restart_rebuilds_status_with_sources(self) -> None:
        clock = FrozenClock(datetime(2026, 9, 26, 8, 0, tzinfo=timezone.utc))
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "survey.sqlite3"
            connection = connect(database)
            service = SurveyCheckoutService(connection, clock)
            service.create_user("planner", "经办", "planner")
            service.create_user("auditor", "审计", "auditor")
            service.publish_rule_set("planner", RULE_SET)
            service.create_job("planner", "job-1", make_manifest())
            claim_a = service.claim_shard("w1", 60, "job-1")
            service.complete_shard("w1", "job-1", "a", claim_a["input_sha256"], claim_a["rule_set_sha256"])
            service.claim_shard("w2", 60, "job-1")  # b 被租出但未完成
            connection.close()

            # 模拟重启：新连接、新服务实例，状态完全由 SQLite 重建
            connection = connect(database)
            service = SurveyCheckoutService(connection, clock)
            status = service.job_status("auditor", "job-1")
            self.assertEqual(status["rebuilt_from"], "sqlite")
            by_id = {item["shard_id"]: item for item in status["shards"]}
            self.assertEqual(by_id["a"]["state"], "succeeded")
            self.assertEqual(by_id["a"]["state_source"], "output_recorded")
            self.assertEqual(by_id["b"]["state"], "leased")
            self.assertEqual(by_id["b"]["state_source"], "lease_active")
            self.assertEqual(by_id["b"]["lease"]["owner"], "w2")
            self.assertEqual(by_id["c"]["state"], "waiting")
            self.assertEqual(by_id["c"]["blocked_by"], ["b"])

            # 租约过期后重启查询把分片重建为可领取，并说明来源
            clock.advance(seconds=61)
            status = service.job_status("auditor", "job-1")
            by_id = {item["shard_id"]: item for item in status["shards"]}
            self.assertEqual(by_id["b"]["state"], "ready")
            self.assertEqual(by_id["b"]["state_source"], "lease_expired")
            takeover = service.claim_shard("w3", 60, "job-1")
            self.assertEqual(takeover["shard_id"], "b")
            self.assertTrue(takeover["lease"]["takeover"])
            self.assertEqual(takeover["lease"]["epoch"], 2)
            connection.close()


class LeaseFencingTests(ServiceTestBase):
    def test_lease_takeover_and_late_result_rejected(self) -> None:
        self._create_job()
        stale = self._claim("w1", lease_seconds=30)
        self.clock.advance(seconds=31)
        takeover = self._claim("w2", lease_seconds=30)
        self.assertEqual(takeover["shard_id"], stale["shard_id"])
        self.assertTrue(takeover["lease"]["takeover"])
        self.assertEqual(takeover["lease"]["epoch"], 2)

        with self.assertRaises(Conflict):
            self._complete(stale, "w1")
        with self.assertRaises(Conflict):
            self.service.fail_shard("w1", "job-1", stale["shard_id"], "迟到失败")
        result = self._complete(takeover, "w2")
        self.assertEqual(result["state"], "succeeded")
        status = self.service.job_status("auditor", "job-1")
        self.assertEqual(self._shard(status, "a")["output"]["completed_by"], "w2")

    def test_active_lease_blocks_other_claimants(self) -> None:
        self._create_job()
        self._claim("w1", lease_seconds=60)
        claim = self.service.claim_shard("w2", 60, "job-1")
        self.assertEqual(claim["shard_id"], "b")
        with self.assertRaises(Conflict):
            self.service.complete_shard("w2", "job-1", "a", "0" * 64, "0" * 64)

    def test_complete_rejects_digest_mismatch(self) -> None:
        self._create_job()
        claim = self._claim()
        with self.assertRaises(Conflict):
            self.service.complete_shard("w1", "job-1", "a", "0" * 64, claim["rule_set_sha256"])
        with self.assertRaises(Conflict):
            self.service.complete_shard("w1", "job-1", "a", claim["input_sha256"], "1" * 64)
        # 校验失败不消耗租约，正确摘要仍可登记
        result = self._complete(claim)
        self.assertEqual(result["state"], "succeeded")

    def test_expired_lease_cannot_complete(self) -> None:
        self._create_job()
        claim = self._claim("w1", lease_seconds=30)
        self.clock.advance(seconds=31)
        with self.assertRaises(Conflict):
            self._complete(claim)


class RetryAndManualTests(ServiceTestBase):
    def test_fail_retries_then_manual_then_retry_decision(self) -> None:
        self._create_job()
        claim = self._claim()
        failed = self.service.fail_shard("w1", "job-1", "a", "全站仪断电")
        self.assertEqual(failed["state"], "ready")
        self.assertEqual(failed["attempts"], 1)
        status = self.service.job_status("auditor", "job-1")
        self.assertEqual(self._shard(status, "a")["last_error"], "全站仪断电")

        claim = self._claim()
        self.assertEqual(claim["attempt"], 2)
        exhausted = self.service.fail_shard("w1", "job-1", "a", "存储卡损坏")
        self.assertEqual(exhausted["state"], "manual")
        status = self.service.job_status("auditor", "job-1")
        shard_a = self._shard(status, "a")
        self.assertEqual(shard_a["state"], "manual")
        self.assertEqual(shard_a["state_source"], "retry_exhausted")

        # 人工决定重新入队：次数清零，可再次领取并完成
        decided = self.service.manual_decision("reviewer", "job-1", "a", "retry", "更换设备后重测")
        self.assertEqual(decided["state"], "ready")
        claim = self._claim("w9")
        self.assertEqual(claim["shard_id"], "a")
        self.assertEqual(claim["attempt"], 1)
        self._complete(claim, "w9")
        status = self.service.job_status("auditor", "job-1")
        shard_a = self._shard(status, "a")
        self.assertEqual(shard_a["state"], "succeeded")
        self.assertEqual(shard_a["manual"]["by"], "reviewer")

    def test_manual_skip_blocks_dependents(self) -> None:
        self._create_job()
        self._claim()
        self.service.fail_shard("w1", "job-1", "a", "错误一")
        self._claim()
        exhausted = self.service.fail_shard("w1", "job-1", "a", "错误二")
        self.assertEqual(exhausted["state"], "manual")
        decided = self.service.manual_decision("reviewer", "job-1", "a", "skip", "界址争议，转实地裁决")
        self.assertEqual(decided["state"], "skipped")

        claim_b = self._claim()
        result = self._complete(claim_b)
        self.assertEqual(result["unlocked_shards"], [])
        status = self.service.job_status("auditor", "job-1")
        shard_c = self._shard(status, "c")
        self.assertEqual(shard_c["state"], "waiting")
        self.assertEqual(shard_c["blocked_by"], ["a"])
        self.assertIsNone(self.service.claim_shard("w1", 60, "job-1"))
        self.assertEqual(self.service.get_job("job-1")["state"], "active")

    def test_lease_expiry_exhaustion_enters_manual(self) -> None:
        self._create_job()
        self._claim("w1", lease_seconds=30)
        self.clock.advance(seconds=31)
        self._claim("w2", lease_seconds=30)
        self.clock.advance(seconds=31)
        claim = self.service.claim_shard("w3", 60, "job-1")
        self.assertEqual(claim["shard_id"], "b")  # a 已转人工，不再发放
        status = self.service.job_status("auditor", "job-1")
        shard_a = self._shard(status, "a")
        self.assertEqual(shard_a["state"], "manual")
        self.assertEqual(shard_a["state_source"], "retry_exhausted")

    def test_manual_decision_requires_manual_state(self) -> None:
        self._create_job()
        with self.assertRaises(InvalidState):
            self.service.manual_decision("reviewer", "job-1", "a", "retry")
        with self.assertRaises(ValidationFailed):
            self.service.manual_decision("reviewer", "job-1", "a", "approve")


class CancelTests(ServiceTestBase):
    def test_cancel_blocks_claims_but_preserves_evidence(self) -> None:
        self._create_job()
        claim = self._claim()
        result = self._complete(claim)
        cancelled = self.service.cancel_job("planner", "job-1", "测区规划调整")
        self.assertEqual(cancelled["state"], "cancelled")
        self.assertEqual(cancelled["cancelled_by"], "planner")

        self.assertIsNone(self.service.claim_shard("w2", 60, "job-1"))
        status = self.service.job_status("auditor", "job-1")
        shard_a = self._shard(status, "a")
        self.assertEqual(shard_a["state"], "succeeded")
        self.assertEqual(shard_a["output"]["sha256"], result["output_sha256"])
        self.assertEqual(self._shard(status, "b")["state"], "ready")
        self.assertEqual(self._shard(status, "c")["state"], "waiting")
        with self.assertRaises(InvalidState):
            self.service.cancel_job("planner", "job-1", "重复取消")
        audit = self.service.audit_trail("auditor", "job-1")
        event_types = [event["event_type"] for event in audit["events"]]
        self.assertIn("job.cancelled", event_types)
        self.assertIn("shard.succeeded", event_types)

    def test_inflight_completion_after_cancel_kept_but_unlocks_nothing(self) -> None:
        self._create_job()
        claim = self._claim("w1", lease_seconds=300)
        self.service.cancel_job("planner", "job-1", "窗口关闭")
        result = self._complete(claim)
        self.assertEqual(result["state"], "succeeded")
        self.assertEqual(result["unlocked_shards"], [])
        self.assertEqual(result["job_state"], "cancelled")
        status = self.service.job_status("auditor", "job-1")
        self.assertEqual(self._shard(status, "a")["state"], "succeeded")
        self.assertEqual(self._shard(status, "b")["state"], "ready")
        self.assertIsNone(self.service.claim_shard("w2", 60, "job-1"))
        self.assertEqual(self.service.get_job("job-1")["state"], "cancelled")


class ValidationTests(ServiceTestBase):
    def test_manifest_rejects_unknown_dependency(self) -> None:
        manifest = make_manifest()
        manifest["shards"][2] = {"shard_id": "c", "depends_on": ["ghost"], "parcels": [_parcel("C-1")]}
        with self.assertRaises(ValidationFailed):
            self.service.create_job("planner", "job-bad", manifest)

    def test_manifest_rejects_dependency_cycle(self) -> None:
        manifest = make_manifest()
        manifest["shards"] = [
            {"shard_id": "a", "depends_on": ["b"], "parcels": [_parcel("A-1")]},
            {"shard_id": "b", "depends_on": ["a"], "parcels": [_parcel("B-1")]},
        ]
        with self.assertRaises(ValidationFailed):
            parse_manifest(manifest)

    def test_manifest_rejects_duplicate_shard_and_self_dependency(self) -> None:
        manifest = make_manifest()
        manifest["shards"] = [
            {"shard_id": "a", "parcels": [_parcel("A-1")]},
            {"shard_id": "a", "parcels": [_parcel("A-2")]},
        ]
        with self.assertRaises(ValidationFailed):
            parse_manifest(manifest)
        manifest["shards"] = [{"shard_id": "a", "depends_on": ["a"], "parcels": [_parcel("A-1")]}]
        with self.assertRaises(ValidationFailed):
            parse_manifest(manifest)

    def test_manifest_rejects_invalid_ring_and_retry_policy(self) -> None:
        manifest = make_manifest()
        manifest["shards"] = [{"shard_id": "a", "parcels": [_parcel("A-1", ring=[[0, 0], [1, 1]])]}]
        with self.assertRaises(ValidationFailed):
            parse_manifest(manifest)
        manifest = make_manifest(retry_policy={"max_attempts": 0, "backoff_seconds": [0]})
        with self.assertRaises(ValidationFailed):
            parse_manifest(manifest)

    def test_permissions(self) -> None:
        with self.assertRaises(Forbidden):
            self.service.create_job("auditor", "job-x", make_manifest())
        with self.assertRaises(Forbidden):
            self.service.publish_rule_set("reviewer", RULE_SET)
        with self.assertRaises(Forbidden):
            self.service.cancel_job("reviewer", "job-x", "无权限")
        with self.assertRaises(NotFound):
            self.service.job_status("ghost", "job-x")
        with self.assertRaises(NotFound):
            self.service.job_status("auditor", "job-x")


class ConcurrentClaimTests(unittest.TestCase):
    def test_parallel_workers_never_share_a_lease(self) -> None:
        import threading

        clock = FrozenClock(datetime(2026, 9, 26, 8, 0, tzinfo=timezone.utc))
        manifest = make_manifest()
        manifest["shards"] = [
            {"shard_id": f"s{index}", "parcels": [_parcel(f"P-{index}")]} for index in range(6)
        ]
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "survey.sqlite3"
            setup_connection = connect(database)
            service = SurveyCheckoutService(setup_connection, clock)
            service.create_user("planner", "经办", "planner")
            service.create_user("auditor", "审计", "auditor")
            service.publish_rule_set("planner", RULE_SET)
            service.create_job("planner", "job-race", manifest)
            setup_connection.close()

            claimed: list[tuple[str, str]] = []
            errors: list[Exception] = []

            def work(worker_id: str) -> None:
                try:
                    connection = connect(database)
                    worker_service = SurveyCheckoutService(connection, clock)
                    while True:
                        claim = worker_service.claim_shard(worker_id, 300, "job-race")
                        if claim is None:
                            break
                        claimed.append((claim["shard_id"], worker_id))
                    connection.close()
                except Exception as exc:  # pragma: no cover - 失败时由断言报告
                    errors.append(exc)

            threads = [threading.Thread(target=work, args=(f"w{index}",)) for index in range(3)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            self.assertEqual(errors, [])
            shard_ids = [shard_id for shard_id, _ in claimed]
            self.assertEqual(sorted(shard_ids), [f"s{index}" for index in range(6)])
            self.assertEqual(len(shard_ids), len(set(shard_ids)))


class GeometryTests(unittest.TestCase):
    def test_ring_area_and_boundary_offset(self) -> None:
        self.assertEqual(ring_area(SQUARE), Decimal(100))
        self.assertEqual(boundary_offset(SQUARE, SQUARE), Decimal(0))
        shifted = [[Decimal("0.2"), 0], [Decimal("10.2"), 0], [Decimal("10.2"), 10], [Decimal("0.2"), 10]]
        self.assertEqual(boundary_offset(SQUARE, shifted), Decimal("0.2"))

    def test_evaluate_shard_verdict_and_determinism(self) -> None:
        from survey_checkout.contracts import parse_rule_set

        rule_set = parse_rule_set(RULE_SET)
        parcels = [
            _parcel("P-1"),
            _parcel("P-2", cadastral=[[0, 0], [20, 0], [20, 20], [0, 20]]),
        ]
        first = evaluate_shard(rule_set, parcels, input_sha256="x" * 64)
        second = evaluate_shard(rule_set, parcels, input_sha256="x" * 64)
        self.assertEqual(canonical_json(first), canonical_json(second))
        self.assertEqual(first["algorithm_version"], ALGORITHM_VERSION)
        self.assertEqual(first["verdict"], "fail")
        self.assertEqual(first["failed_parcels"], ["P-2"])
        self.assertEqual(first["totals"]["surveyed_area_m2"], Decimal(200))
        self.assertEqual(first["totals"]["cadastral_area_m2"], Decimal(500))


class ApiTests(ServiceTestBase):
    def setUp(self) -> None:
        super().setUp()
        self.app = JsonApplication(self.service)

    def test_http_flow(self) -> None:
        headers = {"X-Actor-Id": "planner"}
        response = self.app.handle("POST", "/jobs", headers, _json({"job_id": "job-http", "manifest": make_manifest()}))
        self.assertEqual(response.status, 201)
        response = self.app.handle("GET", "/jobs/job-http", {"X-Actor-Id": "auditor"})
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body["counts"]["ready"], 2)

        response = self.app.handle("POST", "/shards/claim", {}, _json({"worker_id": "w1", "lease_seconds": 60}))
        self.assertEqual(response.status, 200)
        claim = response.body["claim"]
        response = self.app.handle(
            "POST",
            f"/jobs/job-http/{claim['shard_id']}/complete",
            {},
            _json({
                "worker_id": "w1",
                "input_sha256": claim["input_sha256"],
                "rule_set_sha256": claim["rule_set_sha256"],
            }),
        )
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body["state"], "succeeded")

        response = self.app.handle("POST", "/jobs/job-http/cancel", headers, _json({"reason": "暂停"}))
        self.assertEqual(response.status, 200)
        response = self.app.handle("POST", "/shards/claim", {}, _json({"worker_id": "w2"}))
        self.assertIsNone(response.body["claim"])
        response = self.app.handle("GET", "/jobs/job-http/audit", {"X-Actor-Id": "auditor"})
        self.assertEqual(response.status, 200)
        self.assertTrue(response.body["events"])
        response = self.app.handle("GET", "/jobs", {"X-Actor-Id": "auditor"})
        self.assertEqual(response.status, 200)

    def test_http_error_mapping(self) -> None:
        response = self.app.handle("GET", "/jobs/job-http")
        self.assertEqual(response.status, 422)
        response = self.app.handle("GET", "/no-such-route", {})
        self.assertEqual(response.status, 404)
        response = self.app.handle("POST", "/jobs", {"X-Actor-Id": "auditor"}, _json({"job_id": "j", "manifest": {}}))
        self.assertEqual(response.status, 403)
        response = self.app.handle("GET", "/health")
        self.assertEqual(response.status, 200)


def _json(payload: dict) -> bytes:
    import json

    return json.dumps(payload).encode("utf-8")


if __name__ == "__main__":
    unittest.main()
