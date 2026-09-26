from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from survey_check.acceptance import run as acceptance_run
from survey_check.api import JsonApplication, encode_body
from survey_check.clock import FrozenClock
from survey_check.compute import (
    area_stats,
    boundary_compare,
    max_deviation_m,
    mu_to_sqm,
    polygon_area_sqm,
    run_shard,
    summary,
)
from survey_check.contracts import JobDefinition, ValidationError
from survey_check.errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from survey_check.service import SurveyCheckService
from survey_check.storage import connect


RULE = {
    "rule_version": "rules-v1",
    "boundary_tolerance_m": "0.05",
    "area_tolerance_percent": "0.5",
    "lease_seconds": 30,
    "max_attempts": 2,
    "retry_delay_seconds": 5,
}

PARCELS = [
    {
        "parcel_id": "p-1",
        "zone": "east",
        "declared_area_mu": "7.5",
        "declared_boundary": [[0, 0], [100, 0], [100, 50], [0, 50]],
        "surveyed_boundary": [[0, 0], [100, 0], [100, 50], [0, 50]],
        "source_revision": "r1",
    },
    {
        "parcel_id": "p-2",
        "zone": "west",
        "declared_area_mu": "4.5",
        "declared_boundary": [[0, 100], [60, 100], [60, 150], [0, 150]],
        "surveyed_boundary": [[0, 100], [60.03, 100], [60.03, 150], [0, 150]],
        "source_revision": "r1",
    },
]

SHARDS = [
    {"shard_key": "boundary:east", "kind": "boundary-compare", "zone": "east", "depends_on": []},
    {"shard_key": "boundary:west", "kind": "boundary-compare", "zone": "west", "depends_on": []},
    {"shard_key": "area:east", "kind": "area-stats", "zone": "east", "depends_on": ["boundary:east"]},
    {"shard_key": "area:west", "kind": "area-stats", "zone": "west", "depends_on": ["boundary:west"]},
    {"shard_key": "summary", "kind": "summary", "depends_on": ["area:east", "area:west"]},
]


def job_definition(job_id: str = "job-1") -> dict:
    return {
        "job_id": job_id,
        "title": "测试测绘校核",
        "rule": dict(RULE),
        "parcels": [dict(item) for item in PARCELS],
        "shards": [dict(item) for item in SHARDS],
    }


def chain_definition(job_id: str = "job-1") -> dict:
    """单片区三分片链，便于精确控制领取顺序。"""

    definition = job_definition(job_id)
    definition["parcels"] = [dict(PARCELS[0])]
    definition["shards"] = [
        {"shard_key": "boundary:east", "kind": "boundary-compare", "zone": "east", "depends_on": []},
        {"shard_key": "area:east", "kind": "area-stats", "zone": "east", "depends_on": ["boundary:east"]},
        {"shard_key": "summary", "kind": "summary", "depends_on": ["area:east"]},
    ]
    return definition


class ComputeTests(unittest.TestCase):
    def test_mu_conversion_and_polygon_area(self) -> None:
        self.assertEqual(mu_to_sqm(Decimal("7.5")), Decimal("5000.0000"))
        square = [[0, 0], [100, 0], [100, 50], [0, 50]]
        self.assertEqual(polygon_area_sqm(square), Decimal("5000.0000"))

    def test_max_deviation_and_vertex_mismatch(self) -> None:
        declared = [[0, 0], [100, 0], [100, 50], [0, 50]]
        surveyed = [[0.03, 0.04], [100, 0], [100, 50], [0, 50]]
        deviation, mismatch = max_deviation_m(declared, surveyed)
        self.assertEqual(deviation, Decimal("0.05"))
        self.assertFalse(mismatch)
        _, mismatch = max_deviation_m(declared, surveyed[:3])
        self.assertTrue(mismatch)

    def test_boundary_compare_marks_out_of_tolerance(self) -> None:
        rule = {"boundary_tolerance_m": Decimal("0.05"), "area_tolerance_percent": Decimal("0.5")}
        parcels = [
            {"parcel_id": "p-1", "declared_area_mu": "7.5",
             "declared_boundary": [[0, 0], [100, 0], [100, 50], [0, 50]],
             "surveyed_boundary": [[0, 0], [100, 0], [100, 50], [0, 50]]},
            {"parcel_id": "p-2", "declared_area_mu": "7.5",
             "declared_boundary": [[0, 0], [100, 0], [100, 50], [0, 50]],
             "surveyed_boundary": [[0, 0], [110, 0], [110, 50], [0, 50]]},
        ]
        result = boundary_compare("east", parcels, rule)
        self.assertEqual(result["passed"], 1)
        self.assertEqual(result["failed"], 1)
        statuses = {item["parcel_id"]: item["status"] for item in result["parcels"]}
        self.assertEqual(statuses, {"p-1": "pass", "p-2": "fail"})

    def test_area_stats_incomplete_when_dependency_skipped(self) -> None:
        rule = {"boundary_tolerance_m": Decimal("0.05"), "area_tolerance_percent": Decimal("0.5")}
        parcels = [
            {"parcel_id": "p-1", "declared_area_mu": "7.5",
             "declared_boundary": [[0, 0], [100, 0], [100, 50], [0, 50]],
             "surveyed_boundary": [[0, 0], [100, 0], [100, 50], [0, 50]]},
        ]
        deps = [{"shard_key": "boundary:east", "kind": "boundary-compare", "state": "skipped",
                 "output_sha256": None, "result": None}]
        result = area_stats("east", parcels, deps, rule)
        self.assertEqual(result["conclusion"], "incomplete")
        deps[0]["state"] = "succeeded"
        self.assertEqual(area_stats("east", parcels, deps, rule)["conclusion"], "pass")

    def test_summary_aggregates_zone_results(self) -> None:
        deps = [
            {"shard_key": "area:east", "kind": "area-stats", "state": "succeeded", "output_sha256": "a" * 64,
             "result": {"zone": "east", "parcel_count": 2, "declared_area_sqm": "100",
                        "surveyed_area_sqm": "100", "area_variance_percent": "0", "conclusion": "pass"}},
            {"shard_key": "area:west", "kind": "area-stats", "state": "skipped",
             "output_sha256": None, "result": None},
        ]
        result = summary(deps, {})
        self.assertEqual(result["conclusion"], "incomplete")
        self.assertEqual(result["skipped_dependencies"], ["area:west"])
        self.assertEqual(result["parcel_count"], 2)


class ContractTests(unittest.TestCase):
    def test_cycle_rejected(self) -> None:
        definition = chain_definition()
        definition["shards"] = [
            {"shard_key": "a", "kind": "boundary-compare", "zone": "east", "depends_on": ["b"]},
            {"shard_key": "b", "kind": "area-stats", "zone": "east", "depends_on": ["a"]},
        ]
        with self.assertRaises(ValidationError):
            JobDefinition.from_dict(definition)

    def test_missing_dependency_rejected(self) -> None:
        definition = chain_definition()
        definition["shards"][1]["depends_on"] = ["ghost"]
        with self.assertRaises(ValidationError):
            JobDefinition.from_dict(definition)

    def test_unknown_zone_and_summary_rules(self) -> None:
        definition = chain_definition()
        definition["shards"][0]["zone"] = "nowhere"
        with self.assertRaises(ValidationError):
            JobDefinition.from_dict(definition)
        definition = chain_definition()
        definition["shards"][2]["depends_on"] = []
        with self.assertRaises(ValidationError):
            JobDefinition.from_dict(definition)

    def test_duplicate_shard_key_rejected(self) -> None:
        definition = chain_definition()
        definition["shards"].append(dict(definition["shards"][0]))
        with self.assertRaises(ValidationError):
            JobDefinition.from_dict(definition)


class ServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 26, 8, 0, tzinfo=timezone.utc))
        self.service = SurveyCheckService(self.connection, self.clock)
        for user_id, role in (("coord", "coordinator"), ("reviewer", "reviewer"), ("audit", "auditor")):
            self.service.create_user(user_id, user_id, role)

    def tearDown(self) -> None:
        self.connection.close()

    def _create(self, definition: dict | None = None) -> dict:
        return self.service.create_job("coord", definition or chain_definition())

    def _work(self, worker: str = "worker-1", job_id: str = "job-1") -> dict:
        claim = self.service.claim_shard(worker, job_id)
        self.assertIsNotNone(claim)
        result = run_shard(claim["kind"], claim["input"])
        return self.service.complete_shard(worker, claim["shard_id"], claim["fencing_token"], result)

    def test_create_job_sets_initial_states(self) -> None:
        status = self._create()
        states = {item["shard_key"]: item["effective_state"] for item in status["shards"]}
        self.assertEqual(states, {"boundary:east": "ready", "area:east": "blocked", "summary": "blocked"})
        self.assertEqual(len(status["manifest_sha256"]), 64)
        self.assertEqual(status["rule_version"], "rules-v1")
        blocked = status["shards"][1]
        self.assertEqual(blocked["source"]["basis"], "dependency_table")
        self.assertEqual(blocked["source"]["unfinished_dependencies"], ["boundary:east"])

    def test_dependency_blocks_claim_until_upstream_done(self) -> None:
        self._create()
        claim = self.service.claim_shard("worker-1", "job-1")
        self.assertEqual(claim["shard_key"], "boundary:east")
        self.assertIsNone(self.service.claim_shard("worker-2", "job-1"))
        result = run_shard(claim["kind"], claim["input"])
        done = self.service.complete_shard("worker-1", claim["shard_id"], claim["fencing_token"], result)
        self.assertEqual(done["unlocked"], ["area:east"])
        follow_up = self.service.claim_shard("worker-2", "job-1")
        self.assertEqual(follow_up["shard_key"], "area:east")

    def test_lease_takeover_and_late_result_rejected(self) -> None:
        self._create()
        first = self.service.claim_shard("worker-a", "job-1", lease_seconds=10)
        self.clock.advance(seconds=11)
        second = self.service.claim_shard("worker-b", "job-1")
        self.assertEqual(first["shard_id"], second["shard_id"])
        self.assertTrue(second["takeover"])
        self.assertEqual(second["fencing_token"], first["fencing_token"] + 1)
        late = run_shard(first["kind"], first["input"])
        with self.assertRaises(Conflict):
            self.service.complete_shard("worker-a", first["shard_id"], first["fencing_token"], late)

    def test_expired_lease_holder_cannot_complete(self) -> None:
        self._create()
        claim = self.service.claim_shard("worker-a", "job-1", lease_seconds=10)
        self.clock.advance(seconds=11)
        result = run_shard(claim["kind"], claim["input"])
        with self.assertRaises(InvalidState):
            self.service.complete_shard("worker-a", claim["shard_id"], claim["fencing_token"], result)

    def test_complete_replay_is_idempotent(self) -> None:
        self._create()
        claim = self.service.claim_shard("worker-a", "job-1")
        result = run_shard(claim["kind"], claim["input"])
        done = self.service.complete_shard("worker-a", claim["shard_id"], claim["fencing_token"], result)
        replay = self.service.complete_shard("worker-a", claim["shard_id"], claim["fencing_token"], result)
        self.assertFalse(done["replayed"])
        self.assertTrue(replay["replayed"])
        self.assertEqual(done["output_sha256"], replay["output_sha256"])
        changed = dict(result)
        changed["passed"] = 99
        with self.assertRaises(Conflict):
            self.service.complete_shard("worker-a", claim["shard_id"], claim["fencing_token"], changed)

    def test_parcel_correction_invalidates_inflight_completion(self) -> None:
        self._create()
        self._work()
        claim = self.service.claim_shard("worker-a", "job-1")
        self.assertEqual(claim["shard_key"], "area:east")
        corrected = dict(PARCELS[0])
        corrected["surveyed_boundary"] = [[0, 0], [100.02, 0], [100.02, 50], [0, 50]]
        corrected["source_revision"] = "r2"
        self.service.correct_parcel("coord", "job-1", "p-1", corrected)
        stale = run_shard(claim["kind"], claim["input"])
        with self.assertRaises(Conflict):
            self.service.complete_shard("worker-a", claim["shard_id"], claim["fencing_token"], stale)
        reclaimed = self.service.claim_shard("worker-a", "job-1")
        self.assertEqual(reclaimed["shard_key"], "area:east")
        self.assertNotEqual(reclaimed["input_sha256"], claim["input_sha256"])
        fresh = run_shard(reclaimed["kind"], reclaimed["input"])
        done = self.service.complete_shard(
            "worker-a", reclaimed["shard_id"], reclaimed["fencing_token"], fresh
        )
        self.assertEqual(done["state"], "succeeded")

    def test_rule_update_invalidates_inflight_completion(self) -> None:
        self._create()
        claim = self.service.claim_shard("worker-a", "job-1")
        updated_rule = dict(RULE, rule_version="rules-v2", boundary_tolerance_m="0.01")
        self.service.update_rule("coord", "job-1", updated_rule)
        stale = run_shard(claim["kind"], claim["input"])
        with self.assertRaises(Conflict):
            self.service.complete_shard("worker-a", claim["shard_id"], claim["fencing_token"], stale)
        status = self.service.job_status("audit", "job-1")
        self.assertEqual(status["rule_version"], "rules-v2")
        self.assertEqual(status["rule_revision"], 2)

    def test_failure_retry_then_needs_review_then_manual_retry(self) -> None:
        self._create()
        first = self.service.claim_shard("worker-a", "job-1")
        failed = self.service.fail_shard("worker-a", first["shard_id"], first["fencing_token"], "临时故障")
        self.assertEqual(failed["state"], "ready")
        self.assertIsNone(self.service.claim_shard("worker-b", "job-1"))
        self.clock.advance(seconds=5)
        second = self.service.claim_shard("worker-b", "job-1")
        self.assertEqual(second["attempts"], 2)
        exhausted = self.service.fail_shard("worker-b", second["shard_id"], second["fencing_token"], "再次故障")
        self.assertEqual(exhausted["state"], "needs_review")
        with self.assertRaises(InvalidState):
            self.service.fail_shard("worker-b", second["shard_id"], second["fencing_token"], "迟到失败")
        resolved = self.service.resolve_shard("reviewer", second["shard_id"], "retry", "现场复核后重算")
        self.assertEqual(resolved["state"], "ready")
        third = self.service.claim_shard("worker-c", "job-1")
        self.assertEqual(third["attempts"], 1)

    def test_lease_exhaustion_sweeps_to_needs_review(self) -> None:
        self._create()
        first = self.service.claim_shard("worker-a", "job-1", lease_seconds=5)
        self.clock.advance(seconds=6)
        second = self.service.claim_shard("worker-b", "job-1", lease_seconds=5)
        self.assertEqual(second["attempts"], 2)
        self.clock.advance(seconds=6)
        self.assertIsNone(self.service.claim_shard("worker-c", "job-1"))
        status = self.service.job_status("audit", "job-1")
        boundary = status["shards"][0]
        self.assertEqual(boundary["effective_state"], "needs_review")
        self.assertEqual(boundary["source"]["basis"], "retry_policy")

    def test_resolve_skip_unblocks_dependents(self) -> None:
        self._create()
        claim = self.service.claim_shard("worker-a", "job-1")
        self.service.fail_shard("worker-a", claim["shard_id"], claim["fencing_token"], "故障")
        self.clock.advance(seconds=5)
        claim = self.service.claim_shard("worker-a", "job-1")
        exhausted = self.service.fail_shard("worker-a", claim["shard_id"], claim["fencing_token"], "故障")
        self.assertEqual(exhausted["state"], "needs_review")
        resolved = self.service.resolve_shard("reviewer", claim["shard_id"], "skip", "原始图纸缺失，跳过比对")
        self.assertEqual(resolved["unlocked"], ["area:east"])
        follow = self.service.claim_shard("worker-b", "job-1")
        self.assertEqual(follow["shard_key"], "area:east")
        result = run_shard(follow["kind"], follow["input"])
        self.assertEqual(result["conclusion"], "incomplete")

    def test_cancel_blocks_claims_but_preserves_evidence(self) -> None:
        self._create()
        self._work()
        cancelled = self.service.cancel_job("coord", "job-1", "计划调整")
        self.assertEqual(cancelled["preserved_succeeded_shards"], 1)
        with self.assertRaises(InvalidState):
            self.service.claim_shard("worker-b", "job-1")
        status = self.service.job_status("audit", "job-1")
        self.assertFalse(status["claimable"])
        self.assertIn("任务已取消：阻止新领取，已完成分片证据保留", status["notes"])
        succeeded = [item for item in status["shards"] if item["effective_state"] == "succeeded"]
        self.assertEqual(len(succeeded), 1)
        self.assertEqual(succeeded[0]["source"]["basis"], "output_row")
        self.assertEqual(len(succeeded[0]["output_sha256"]), 64)

    def test_inflight_completion_still_recorded_after_cancel(self) -> None:
        self._create()
        claim = self.service.claim_shard("worker-a", "job-1")
        self.service.cancel_job("coord", "job-1", "计划调整")
        result = run_shard(claim["kind"], claim["input"])
        done = self.service.complete_shard("worker-a", claim["shard_id"], claim["fencing_token"], result)
        self.assertEqual(done["state"], "succeeded")
        status = self.service.job_status("audit", "job-1")
        self.assertEqual(status["state"], "cancelled")
        self.assertEqual(status["progress"].get("succeeded"), 1)

    def test_restart_rebuilds_status_with_sources(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "survey.sqlite3"
            connection = connect(database)
            service = SurveyCheckService(connection, self.clock)
            for user_id, role in (("coord", "coordinator"), ("reviewer", "reviewer"), ("audit", "auditor")):
                service.create_user(user_id, user_id, role)
            service.create_job("coord", chain_definition())
            claim = service.claim_shard("worker-a", "job-1")
            result = run_shard(claim["kind"], claim["input"])
            service.complete_shard("worker-a", claim["shard_id"], claim["fencing_token"], result)
            leased = service.claim_shard("worker-b", "job-1", lease_seconds=10)
            self.clock.advance(seconds=11)
            connection.close()

            reopened = connect(database)
            recovered = SurveyCheckService(reopened, self.clock)
            status = recovered.job_status("audit", "job-1")
            by_key = {item["shard_key"]: item for item in status["shards"]}
            self.assertEqual(by_key["boundary:east"]["effective_state"], "succeeded")
            self.assertEqual(by_key["boundary:east"]["source"]["basis"], "output_row")
            self.assertEqual(by_key["area:east"]["effective_state"], "expired")
            self.assertEqual(by_key["area:east"]["source"]["basis"], "lease_columns")
            self.assertEqual(by_key["summary"]["effective_state"], "blocked")
            self.assertEqual(by_key["summary"]["source"]["unfinished_dependencies"], ["area:east"])
            # 过期租约在重启后仍可被其他进程接管。
            takeover = recovered.claim_shard("worker-c", "job-1")
            self.assertEqual(takeover["shard_id"], leased["shard_id"])
            self.assertEqual(takeover["fencing_token"], leased["fencing_token"] + 1)
            audit = recovered.audit_trail("audit", "job-1")
            self.assertTrue(audit["valid"])
            reopened.close()

    def test_retry_waiting_and_stale_flags_in_status(self) -> None:
        self._create()
        claim = self.service.claim_shard("worker-a", "job-1")
        self.service.fail_shard("worker-a", claim["shard_id"], claim["fencing_token"], "故障")
        status = self.service.job_status("audit", "job-1")
        boundary = status["shards"][0]
        self.assertEqual(boundary["effective_state"], "retry_waiting")
        self.assertEqual(boundary["source"]["basis"], "retry_schedule")
        self.clock.advance(seconds=5)
        claim = self.service.claim_shard("worker-a", "job-1")
        result = run_shard(claim["kind"], claim["input"])
        self.service.complete_shard("worker-a", claim["shard_id"], claim["fencing_token"], result)
        corrected = dict(PARCELS[0])
        corrected["source_revision"] = "r2"
        self.service.correct_parcel("coord", "job-1", "p-1", corrected)
        status = self.service.job_status("audit", "job-1")
        boundary = status["shards"][0]
        self.assertEqual(boundary["effective_state"], "succeeded")
        self.assertTrue(boundary["input_stale"])
        self.assertTrue(any("旧输入清单" in note for note in status["notes"]))

    def test_result_consistency_checks(self) -> None:
        self._create()
        claim = self.service.claim_shard("worker-a", "job-1")
        result = run_shard(claim["kind"], claim["input"])
        tampered = dict(result)
        tampered["parcels"] = []
        with self.assertRaises(ValidationFailed):
            self.service.complete_shard("worker-a", claim["shard_id"], claim["fencing_token"], tampered)
        wrong_zone = dict(result)
        wrong_zone["zone"] = "west"
        with self.assertRaises(ValidationFailed):
            self.service.complete_shard("worker-a", claim["shard_id"], claim["fencing_token"], wrong_zone)

    def test_permissions_and_not_found(self) -> None:
        with self.assertRaises(Forbidden):
            self.service.create_job("reviewer", chain_definition())
        with self.assertRaises(Forbidden):
            self.service.cancel_job("audit", "job-1", "无权限")
        with self.assertRaises(Forbidden):
            self.service.resolve_shard("coord", 1, "retry", "无权限")
        with self.assertRaises(NotFound):
            self.service.job_status("audit", "ghost")
        self._create()
        with self.assertRaises(Forbidden):
            self.service.audit_trail("coord", "job-1")

    def test_full_dag_completes_job(self) -> None:
        self._create(job_definition())
        for _ in range(5):
            self._work()
        status = self.service.job_status("audit", "job-1")
        self.assertEqual(status["state"], "completed")
        self.assertEqual(status["progress"], {"succeeded": 5})
        summary_shard = [item for item in status["shards"] if item["shard_key"] == "summary"][0]
        self.assertEqual(summary_shard["source"]["basis"], "output_row")


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 26, 8, 0, tzinfo=timezone.utc))
        self.app = JsonApplication(SurveyCheckService(self.connection, self.clock))
        self.app.handle(
            "POST", "/users",
            body=json.dumps({"user_id": "coord", "display_name": "协调员", "role": "coordinator"}).encode(),
        )

    def tearDown(self) -> None:
        self.connection.close()

    def test_health(self) -> None:
        response = self.app.handle("GET", "/health")
        self.assertEqual(response.status, 200)

    def test_job_lifecycle_over_http(self) -> None:
        created = self.app.handle(
            "POST", "/jobs", {"X-Actor-Id": "coord"}, json.dumps(chain_definition()).encode()
        )
        self.assertEqual(created.status, 201)
        claim = self.app.handle("POST", "/shards/claim", body=json.dumps({"worker_id": "w-1"}).encode())
        self.assertEqual(claim.status, 200)
        # 领取载荷含 Decimal 数值，编码器必须能序列化。
        encode_body(claim.body)
        leased = claim.body["claim"]
        result = run_shard(leased["kind"], leased["input"])
        done = self.app.handle(
            "POST", f"/shards/{leased['shard_id']}/complete",
            body=json.dumps({
                "worker_id": "w-1",
                "fencing_token": leased["fencing_token"],
                "result": json.loads(json.dumps(result, default=str)),
            }).encode(),
        )
        self.assertEqual(done.status, 200)
        status = self.app.handle("GET", "/jobs/job-1", {"X-Actor-Id": "coord"})
        self.assertEqual(status.status, 200)
        self.assertEqual(status.body["progress"].get("succeeded"), 1)

    def test_error_shape(self) -> None:
        response = self.app.handle("POST", "/jobs", body=b"{}")
        self.assertEqual(response.status, 422)
        self.assertIn("error", response.body)


class AcceptanceTests(unittest.TestCase):
    def test_offline_acceptance(self) -> None:
        result = acceptance_run()
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["restart_progress"], {"succeeded": 5})
        self.assertTrue(result["audit_valid"])
        self.assertEqual(result["job_state_after_summary"], "completed")
        self.assertFalse(result["cancelled_evidence"]["claimable"])


if __name__ == "__main__":
    unittest.main()
