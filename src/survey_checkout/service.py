"""测绘校核可恢复分片执行的领域用例。

设计要点：
- 任务创建时把输入清单摘要、规则版本摘要、分片依赖和每个分片的输入快照全部落库；
- 工作进程凭有期限的租约领取分片，租约过期后其他进程可以接管；
- 完成登记前校验租约归属、输入摘要与规则摘要，迟到结果无法覆盖新持有者；
- 成功登记与后续分片解锁、任务收尾在同一个事务内完成；
- 失败按重试策略退避，次数用尽进入人工处置；
- 取消只阻止新的领取，已完成成果与审计证据全部保留；
- 状态查询完全由 SQLite 中的持久行重建，并标明每个分片状态的来源。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import timedelta
from decimal import Decimal
from typing import Any, Mapping

from .checks import evaluate_shard
from .clock import SystemClock, isoformat
from .contracts import RuleSet, identifier, non_negative_int, parse_manifest, parse_rule_set, required_text
from .errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from .jsonio import canonical_json, content_digest
from .storage import initialize, transaction


ROLE_PERMISSIONS = {
    "planner": {"rules.publish", "job.create", "job.cancel", "job.read"},
    "reviewer": {"shard.manual", "job.read"},
    "auditor": {"job.read", "audit.read"},
}

SHARD_STATES = ("waiting", "ready", "leased", "succeeded", "manual", "skipped")


class SurveyCheckoutService:
    """在单个 SQLite 连接上提供测绘校核分片执行的全部操作。"""

    def __init__(self, connection: sqlite3.Connection, clock=None) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        initialize(connection)

    def _now(self) -> str:
        return isoformat(self.clock.now())

    def _user(self, user_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT user_id, display_name, role, active FROM users WHERE user_id=?", (user_id,)
        ).fetchone()
        if row is None:
            raise NotFound(f"用户不存在: {user_id}")
        if not row["active"]:
            raise Forbidden("用户已停用")
        return row

    def _require(self, user_id: str, permission: str) -> sqlite3.Row:
        user = self._user(user_id)
        if permission not in ROLE_PERMISSIONS[user["role"]]:
            raise Forbidden(f"角色 {user['role']} 无权执行 {permission}")
        return user

    def _audit(
        self, entity_type: str, entity_id: str, event_type: str, actor_id: str, payload: Mapping[str, Any]
    ) -> None:
        self.connection.execute(
            "INSERT INTO audit_events(entity_type,entity_id,event_type,actor_id,payload_json,created_at) "
            "VALUES(?,?,?,?,?,?)",
            (entity_type, entity_id, event_type, actor_id, canonical_json(payload), self._now()),
        )

    @staticmethod
    def _shard_entity(job_id: str, shard_id: str) -> str:
        return f"{job_id}/{shard_id}"

    # ------------------------------------------------------------------
    # 用户与规则集
    # ------------------------------------------------------------------

    def create_user(self, user_id: str, display_name: str, role: str) -> dict[str, Any]:
        if role not in ROLE_PERMISSIONS:
            raise ValidationFailed(f"未知角色: {role}")
        if not user_id.strip() or not display_name.strip():
            raise ValidationFailed("用户编号和名称不能为空")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO users(user_id,display_name,role) VALUES(?,?,?)",
                    (user_id.strip(), display_name.strip(), role),
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict(f"用户已存在: {user_id}") from exc
        return {"user_id": user_id.strip(), "role": role}

    def publish_rule_set(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "rules.publish")
        rule_set = parse_rule_set(raw)
        text = canonical_json(rule_set.as_dict())
        digest = content_digest([rule_set.as_dict()])
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO rule_sets(rule_set_id,version,title,canonical_json,content_sha256,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (
                        rule_set.rule_set_id,
                        rule_set.version,
                        rule_set.title,
                        text,
                        digest,
                        actor_id,
                        self._now(),
                    ),
                )
                identity = f"{rule_set.rule_set_id}@{rule_set.version}"
                self._audit("rule_set", identity, "rule_set.published", actor_id, {"sha256": digest})
        except sqlite3.IntegrityError as exc:
            raise Conflict("规则集版本或内容摘要已经存在") from exc
        return {"rule_set_id": rule_set.rule_set_id, "version": rule_set.version, "sha256": digest}

    def _rule_set(self, rule_set_id: str, version: int) -> RuleSet:
        row = self.connection.execute(
            "SELECT * FROM rule_sets WHERE rule_set_id=? AND version=?", (rule_set_id, version)
        ).fetchone()
        if row is None:
            raise NotFound("规则集版本不存在")
        body = json.loads(row["canonical_json"])
        return RuleSet(
            rule_set_id=body["rule_set_id"],
            version=body["version"],
            title=body["title"],
            area_tolerance_m2=Decimal(str(body["area_tolerance_m2"])),
            area_tolerance_ratio=Decimal(str(body["area_tolerance_ratio"])),
            boundary_offset_tolerance_m=Decimal(str(body["boundary_offset_tolerance_m"])),
            content_sha256=row["content_sha256"],
        )

    # ------------------------------------------------------------------
    # 任务创建：清单摘要、规则版本、依赖关系一次落库
    # ------------------------------------------------------------------

    def create_job(self, actor_id: str, job_id: str, raw_manifest: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "job.create")
        job_id = identifier(job_id, "job_id")
        manifest = parse_manifest(raw_manifest)
        rule_set = self._rule_set(manifest.rule_set_id, manifest.rule_set_version)
        manifest_json = canonical_json(raw_manifest)
        manifest_sha256 = content_digest([raw_manifest])
        now = self._now()
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO survey_jobs(job_id,rule_set_id,rule_set_version,rule_set_sha256,manifest_sha256,"
                    "manifest_json,shard_count,max_attempts,backoff_json,state,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        job_id,
                        rule_set.rule_set_id,
                        rule_set.version,
                        rule_set.content_sha256,
                        manifest_sha256,
                        manifest_json,
                        len(manifest.shards),
                        manifest.max_attempts,
                        canonical_json(list(manifest.backoff_seconds)),
                        "active",
                        actor_id,
                        now,
                    ),
                )
                for sequence, shard in enumerate(manifest.shards):
                    slice_payload = {
                        "job_id": job_id,
                        "shard_id": shard.shard_id,
                        "rule_set_id": rule_set.rule_set_id,
                        "rule_set_version": rule_set.version,
                        "parcels": list(shard.parcels),
                    }
                    input_sha256 = content_digest([slice_payload])
                    self.connection.execute(
                        "INSERT INTO shards(job_id,shard_id,sequence,input_sha256,slice_json,state,ready_reason,"
                        "attempts,available_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (
                            job_id,
                            shard.shard_id,
                            sequence,
                            input_sha256,
                            canonical_json(slice_payload),
                            "ready" if not shard.depends_on else "waiting",
                            "initial",
                            0,
                            now,
                            now,
                        ),
                    )
                    for dependency in shard.depends_on:
                        self.connection.execute(
                            "INSERT INTO shard_dependencies(job_id,shard_id,depends_on) VALUES(?,?,?)",
                            (job_id, shard.shard_id, dependency),
                        )
                self._audit(
                    "job",
                    job_id,
                    "job.created",
                    actor_id,
                    {
                        "manifest_sha256": manifest_sha256,
                        "rule_set_sha256": rule_set.content_sha256,
                        "shard_count": len(manifest.shards),
                        "max_attempts": manifest.max_attempts,
                    },
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict(f"任务编号冲突: {job_id}") from exc
        return {
            "job_id": job_id,
            "manifest_sha256": manifest_sha256,
            "rule_set_sha256": rule_set.content_sha256,
            "shard_count": len(manifest.shards),
            "initial_ready": [shard.shard_id for shard in manifest.shards if not shard.depends_on],
            "waiting": [shard.shard_id for shard in manifest.shards if shard.depends_on],
        }

    def get_job(self, job_id: str) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM survey_jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            raise NotFound(f"测绘校核任务不存在: {job_id}")
        return dict(row)

    # ------------------------------------------------------------------
    # 租约领取：过期租约可被接管，重试次数用尽的过期分片直接转人工
    # ------------------------------------------------------------------

    def claim_shard(self, worker_id: str, lease_seconds: int = 60, job_id: str | None = None) -> dict[str, Any] | None:
        worker_id = required_text(worker_id, "worker_id", 128)
        if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, int) or lease_seconds <= 0:
            raise ValidationFailed("租约时长必须是正整数秒")
        if job_id is not None:
            job_id = identifier(job_id, "job_id")
        now = self._now()
        expires = isoformat(self.clock.now() + timedelta(seconds=lease_seconds))
        with transaction(self.connection, immediate=True):
            candidates = self.connection.execute(
                "SELECT s.*, j.max_attempts, j.rule_set_sha256 FROM shards s "
                "JOIN survey_jobs j ON j.job_id=s.job_id "
                "WHERE j.state='active' AND ("
                "  (s.state='ready' AND s.available_at<=?) OR (s.state='leased' AND s.lease_expires_at<=?)"
                ") " + ("AND s.job_id=? " if job_id else "") + "ORDER BY s.sequence, s.job_id, s.shard_id",
                (now, now, job_id) if job_id else (now, now),
            ).fetchall()
            for row in candidates:
                if row["attempts"] + 1 > row["max_attempts"]:
                    if row["state"] == "leased":
                        self.connection.execute(
                            "UPDATE shards SET state='manual',lease_owner=NULL,lease_expires_at=NULL,"
                            "last_error=?,updated_at=? "
                            "WHERE job_id=? AND shard_id=? AND state='leased' AND lease_expires_at<=?",
                            ("租约过期且重试次数用尽", now, row["job_id"], row["shard_id"], now),
                        )
                        self._audit(
                            "shard",
                            self._shard_entity(row["job_id"], row["shard_id"]),
                            "shard.manual",
                            worker_id,
                            {"reason": "lease_expired_attempts_exhausted", "attempts": row["attempts"]},
                        )
                    continue
                takeover = row["state"] == "leased"
                cursor = self.connection.execute(
                    "UPDATE shards SET state='leased',attempts=attempts+1,lease_owner=?,lease_expires_at=?,"
                    "lease_epoch=lease_epoch+1,updated_at=? "
                    "WHERE job_id=? AND shard_id=? AND ("
                    "  (state='ready' AND available_at<=?) OR (state='leased' AND lease_expires_at<=?)"
                    ")",
                    (worker_id, expires, now, row["job_id"], row["shard_id"], now, now),
                )
                if cursor.rowcount != 1:
                    continue
                claimed = self.connection.execute(
                    "SELECT * FROM shards WHERE job_id=? AND shard_id=?",
                    (row["job_id"], row["shard_id"]),
                ).fetchone()
                self._audit(
                    "shard",
                    self._shard_entity(row["job_id"], row["shard_id"]),
                    "shard.claimed",
                    worker_id,
                    {
                        "worker_id": worker_id,
                        "lease_epoch": claimed["lease_epoch"],
                        "lease_expires_at": expires,
                        "takeover": takeover,
                    },
                )
                return {
                    "job_id": claimed["job_id"],
                    "shard_id": claimed["shard_id"],
                    "attempt": claimed["attempts"],
                    "input_sha256": claimed["input_sha256"],
                    "rule_set_sha256": row["rule_set_sha256"],
                    "lease": {
                        "owner": worker_id,
                        "expires_at": expires,
                        "epoch": claimed["lease_epoch"],
                        "takeover": takeover,
                    },
                    "slice": json.loads(claimed["slice_json"]),
                }
        return None

    # ------------------------------------------------------------------
    # 完成登记：租约栅栏 + 输入/规则校验 + 原子解锁后续分片
    # ------------------------------------------------------------------

    @staticmethod
    def _digest(value: object, field: str) -> str:
        text = required_text(value, field, 64)
        if len(text) != 64 or any(char not in "0123456789abcdefABCDEF" for char in text):
            raise ValidationFailed(f"{field} 必须是 64 位十六进制 SHA-256")
        return text.lower()

    def _leased_shard(self, worker_id: str, job_id: str, shard_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT s.*, j.state AS job_state, j.rule_set_id AS job_rule_set_id, "
            "j.rule_set_version AS job_rule_set_version, j.rule_set_sha256 AS job_rule_set_sha256, "
            "j.max_attempts, j.backoff_json "
            "FROM shards s JOIN survey_jobs j ON j.job_id=s.job_id "
            "WHERE s.job_id=? AND s.shard_id=?",
            (job_id, shard_id),
        ).fetchone()
        if row is None:
            raise NotFound(f"分片不存在: {job_id}/{shard_id}")
        if row["state"] != "leased" or row["lease_owner"] != worker_id or row["lease_expires_at"] <= self._now():
            raise Conflict("分片租约已易主或过期，迟到结果不能覆盖新持有者")
        return row

    def complete_shard(
        self,
        worker_id: str,
        job_id: str,
        shard_id: str,
        input_sha256: str,
        rule_set_sha256: str,
    ) -> dict[str, Any]:
        worker_id = required_text(worker_id, "worker_id", 128)
        expected_input = self._digest(input_sha256, "input_sha256")
        expected_rules = self._digest(rule_set_sha256, "rule_set_sha256")
        now = self._now()
        with transaction(self.connection, immediate=True):
            row = self._leased_shard(worker_id, job_id, shard_id)
            if row["input_sha256"] != expected_input:
                raise Conflict("输入清单摘要与任务登记值不一致，分片输入可能已被替换")
            if row["job_rule_set_sha256"] != expected_rules:
                raise Conflict("规则版本摘要与任务登记值不一致")
            rule_set = self._rule_set(row["job_rule_set_id"], row["job_rule_set_version"])
            if rule_set.content_sha256 != row["job_rule_set_sha256"]:
                raise Conflict("规则集内容与任务登记摘要不再一致")
            slice_payload = json.loads(row["slice_json"])
            if content_digest([slice_payload]) != row["input_sha256"]:
                raise Conflict("分片输入快照与登记摘要不一致，存储可能已损坏")
            result = evaluate_shard(rule_set, slice_payload["parcels"], input_sha256=row["input_sha256"])
            output_json = canonical_json(result)
            output_sha256 = hashlib.sha256(output_json.encode("utf-8")).hexdigest()
            cursor = self.connection.execute(
                "UPDATE shards SET state='succeeded',lease_owner=NULL,lease_expires_at=NULL,output_json=?,"
                "output_sha256=?,completed_by=?,completed_at=?,updated_at=? "
                "WHERE job_id=? AND shard_id=? AND state='leased' AND lease_owner=? AND lease_expires_at>?",
                (output_json, output_sha256, worker_id, now, now, job_id, shard_id, worker_id, now),
            )
            if cursor.rowcount != 1:
                raise Conflict("分片租约在登记期间已易主，结果未写入")
            entity = self._shard_entity(job_id, shard_id)
            self._audit(
                "shard",
                entity,
                "shard.succeeded",
                worker_id,
                {"output_sha256": output_sha256, "verdict": result["verdict"], "attempt": row["attempts"]},
            )
            unlocked: list[str] = []
            job_state = row["job_state"]
            if job_state == "active":
                candidates = self.connection.execute(
                    "SELECT shard_id FROM shards WHERE job_id=? AND state='waiting' AND NOT EXISTS ("
                    "  SELECT 1 FROM shard_dependencies d JOIN shards dep "
                    "  ON dep.job_id=d.job_id AND dep.shard_id=d.depends_on "
                    "  WHERE d.job_id=shards.job_id AND d.shard_id=shards.shard_id AND dep.state<>'succeeded'"
                    ") ORDER BY sequence",
                    (job_id,),
                ).fetchall()
                for candidate in candidates:
                    self.connection.execute(
                        "UPDATE shards SET state='ready',ready_reason='dependency',available_at=?,updated_at=? "
                        "WHERE job_id=? AND shard_id=? AND state='waiting'",
                        (now, now, job_id, candidate["shard_id"]),
                    )
                    unlocked.append(candidate["shard_id"])
                    self._audit(
                        "shard",
                        self._shard_entity(job_id, candidate["shard_id"]),
                        "shard.unlocked",
                        worker_id,
                        {"by_shard": shard_id},
                    )
                totals = self.connection.execute(
                    "SELECT count(*) AS total, "
                    "sum(CASE WHEN state='succeeded' THEN 1 ELSE 0 END) AS succeeded "
                    "FROM shards WHERE job_id=?",
                    (job_id,),
                ).fetchone()
                if totals["total"] == totals["succeeded"]:
                    self.connection.execute(
                        "UPDATE survey_jobs SET state='completed',completed_at=? WHERE job_id=? AND state='active'",
                        (now, job_id),
                    )
                    job_state = "completed"
                    self._audit("job", job_id, "job.completed", worker_id, {"shard_count": totals["total"]})
        return {
            "job_id": job_id,
            "shard_id": shard_id,
            "state": "succeeded",
            "output_sha256": output_sha256,
            "verdict": result["verdict"],
            "unlocked_shards": unlocked,
            "job_state": job_state,
        }

    # ------------------------------------------------------------------
    # 失败与人工处置
    # ------------------------------------------------------------------

    def fail_shard(
        self,
        worker_id: str,
        job_id: str,
        shard_id: str,
        error: str,
        retry_seconds: int | None = None,
    ) -> dict[str, Any]:
        worker_id = required_text(worker_id, "worker_id", 128)
        error = required_text(error, "error", 1000)
        now = self._now()
        with transaction(self.connection, immediate=True):
            row = self._leased_shard(worker_id, job_id, shard_id)
            backoff = json.loads(row["backoff_json"])
            if retry_seconds is not None:
                delay = non_negative_int(retry_seconds, "retry_seconds")
            else:
                delay = backoff[min(row["attempts"], len(backoff)) - 1]
            entity = self._shard_entity(job_id, shard_id)
            if row["attempts"] >= row["max_attempts"]:
                cursor = self.connection.execute(
                    "UPDATE shards SET state='manual',lease_owner=NULL,lease_expires_at=NULL,last_error=?,updated_at=? "
                    "WHERE job_id=? AND shard_id=? AND state='leased' AND lease_owner=? AND lease_expires_at>?",
                    (error, now, job_id, shard_id, worker_id, now),
                )
                if cursor.rowcount != 1:
                    raise Conflict("分片租约在登记期间已易主")
                self._audit(
                    "shard", entity, "shard.manual", worker_id,
                    {"reason": "attempts_exhausted", "attempts": row["attempts"], "error": error},
                )
                return {"job_id": job_id, "shard_id": shard_id, "state": "manual", "attempts": row["attempts"]}
            available = isoformat(self.clock.now() + timedelta(seconds=delay))
            cursor = self.connection.execute(
                "UPDATE shards SET state='ready',available_at=?,lease_owner=NULL,lease_expires_at=NULL,"
                "last_error=?,updated_at=? "
                "WHERE job_id=? AND shard_id=? AND state='leased' AND lease_owner=? AND lease_expires_at>?",
                (available, error, now, job_id, shard_id, worker_id, now),
            )
            if cursor.rowcount != 1:
                raise Conflict("分片租约在登记期间已易主")
            self._audit(
                "shard", entity, "shard.failed", worker_id,
                {"error": error, "attempts": row["attempts"], "next_available_at": available},
            )
            return {
                "job_id": job_id,
                "shard_id": shard_id,
                "state": "ready",
                "attempts": row["attempts"],
                "available_at": available,
            }

    def manual_decision(
        self, actor_id: str, job_id: str, shard_id: str, decision: str, note: str = ""
    ) -> dict[str, Any]:
        self._require(actor_id, "shard.manual")
        if decision not in {"retry", "skip"}:
            raise ValidationFailed("人工处置决定必须是 retry 或 skip")
        note = note.strip() if isinstance(note, str) else ""
        now = self._now()
        with transaction(self.connection, immediate=True):
            row = self.connection.execute(
                "SELECT s.*, j.state AS job_state, j.backoff_json FROM shards s "
                "JOIN survey_jobs j ON j.job_id=s.job_id WHERE s.job_id=? AND s.shard_id=?",
                (job_id, shard_id),
            ).fetchone()
            if row is None:
                raise NotFound(f"分片不存在: {job_id}/{shard_id}")
            if row["state"] != "manual":
                raise InvalidState("分片不在人工处置状态")
            entity = self._shard_entity(job_id, shard_id)
            if decision == "retry":
                if row["job_state"] != "active":
                    raise InvalidState("任务已取消或完成，不能重新入队")
                backoff = json.loads(row["backoff_json"])
                available = isoformat(self.clock.now() + timedelta(seconds=backoff[0] if backoff else 0))
                self.connection.execute(
                    "UPDATE shards SET state='ready',attempts=0,available_at=?,lease_owner=NULL,lease_expires_at=NULL,"
                    "manual_note=?,manual_by=?,manual_at=?,updated_at=? "
                    "WHERE job_id=? AND shard_id=? AND state='manual'",
                    (available, note, actor_id, now, now, job_id, shard_id),
                )
                self._audit("shard", entity, "shard.manual_retry", actor_id, {"note": note})
                return {"job_id": job_id, "shard_id": shard_id, "state": "ready", "available_at": available}
            self.connection.execute(
                "UPDATE shards SET state='skipped',lease_owner=NULL,lease_expires_at=NULL,"
                "manual_note=?,manual_by=?,manual_at=?,updated_at=? "
                "WHERE job_id=? AND shard_id=? AND state='manual'",
                (note, actor_id, now, now, job_id, shard_id),
            )
            self._audit("shard", entity, "shard.manual_skip", actor_id, {"note": note})
            return {"job_id": job_id, "shard_id": shard_id, "state": "skipped"}

    # ------------------------------------------------------------------
    # 取消：阻止新领取，保留已完成证据
    # ------------------------------------------------------------------

    def cancel_job(self, actor_id: str, job_id: str, reason: str) -> dict[str, Any]:
        self._require(actor_id, "job.cancel")
        reason = required_text(reason, "reason", 500)
        self.get_job(job_id)
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE survey_jobs SET state='cancelled',cancelled_by=?,cancelled_at=?,cancel_reason=? "
                "WHERE job_id=? AND state='active'",
                (actor_id, self._now(), reason, job_id),
            )
            if cursor.rowcount != 1:
                raise InvalidState("任务已取消或已完成，不能重复取消")
            self._audit("job", job_id, "job.cancelled", actor_id, {"reason": reason})
        return self.get_job(job_id)

    # ------------------------------------------------------------------
    # 状态重建：重启后由持久行推导各分片状态并说明来源
    # ------------------------------------------------------------------

    @staticmethod
    def _derive_state(row: sqlite3.Row, now: str) -> tuple[str, str]:
        stored = row["state"]
        if stored == "waiting":
            return "waiting", "dependency_pending"
        if stored == "ready":
            return "ready", row["ready_reason"] if row["ready_reason"] == "initial" else "dependency_unlocked"
        if stored == "leased":
            if row["lease_expires_at"] <= now:
                return "ready", "lease_expired"
            return "leased", "lease_active"
        if stored == "succeeded":
            return "succeeded", "output_recorded"
        if stored == "manual":
            return "manual", "retry_exhausted"
        return "skipped", "manual_resolution"

    def _shard_view(self, row: sqlite3.Row, now: str, dependencies: list[str], blocked_by: list[str]) -> dict[str, Any]:
        state, source = self._derive_state(row, now)
        lease = None
        if row["lease_owner"] is not None:
            lease = {
                "owner": row["lease_owner"],
                "expires_at": row["lease_expires_at"],
                "epoch": row["lease_epoch"],
                "expired": row["lease_expires_at"] <= now,
            }
        output = None
        if row["output_sha256"] is not None:
            output = {
                "sha256": row["output_sha256"],
                "completed_by": row["completed_by"],
                "completed_at": row["completed_at"],
                "verdict": json.loads(row["output_json"])["verdict"],
            }
        manual = None
        if row["manual_at"] is not None:
            manual = {"by": row["manual_by"], "at": row["manual_at"], "note": row["manual_note"]}
        return {
            "shard_id": row["shard_id"],
            "sequence": row["sequence"],
            "state": state,
            "state_source": source,
            "stored_state": row["state"],
            "input_sha256": row["input_sha256"],
            "depends_on": dependencies,
            "blocked_by": blocked_by,
            "attempts": row["attempts"],
            "available_at": row["available_at"],
            "lease": lease,
            "last_error": row["last_error"],
            "output": output,
            "manual": manual,
            "updated_at": row["updated_at"],
        }

    def _build_status(self, job_id: str) -> dict[str, Any]:
        job = self.get_job(job_id)
        now = self._now()
        rows = self.connection.execute(
            "SELECT * FROM shards WHERE job_id=? ORDER BY sequence", (job_id,)
        ).fetchall()
        dependency_rows = self.connection.execute(
            "SELECT shard_id, depends_on FROM shard_dependencies WHERE job_id=? ORDER BY shard_id, depends_on",
            (job_id,),
        ).fetchall()
        dependencies: dict[str, list[str]] = {}
        for item in dependency_rows:
            dependencies.setdefault(item["shard_id"], []).append(item["depends_on"])
        stored_states = {row["shard_id"]: row["state"] for row in rows}
        shards: list[dict[str, Any]] = []
        counts = {state: 0 for state in SHARD_STATES}
        for row in rows:
            deps = dependencies.get(row["shard_id"], [])
            blocked_by = [dep for dep in deps if stored_states[dep] != "succeeded"]
            view = self._shard_view(row, now, deps, blocked_by)
            counts[view["state"]] += 1
            shards.append(view)
        return {
            "job": job,
            "rebuilt_from": "sqlite",
            "generated_at": now,
            "counts": counts,
            "shards": shards,
        }

    def job_status(self, actor_id: str, job_id: str) -> dict[str, Any]:
        self._require(actor_id, "job.read")
        return self._build_status(job_id)

    def list_jobs(self, actor_id: str) -> dict[str, Any]:
        self._require(actor_id, "job.read")
        rows = self.connection.execute("SELECT * FROM survey_jobs ORDER BY created_at, job_id").fetchall()
        jobs = []
        for row in rows:
            status = self._build_status(row["job_id"])
            jobs.append({
                "job_id": row["job_id"],
                "state": row["state"],
                "rule_set_id": row["rule_set_id"],
                "rule_set_version": row["rule_set_version"],
                "manifest_sha256": row["manifest_sha256"],
                "created_by": row["created_by"],
                "created_at": row["created_at"],
                "counts": status["counts"],
            })
        return {"jobs": jobs}

    def audit_trail(self, actor_id: str, job_id: str) -> dict[str, Any]:
        self._require(actor_id, "audit.read")
        self.get_job(job_id)
        rows = self.connection.execute(
            "SELECT event_type,entity_type,entity_id,actor_id,payload_json,created_at FROM audit_events "
            "WHERE entity_id=? OR substr(entity_id, 1, ?)=? ORDER BY event_id",
            (job_id, len(job_id) + 1, f"{job_id}/"),
        ).fetchall()
        return {
            "job_id": job_id,
            "events": [dict(row) | {"payload": json.loads(row["payload_json"])} for row in rows],
        }
