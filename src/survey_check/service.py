"""测绘校核任务的可恢复分片执行服务。

输入清单摘要、规则版本、分片依赖与输出校验值全部保存在 SQLite 中，
进程重启后可以从表中重建每个分片的状态并说明状态来源。
"""

from __future__ import annotations

import hashlib
import sqlite3
from datetime import timedelta
from decimal import Decimal
from typing import Any, Mapping

from .clock import SystemClock, utc_text
from .contracts import JobDefinition, ParcelEntry, Rule, ValidationError, validate_result
from .errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from .jsonio import canonical_json, content_digest, parse_json
from .storage import initialize, transaction


ROLE_PERMISSIONS = {
    "coordinator": {"job.create", "job.cancel", "job.correct", "rule.update", "job.read"},
    "reviewer": {"shard.resolve", "job.read"},
    "auditor": {"job.read", "audit.read"},
}

TERMINAL_SHARD_STATES = ("succeeded", "skipped")


class SurveyCheckService:
    """在单个 SQLite 连接上提供测绘校核的全部业务操作。"""

    def __init__(self, connection: sqlite3.Connection, clock=None) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        initialize(connection)

    def _now(self) -> str:
        return utc_text(self.clock.now())

    def _user(self, user_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT user_id, display_name, role, active FROM survey_users WHERE user_id=?", (user_id,)
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
        previous = self.connection.execute(
            "SELECT event_hash FROM survey_audit_events ORDER BY event_id DESC LIMIT 1"
        ).fetchone()
        previous_hash = "0" * 64 if previous is None else previous["event_hash"]
        body = {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "event_type": event_type,
            "actor_id": actor_id,
            "payload": payload,
            "created_at": self._now(),
            "previous_hash": previous_hash,
        }
        event_hash = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
        self.connection.execute(
            "INSERT INTO survey_audit_events(entity_type,entity_id,event_type,actor_id,payload_json,"
            "previous_hash,event_hash,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (
                entity_type,
                entity_id,
                event_type,
                actor_id,
                canonical_json(payload),
                previous_hash,
                event_hash,
                body["created_at"],
            ),
        )

    def create_user(self, user_id: str, display_name: str, role: str) -> dict[str, Any]:
        if role not in ROLE_PERMISSIONS:
            raise ValidationFailed(f"未知角色: {role}")
        if not user_id.strip() or not display_name.strip():
            raise ValidationFailed("用户编号和名称不能为空")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO survey_users(user_id,display_name,role,created_at) VALUES(?,?,?,?)",
                    (user_id.strip(), display_name.strip(), role, self._now()),
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict(f"用户已存在: {user_id}") from exc
        return {"user_id": user_id.strip(), "role": role}

    # ------------------------------------------------------------------
    # 任务定义：输入清单摘要、规则版本与分片依赖
    # ------------------------------------------------------------------

    @staticmethod
    def _manifest_digest(parcels: list[dict[str, Any]]) -> str:
        ordered = sorted(parcels, key=lambda item: item["parcel_id"])
        return content_digest(ordered)

    def create_job(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "job.create")
        try:
            definition = JobDefinition.from_dict(raw)
        except ValidationError as exc:
            raise ValidationFailed(str(exc)) from exc
        rule_doc = definition.rule.as_dict()
        rule_sha256 = content_digest([rule_doc])
        parcel_docs = [parcel.as_dict() for parcel in definition.parcels]
        manifest_sha256 = self._manifest_digest(parcel_docs)
        now = self._now()
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO survey_jobs(job_id,title,state,rule_version,rule_json,rule_sha256,"
                    "manifest_sha256,parcel_count,lease_seconds,max_attempts,retry_delay_seconds,"
                    "created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        definition.job_id,
                        definition.title,
                        "active",
                        definition.rule.rule_version,
                        canonical_json(rule_doc),
                        rule_sha256,
                        manifest_sha256,
                        len(parcel_docs),
                        definition.rule.lease_seconds,
                        definition.rule.max_attempts,
                        definition.rule.retry_delay_seconds,
                        actor_id,
                        now,
                    ),
                )
                for parcel, doc in zip(definition.parcels, parcel_docs):
                    self.connection.execute(
                        "INSERT INTO survey_parcels(job_id,parcel_id,zone,declared_area_mu,"
                        "declared_boundary_json,surveyed_boundary_json,source_revision,content_sha256,"
                        "updated_by,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (
                            definition.job_id,
                            parcel.parcel_id,
                            parcel.zone,
                            format(parcel.declared_area_mu, "f"),
                            canonical_json(doc["declared_boundary"]),
                            canonical_json(doc["surveyed_boundary"]),
                            parcel.source_revision,
                            content_digest([doc]),
                            actor_id,
                            now,
                        ),
                    )
                for shard in definition.shards:
                    self.connection.execute(
                        "INSERT INTO survey_shards(job_id,shard_key,kind,zone,state,available_at,"
                        "created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                        (
                            definition.job_id,
                            shard.shard_key,
                            shard.kind,
                            shard.zone,
                            "ready" if not shard.depends_on else "blocked",
                            now,
                            now,
                            now,
                        ),
                    )
                for shard in definition.shards:
                    for dependency in shard.depends_on:
                        self.connection.execute(
                            "INSERT INTO survey_shard_deps(job_id,shard_key,depends_on) VALUES(?,?,?)",
                            (definition.job_id, shard.shard_key, dependency),
                        )
                self._audit(
                    "job",
                    definition.job_id,
                    "job.created",
                    actor_id,
                    {
                        "manifest_sha256": manifest_sha256,
                        "rule_version": definition.rule.rule_version,
                        "rule_sha256": rule_sha256,
                        "parcel_count": len(parcel_docs),
                        "shards": [
                            {"shard_key": item.shard_key, "kind": item.kind, "depends_on": list(item.depends_on)}
                            for item in definition.shards
                        ],
                    },
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict(f"任务编号已存在: {definition.job_id}") from exc
        return self.job_status(actor_id, definition.job_id)

    def _job_row(self, job_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM survey_jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        if row is None:
            raise NotFound(f"校核任务不存在: {job_id}")
        return row

    def _active_job(self, job_id: str) -> sqlite3.Row:
        job = self._job_row(job_id)
        if job["state"] != "active":
            raise InvalidState("任务已取消或已完成，不能再修改输入清单或规则")
        return job

    def correct_parcel(
        self, actor_id: str, job_id: str, parcel_id: str, raw: Mapping[str, Any]
    ) -> dict[str, Any]:
        """更正输入清单中的一条地块登记；在途分片完成时会发现输入已变化。"""

        self._require(actor_id, "job.correct")
        job = self._active_job(job_id)
        existing = self.connection.execute(
            "SELECT * FROM survey_parcels WHERE job_id=? AND parcel_id=?", (job_id, parcel_id)
        ).fetchone()
        if existing is None:
            raise NotFound(f"地块不在输入清单中: {parcel_id}")
        try:
            entry = ParcelEntry.from_dict(raw)
        except ValidationError as exc:
            raise ValidationFailed(str(exc)) from exc
        if entry.parcel_id != parcel_id or entry.zone != existing["zone"]:
            raise ValidationFailed("更正不能改变地块编号或所属片区")
        doc = entry.as_dict()
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "UPDATE survey_parcels SET declared_area_mu=?,declared_boundary_json=?,"
                "surveyed_boundary_json=?,source_revision=?,content_sha256=?,updated_by=?,updated_at=? "
                "WHERE job_id=? AND parcel_id=?",
                (
                    format(entry.declared_area_mu, "f"),
                    canonical_json(doc["declared_boundary"]),
                    canonical_json(doc["surveyed_boundary"]),
                    entry.source_revision,
                    content_digest([doc]),
                    actor_id,
                    self._now(),
                    job_id,
                    parcel_id,
                ),
            )
            manifest_sha256 = self._manifest_digest(self._parcel_docs(job_id))
            self.connection.execute(
                "UPDATE survey_jobs SET manifest_sha256=?,input_revision=input_revision+1 WHERE job_id=?",
                (manifest_sha256, job_id),
            )
            self._audit(
                "job",
                job_id,
                "parcel.corrected",
                actor_id,
                {
                    "parcel_id": parcel_id,
                    "manifest_sha256": manifest_sha256,
                    "input_revision": job["input_revision"] + 1,
                },
            )
        return {"job_id": job_id, "parcel_id": parcel_id, "manifest_sha256": manifest_sha256}

    def update_rule(self, actor_id: str, job_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        """登记新的规则版本；在途分片完成时会发现规则已变化。"""

        self._require(actor_id, "rule.update")
        job = self._active_job(job_id)
        try:
            rule = Rule.from_dict(raw)
        except ValidationError as exc:
            raise ValidationFailed(str(exc)) from exc
        rule_doc = rule.as_dict()
        rule_sha256 = content_digest([rule_doc])
        if rule_sha256 == job["rule_sha256"]:
            raise Conflict("规则内容与当前版本一致，无需变更")
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "UPDATE survey_jobs SET rule_version=?,rule_json=?,rule_sha256=?,"
                "rule_revision=rule_revision+1,lease_seconds=?,max_attempts=?,retry_delay_seconds=? "
                "WHERE job_id=?",
                (
                    rule.rule_version,
                    canonical_json(rule_doc),
                    rule_sha256,
                    rule.lease_seconds,
                    rule.max_attempts,
                    rule.retry_delay_seconds,
                    job_id,
                ),
            )
            self._audit(
                "job",
                job_id,
                "rule.updated",
                actor_id,
                {
                    "rule_version": rule.rule_version,
                    "rule_sha256": rule_sha256,
                    "rule_revision": job["rule_revision"] + 1,
                },
            )
        return {"job_id": job_id, "rule_version": rule.rule_version, "rule_sha256": rule_sha256}

    # ------------------------------------------------------------------
    # 分片领取：有期限租约与围栏令牌
    # ------------------------------------------------------------------

    def _parcel_docs(self, job_id: str, zone: str | None = None) -> list[dict[str, Any]]:
        sql = (
            "SELECT * FROM survey_parcels WHERE job_id=?"
            + (" AND zone=?" if zone is not None else "")
            + " ORDER BY parcel_id"
        )
        rows = self.connection.execute(sql, (job_id, zone) if zone is not None else (job_id,)).fetchall()
        return [self._parcel_doc(row) for row in rows]

    @staticmethod
    def _parcel_doc(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "parcel_id": row["parcel_id"],
            "zone": row["zone"],
            "declared_area_mu": Decimal(row["declared_area_mu"]),
            "declared_boundary": parse_json(row["declared_boundary_json"]),
            "surveyed_boundary": parse_json(row["surveyed_boundary_json"]),
            "source_revision": row["source_revision"],
        }

    def _shard_input(self, job: sqlite3.Row, shard: sqlite3.Row) -> dict[str, Any]:
        """重建分片领取时的输入载荷：清单子集、规则版本与上游成果。"""

        payload: dict[str, Any] = {
            "shard_key": shard["shard_key"],
            "kind": shard["kind"],
            "zone": shard["zone"],
            "rule": parse_json(job["rule_json"]),
        }
        if shard["kind"] in {"boundary-compare", "area-stats"}:
            payload["parcels"] = self._parcel_docs(job["job_id"], shard["zone"])
        if shard["kind"] in {"area-stats", "summary"}:
            rows = self.connection.execute(
                "SELECT d.depends_on,s.kind,s.state,s.output_sha256,s.result_json "
                "FROM survey_shard_deps d JOIN survey_shards s "
                "ON s.job_id=d.job_id AND s.shard_key=d.depends_on "
                "WHERE d.job_id=? AND d.shard_key=? ORDER BY d.depends_on",
                (job["job_id"], shard["shard_key"]),
            ).fetchall()
            payload["dependencies"] = [
                {
                    "shard_key": row["depends_on"],
                    "kind": row["kind"],
                    "state": row["state"],
                    "output_sha256": row["output_sha256"],
                    "result": None if row["result_json"] is None else parse_json(row["result_json"]),
                }
                for row in rows
            ]
        return payload

    def claim_shard(
        self, worker_id: str, job_id: str | None = None, lease_seconds: int | None = None
    ) -> dict[str, Any] | None:
        """工作进程通过有期限租约领取一个可分片；过期租约可被接管。"""

        if not worker_id.strip():
            raise ValidationFailed("工作进程编号不能为空")
        now = self._now()
        if job_id is not None:
            job = self._job_row(job_id)
            if job["state"] == "cancelled":
                raise InvalidState("任务已取消：阻止新领取，已完成分片证据保留")
            if job["state"] == "completed":
                raise InvalidState("任务已完成，没有可领取的分片")
        with transaction(self.connection, immediate=True):
            # 租约多次过期且重试预算已用尽的分片，直接转入人工处置。
            exhausted = self.connection.execute(
                "SELECT s.shard_id,s.shard_key,s.job_id,s.lease_owner,s.attempts FROM survey_shards s "
                "JOIN survey_jobs j ON j.job_id=s.job_id "
                "WHERE s.state='leased' AND s.lease_expires_at<=? AND s.attempts>=j.max_attempts",
                (now,),
            ).fetchall()
            for row in exhausted:
                self.connection.execute(
                    "UPDATE survey_shards SET state='needs_review',lease_owner=NULL,lease_expires_at=NULL,"
                    "last_error=?,updated_at=? WHERE shard_id=? AND state='leased'",
                    ("租约过期且重试次数已用尽", now, row["shard_id"]),
                )
                self._audit(
                    "shard",
                    f"{row['job_id']}:{row['shard_key']}",
                    "shard.needs_review",
                    "system",
                    {"reason": "lease_exhausted", "attempts": row["attempts"], "last_owner": row["lease_owner"]},
                )
            sql = (
                "SELECT s.* FROM survey_shards s JOIN survey_jobs j ON j.job_id=s.job_id "
                "WHERE j.state='active' AND ("
                "(s.state='ready' AND s.available_at<=?) OR (s.state='leased' AND s.lease_expires_at<=?)"
                + (") AND s.job_id=?" if job_id is not None else ")")
                + " ORDER BY s.available_at,s.shard_id LIMIT 1"
            )
            params = (now, now, job_id) if job_id is not None else (now, now)
            shard = self.connection.execute(sql, params).fetchone()
            if shard is None:
                return None
            job = self._job_row(shard["job_id"])
            seconds = job["lease_seconds"] if lease_seconds is None else lease_seconds
            if not isinstance(seconds, int) or isinstance(seconds, bool) or seconds <= 0:
                raise ValidationFailed("租约时长必须是正整数秒")
            expires = utc_text(self.clock.now() + timedelta(seconds=seconds))
            takeover = shard["lease_owner"] is not None
            previous_owner = shard["lease_owner"]
            self.connection.execute(
                "UPDATE survey_shards SET state='leased',attempts=attempts+1,fencing_token=fencing_token+1,"
                "lease_owner=?,lease_expires_at=?,updated_at=? WHERE shard_id=?",
                (worker_id, expires, now, shard["shard_id"]),
            )
            leased = self.connection.execute(
                "SELECT * FROM survey_shards WHERE shard_id=?", (shard["shard_id"],)
            ).fetchone()
            payload = self._shard_input(job, leased)
            input_sha256 = content_digest([payload])
            self.connection.execute(
                "UPDATE survey_shards SET claimed_input_sha256=? WHERE shard_id=?",
                (input_sha256, shard["shard_id"]),
            )
            self._audit(
                "shard",
                f"{job['job_id']}:{shard['shard_key']}",
                "shard.leased",
                worker_id,
                {
                    "fencing_token": leased["fencing_token"],
                    "attempts": leased["attempts"],
                    "lease_expires_at": expires,
                    "takeover": takeover,
                    "previous_owner": previous_owner,
                    "input_sha256": input_sha256,
                },
            )
        return {
            "job_id": job["job_id"],
            "shard_id": shard["shard_id"],
            "shard_key": shard["shard_key"],
            "kind": shard["kind"],
            "zone": shard["zone"],
            "fencing_token": leased["fencing_token"],
            "attempts": leased["attempts"],
            "lease_expires_at": expires,
            "takeover": takeover,
            "input_sha256": input_sha256,
            "input": payload,
        }

    # ------------------------------------------------------------------
    # 分片完成：验证输入与规则未变，原子解锁后续分片
    # ------------------------------------------------------------------

    def _shard_with_job(self, shard_id: int) -> tuple[sqlite3.Row, sqlite3.Row]:
        shard = self.connection.execute(
            "SELECT * FROM survey_shards WHERE shard_id=?", (shard_id,)
        ).fetchone()
        if shard is None:
            raise NotFound(f"分片不存在: {shard_id}")
        return shard, self._job_row(shard["job_id"])

    def _check_result_consistency(
        self, shard: sqlite3.Row, payload: Mapping[str, Any], result: Mapping[str, Any]
    ) -> None:
        if shard["kind"] in {"boundary-compare", "area-stats"} and result["zone"] != shard["zone"]:
            raise ValidationFailed("结果片区与分片不一致")
        if shard["kind"] == "boundary-compare":
            expected = {item["parcel_id"] for item in payload["parcels"]}
            reported = {item["parcel_id"] for item in result["parcels"]}
            if reported != expected:
                raise ValidationFailed("结果地块集合与领取输入不一致")
        if shard["kind"] == "area-stats" and result["parcel_count"] != len(payload["parcels"]):
            raise ValidationFailed("结果地块数量与领取输入不一致")
        if shard["kind"] == "summary":
            expected = {
                item["result"]["zone"]
                for item in payload["dependencies"]
                if item["kind"] == "area-stats" and item["state"] == "succeeded" and item["result"] is not None
            }
            reported = {item["zone"] for item in result["zones"]}
            if reported != expected:
                raise ValidationFailed("汇总结果片区集合与上游成果不一致")

    def _unlock_dependents(self, job_id: str, shard_key: str, actor_id: str) -> list[str]:
        """在同一事务内把依赖已全部就绪的后续分片置为可领取。"""

        unlocked: list[str] = []
        dependents = self.connection.execute(
            "SELECT shard_key FROM survey_shard_deps WHERE job_id=? AND depends_on=? ORDER BY shard_key",
            (job_id, shard_key),
        ).fetchall()
        now = self._now()
        for row in dependents:
            unfinished = self.connection.execute(
                "SELECT count(*) FROM survey_shard_deps d JOIN survey_shards s "
                "ON s.job_id=d.job_id AND s.shard_key=d.depends_on "
                "WHERE d.job_id=? AND d.shard_key=? AND s.state NOT IN ('succeeded','skipped')",
                (job_id, row["shard_key"]),
            ).fetchone()[0]
            if unfinished:
                continue
            cursor = self.connection.execute(
                "UPDATE survey_shards SET state='ready',available_at=?,updated_at=? "
                "WHERE job_id=? AND shard_key=? AND state='blocked'",
                (now, now, job_id, row["shard_key"]),
            )
            if cursor.rowcount == 1:
                unlocked.append(row["shard_key"])
                self._audit(
                    "shard",
                    f"{job_id}:{row['shard_key']}",
                    "shard.unlocked",
                    actor_id,
                    {"unlocked_by": shard_key},
                )
        return unlocked

    def _maybe_complete_job(self, job: sqlite3.Row, actor_id: str) -> bool:
        remaining = self.connection.execute(
            "SELECT count(*) FROM survey_shards WHERE job_id=? AND state NOT IN ('succeeded','skipped')",
            (job["job_id"],),
        ).fetchone()[0]
        if remaining or job["state"] != "active":
            return False
        self.connection.execute(
            "UPDATE survey_jobs SET state='completed',completed_at=? WHERE job_id=? AND state='active'",
            (self._now(), job["job_id"]),
        )
        self._audit("job", job["job_id"], "job.completed", actor_id, {})
        return True

    def complete_shard(
        self, worker_id: str, shard_id: int, fencing_token: int, raw_result: Mapping[str, Any]
    ) -> dict[str, Any]:
        shard, job = self._shard_with_job(shard_id)
        try:
            result = validate_result(shard["kind"], raw_result)
        except ValidationError as exc:
            raise ValidationFailed(str(exc)) from exc
        output_sha256 = content_digest([result])
        # 幂等重放：同一租约令牌提交同一结果，直接返回已存证据。
        if shard["state"] == "succeeded" and shard["fencing_token"] == fencing_token:
            if shard["output_sha256"] != output_sha256:
                raise Conflict("同一租约令牌提交了不同结果")
            return {
                "shard_id": shard_id,
                "shard_key": shard["shard_key"],
                "state": "succeeded",
                "output_sha256": shard["output_sha256"],
                "unlocked": [],
                "job_state": self._job_row(job["job_id"])["state"],
                "replayed": True,
            }
        if shard["state"] != "leased":
            raise InvalidState("分片未由当前工作进程持有")
        if shard["fencing_token"] != fencing_token:
            raise Conflict("租约令牌已失效：分片已被其他进程接管，迟到结果不能覆盖新持有者")
        if shard["lease_owner"] != worker_id:
            raise InvalidState("分片未由当前工作进程持有")
        if shard["lease_expires_at"] <= self._now():
            raise InvalidState("租约已过期，分片可被其他进程接管")
        payload = self._shard_input(job, shard)
        self._check_result_consistency(shard, payload, result)
        current_sha256 = content_digest([payload])
        if current_sha256 != shard["claimed_input_sha256"]:
            # 输入清单或规则版本在计算期间发生变化：拒绝登记，重新排队按新输入计算。
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "UPDATE survey_shards SET state='ready',available_at=?,lease_owner=NULL,"
                    "lease_expires_at=NULL,claimed_input_sha256=NULL,updated_at=? WHERE shard_id=?",
                    (self._now(), self._now(), shard_id),
                )
                self._audit(
                    "shard",
                    f"{job['job_id']}:{shard['shard_key']}",
                    "shard.input_stale",
                    worker_id,
                    {"claimed_input_sha256": shard["claimed_input_sha256"], "current_input_sha256": current_sha256},
                )
            raise Conflict("输入清单或规则版本已变化，分片已按新输入重新排队")
        now = self._now()
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE survey_shards SET state='succeeded',result_json=?,output_sha256=?,"
                "completed_by=?,completed_at=?,lease_owner=NULL,lease_expires_at=NULL,updated_at=? "
                "WHERE shard_id=? AND state='leased' AND lease_owner=? AND fencing_token=?",
                (
                    canonical_json(result),
                    output_sha256,
                    worker_id,
                    now,
                    now,
                    shard_id,
                    worker_id,
                    fencing_token,
                ),
            )
            if cursor.rowcount != 1:
                raise InvalidState("分片租约状态已变化")
            unlocked = self._unlock_dependents(job["job_id"], shard["shard_key"], worker_id)
            self._maybe_complete_job(job, worker_id)
            self._audit(
                "shard",
                f"{job['job_id']}:{shard['shard_key']}",
                "shard.succeeded",
                worker_id,
                {
                    "fencing_token": fencing_token,
                    "input_sha256": shard["claimed_input_sha256"],
                    "output_sha256": output_sha256,
                    "unlocked": unlocked,
                },
            )
        return {
            "shard_id": shard_id,
            "shard_key": shard["shard_key"],
            "state": "succeeded",
            "output_sha256": output_sha256,
            "unlocked": unlocked,
            "job_state": self._job_row(job["job_id"])["state"],
            "replayed": False,
        }

    # ------------------------------------------------------------------
    # 失败重试与人工处置
    # ------------------------------------------------------------------

    def fail_shard(
        self, worker_id: str, shard_id: int, fencing_token: int, error: str
    ) -> dict[str, Any]:
        if not error.strip():
            raise ValidationFailed("失败原因不能为空")
        shard, job = self._shard_with_job(shard_id)
        if shard["state"] != "leased":
            raise InvalidState("分片未由当前工作进程持有")
        if shard["fencing_token"] != fencing_token:
            raise Conflict("租约令牌已失效：分片已被其他进程接管")
        if shard["lease_owner"] != worker_id:
            raise InvalidState("分片未由当前工作进程持有")
        if shard["lease_expires_at"] <= self._now():
            raise InvalidState("租约已过期，分片可被其他进程接管")
        now = self._now()
        with transaction(self.connection, immediate=True):
            if shard["attempts"] < job["max_attempts"]:
                available = utc_text(
                    self.clock.now() + timedelta(seconds=job["retry_delay_seconds"] * shard["attempts"])
                )
                self.connection.execute(
                    "UPDATE survey_shards SET state='ready',available_at=?,lease_owner=NULL,"
                    "lease_expires_at=NULL,claimed_input_sha256=NULL,last_error=?,updated_at=? "
                    "WHERE shard_id=?",
                    (available, error[:1000], now, shard_id),
                )
                self._audit(
                    "shard",
                    f"{job['job_id']}:{shard['shard_key']}",
                    "shard.failed",
                    worker_id,
                    {"attempts": shard["attempts"], "will_retry": True, "available_at": available, "error": error[:1000]},
                )
                return {
                    "shard_id": shard_id,
                    "shard_key": shard["shard_key"],
                    "state": "ready",
                    "attempts": shard["attempts"],
                    "available_at": available,
                }
            self.connection.execute(
                "UPDATE survey_shards SET state='needs_review',lease_owner=NULL,lease_expires_at=NULL,"
                "claimed_input_sha256=NULL,last_error=?,updated_at=? WHERE shard_id=?",
                (error[:1000], now, shard_id),
            )
            self._audit(
                "shard",
                f"{job['job_id']}:{shard['shard_key']}",
                "shard.needs_review",
                worker_id,
                {"attempts": shard["attempts"], "max_attempts": job["max_attempts"], "error": error[:1000]},
            )
        return {
            "shard_id": shard_id,
            "shard_key": shard["shard_key"],
            "state": "needs_review",
            "attempts": shard["attempts"],
        }

    def resolve_shard(self, actor_id: str, shard_id: int, action: str, note: str) -> dict[str, Any]:
        """人工处置：重试（重置重试预算）或跳过（视为已满足并解锁下游）。"""

        self._require(actor_id, "shard.resolve")
        if action not in {"retry", "skip"}:
            raise ValidationFailed("处置动作必须是 retry 或 skip")
        if not note.strip():
            raise ValidationFailed("处置说明不能为空")
        shard, job = self._shard_with_job(shard_id)
        if shard["state"] != "needs_review":
            raise InvalidState("只有进入人工处置的分片可以处理")
        now = self._now()
        unlocked: list[str] = []
        with transaction(self.connection, immediate=True):
            if action == "retry":
                self.connection.execute(
                    "UPDATE survey_shards SET state='ready',attempts=0,available_at=?,lease_owner=NULL,"
                    "lease_expires_at=NULL,claimed_input_sha256=NULL,updated_at=? WHERE shard_id=?",
                    (now, now, shard_id),
                )
            else:
                skip_doc = {"skipped": True, "note": note.strip(), "resolved_by": actor_id, "resolved_at": now}
                self.connection.execute(
                    "UPDATE survey_shards SET state='skipped',result_json=?,output_sha256=?,completed_by=?,"
                    "completed_at=?,lease_owner=NULL,lease_expires_at=NULL,claimed_input_sha256=NULL,updated_at=? "
                    "WHERE shard_id=?",
                    (canonical_json(skip_doc), content_digest([skip_doc]), actor_id, now, now, shard_id),
                )
                unlocked = self._unlock_dependents(job["job_id"], shard["shard_key"], actor_id)
                self._maybe_complete_job(job, actor_id)
            self._audit(
                "shard",
                f"{job['job_id']}:{shard['shard_key']}",
                "shard.resolved",
                actor_id,
                {"action": action, "note": note.strip(), "unlocked": unlocked},
            )
        return {
            "shard_id": shard_id,
            "shard_key": shard["shard_key"],
            "state": "ready" if action == "retry" else "skipped",
            "unlocked": unlocked,
        }

    # ------------------------------------------------------------------
    # 取消与状态重建
    # ------------------------------------------------------------------

    def cancel_job(self, actor_id: str, job_id: str, reason: str) -> dict[str, Any]:
        """取消任务：阻止新领取，但保留已完成分片的全部证据。"""

        self._require(actor_id, "job.cancel")
        if not reason.strip():
            raise ValidationFailed("取消原因不能为空")
        job = self._job_row(job_id)
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE survey_jobs SET state='cancelled',cancelled_by=?,cancelled_at=?,cancel_reason=? "
                "WHERE job_id=? AND state='active'",
                (actor_id, self._now(), reason.strip(), job_id),
            )
            if cursor.rowcount != 1:
                raise InvalidState("任务已完成或已取消")
            succeeded = self.connection.execute(
                "SELECT count(*) FROM survey_shards WHERE job_id=? AND state='succeeded'", (job_id,)
            ).fetchone()[0]
            self._audit(
                "job",
                job_id,
                "job.cancelled",
                actor_id,
                {"reason": reason.strip(), "preserved_succeeded_shards": succeeded},
            )
        return {"job_id": job_id, "state": "cancelled", "preserved_succeeded_shards": succeeded}

    def _derive_shard_status(
        self, job: sqlite3.Row, shard: sqlite3.Row, dependencies: list[dict[str, str]], now: str
    ) -> dict[str, Any]:
        """从存储列和当前时间派生分片的有效状态，并说明状态来源。"""

        stored = shard["state"]
        source: dict[str, Any]
        if stored == "leased":
            source = {
                "basis": "lease_columns",
                "lease_owner": shard["lease_owner"],
                "lease_expires_at": shard["lease_expires_at"],
                "now": now,
            }
            if shard["lease_expires_at"] <= now:
                effective = "expired"
                source["detail"] = "租约已过期，其他进程可接管该分片"
            else:
                effective = "leased"
                source["detail"] = "租约有效，由持有进程计算中"
        elif stored == "ready":
            if shard["available_at"] > now:
                effective = "retry_waiting"
                source = {
                    "basis": "retry_schedule",
                    "available_at": shard["available_at"],
                    "attempts": shard["attempts"],
                    "last_error": shard["last_error"],
                    "detail": "失败退避中，到达可用时间后才能再次领取",
                }
            else:
                effective = "ready"
                source = {"basis": "job_queue", "available_at": shard["available_at"], "detail": "依赖已就绪，等待领取"}
        elif stored == "blocked":
            unfinished = [item["depends_on"] for item in dependencies if item["state"] not in TERMINAL_SHARD_STATES]
            effective = "blocked"
            source = {
                "basis": "dependency_table",
                "dependencies": [item["depends_on"] for item in dependencies],
                "unfinished_dependencies": unfinished,
                "detail": "等待上游分片完成",
            }
        elif stored == "succeeded":
            effective = "succeeded"
            source = {
                "basis": "output_row",
                "output_sha256": shard["output_sha256"],
                "completed_by": shard["completed_by"],
                "completed_at": shard["completed_at"],
                "detail": "成果已登记，输出校验值可复核",
            }
        elif stored == "needs_review":
            effective = "needs_review"
            source = {
                "basis": "retry_policy",
                "attempts": shard["attempts"],
                "max_attempts": job["max_attempts"],
                "last_error": shard["last_error"],
                "detail": "重试次数已用尽，等待人工处置",
            }
        else:
            effective = "skipped"
            note = None
            if shard["result_json"] is not None:
                note = parse_json(shard["result_json"]).get("note")
            source = {
                "basis": "manual_resolution",
                "resolved_by": shard["completed_by"],
                "resolved_at": shard["completed_at"],
                "note": note,
                "detail": "人工处置为跳过，下游按已满足处理",
            }
        input_stale: bool | None = None
        if stored in {"leased", "succeeded"} and shard["claimed_input_sha256"] is not None:
            current = content_digest([self._shard_input(job, shard)])
            input_stale = current != shard["claimed_input_sha256"]
        return {
            "shard_id": shard["shard_id"],
            "shard_key": shard["shard_key"],
            "kind": shard["kind"],
            "zone": shard["zone"],
            "stored_state": stored,
            "effective_state": effective,
            "attempts": shard["attempts"],
            "fencing_token": shard["fencing_token"],
            "claimed_input_sha256": shard["claimed_input_sha256"],
            "output_sha256": shard["output_sha256"],
            "input_stale": input_stale,
            "source": source,
        }

    def job_status(self, actor_id: str, job_id: str) -> dict[str, Any]:
        """重建任务各分片状态；全部信息来自 SQLite，进程重启后结果一致。"""

        self._require(actor_id, "job.read")
        job = self._job_row(job_id)
        now = self._now()
        shards = self.connection.execute(
            "SELECT * FROM survey_shards WHERE job_id=? ORDER BY shard_id", (job_id,)
        ).fetchall()
        dep_rows = self.connection.execute(
            "SELECT d.shard_key,d.depends_on,s.state FROM survey_shard_deps d "
            "JOIN survey_shards s ON s.job_id=d.job_id AND s.shard_key=d.depends_on WHERE d.job_id=?",
            (job_id,),
        ).fetchall()
        deps_by_shard: dict[str, list[dict[str, str]]] = {}
        for row in dep_rows:
            deps_by_shard.setdefault(row["shard_key"], []).append(
                {"depends_on": row["depends_on"], "state": row["state"]}
            )
        statuses = [
            self._derive_shard_status(job, shard, deps_by_shard.get(shard["shard_key"], []), now)
            for shard in shards
        ]
        progress: dict[str, int] = {}
        for status in statuses:
            progress[status["effective_state"]] = progress.get(status["effective_state"], 0) + 1
        notes: list[str] = []
        if job["state"] == "cancelled":
            notes.append("任务已取消：阻止新领取，已完成分片证据保留")
        if any(item["effective_state"] == "expired" for item in statuses):
            notes.append("存在已过期租约，对应分片可被其他进程接管")
        if any(item["input_stale"] for item in statuses):
            notes.append("存在基于旧输入清单或旧规则版本的证据，可对照输入修订号复核")
        return {
            "job_id": job_id,
            "title": job["title"],
            "state": job["state"],
            "claimable": job["state"] == "active",
            "manifest_sha256": job["manifest_sha256"],
            "input_revision": job["input_revision"],
            "rule_version": job["rule_version"],
            "rule_sha256": job["rule_sha256"],
            "rule_revision": job["rule_revision"],
            "parcel_count": job["parcel_count"],
            "progress": progress,
            "shards": statuses,
            "notes": notes,
            "derived_at": now,
        }

    def audit_trail(self, actor_id: str, job_id: str) -> dict[str, Any]:
        """返回任务的审计事件，并校验全表哈希链的完整性。"""

        self._require(actor_id, "audit.read")
        self._job_row(job_id)
        rows = self.connection.execute("SELECT * FROM survey_audit_events ORDER BY event_id").fetchall()
        previous_hash = "0" * 64
        valid = True
        for row in rows:
            body = {
                "entity_type": row["entity_type"],
                "entity_id": row["entity_id"],
                "event_type": row["event_type"],
                "actor_id": row["actor_id"],
                "payload": parse_json(row["payload_json"]),
                "created_at": row["created_at"],
                "previous_hash": row["previous_hash"],
            }
            calculated = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
            if row["previous_hash"] != previous_hash or row["event_hash"] != calculated:
                valid = False
                break
            previous_hash = row["event_hash"]
        events = [
            {
                "event_id": row["event_id"],
                "entity_type": row["entity_type"],
                "entity_id": row["entity_id"],
                "event_type": row["event_type"],
                "actor_id": row["actor_id"],
                "payload": parse_json(row["payload_json"]),
                "created_at": row["created_at"],
            }
            for row in rows
            if row["entity_id"] == job_id or row["entity_id"].startswith(f"{job_id}:")
        ]
        return {"valid": valid, "event_count": len(events), "events": events}
