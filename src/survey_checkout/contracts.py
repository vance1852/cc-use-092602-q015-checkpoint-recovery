"""测绘校核任务的输入清单与规则集契约。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from .errors import ValidationFailed


IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$")
MAX_RING_POINTS = 512
MAX_PARCELS_PER_SHARD = 256
MAX_SHARDS_PER_JOB = 512


def required_text(value: object, field: str, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationFailed(f"{field} 不能为空")
    result = value.strip()
    if len(result) > maximum:
        raise ValidationFailed(f"{field} 不能超过 {maximum} 个字符")
    return result


def identifier(value: object, field: str) -> str:
    result = required_text(value, field, 64)
    if not IDENTIFIER.fullmatch(result):
        raise ValidationFailed(f"{field} 格式不正确: {result}")
    return result


def decimal_value(
    value: object,
    field: str,
    *,
    minimum: Decimal | None = None,
    maximum: Decimal | None = None,
) -> Decimal:
    if isinstance(value, bool):
        raise ValidationFailed(f"{field} 必须是数值")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValidationFailed(f"{field} 必须是十进制数值") from exc
    if not result.is_finite():
        raise ValidationFailed(f"{field} 必须是有限数值")
    if minimum is not None and result < minimum:
        raise ValidationFailed(f"{field} 不能小于 {minimum}")
    if maximum is not None and result > maximum:
        raise ValidationFailed(f"{field} 不能大于 {maximum}")
    return result


def non_negative_int(value: object, field: str, *, maximum: int = 86400) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValidationFailed(f"{field} 必须是非负整数")
    if value > maximum:
        raise ValidationFailed(f"{field} 不能超过 {maximum}")
    return value


def _coordinate(value: object, field: str) -> Decimal:
    return decimal_value(value, field, minimum=Decimal("-100000000"), maximum=Decimal("100000000"))


def _ring(value: object, field: str) -> tuple[tuple[Decimal, Decimal], ...]:
    if not isinstance(value, (list, tuple)):
        raise ValidationFailed(f"{field} 必须是坐标数组")
    if not 3 <= len(value) <= MAX_RING_POINTS:
        raise ValidationFailed(f"{field} 顶点数必须在 3 到 {MAX_RING_POINTS} 之间")
    points: list[tuple[Decimal, Decimal]] = []
    for index, point in enumerate(value):
        if not isinstance(point, (list, tuple)) or len(point) != 2:
            raise ValidationFailed(f"{field}[{index}] 必须是 [x, y] 坐标对")
        points.append((
            _coordinate(point[0], f"{field}[{index}][0]"),
            _coordinate(point[1], f"{field}[{index}][1]"),
        ))
    if len(set(points)) != len(points):
        raise ValidationFailed(f"{field} 存在重复顶点")
    return tuple(points)


def parse_parcel(value: object, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValidationFailed(f"{field} 必须是对象")
    parcel_id = identifier(value.get("parcel_id"), f"{field}.parcel_id")
    ring_a = _ring(value.get("surveyed_ring"), f"{field}.surveyed_ring")
    ring_b = _ring(value.get("cadastral_ring"), f"{field}.cadastral_ring")
    return {
        "parcel_id": parcel_id,
        "surveyed_ring": [[x, y] for x, y in ring_a],
        "cadastral_ring": [[x, y] for x, y in ring_b],
    }


@dataclass(frozen=True, slots=True)
class RuleSet:
    rule_set_id: str
    version: int
    title: str
    area_tolerance_m2: Decimal
    area_tolerance_ratio: Decimal
    boundary_offset_tolerance_m: Decimal
    content_sha256: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "rule_set_id": self.rule_set_id,
            "version": self.version,
            "title": self.title,
            "area_tolerance_m2": self.area_tolerance_m2,
            "area_tolerance_ratio": self.area_tolerance_ratio,
            "boundary_offset_tolerance_m": self.boundary_offset_tolerance_m,
        }


def parse_rule_set(raw: object) -> RuleSet:
    if not isinstance(raw, Mapping):
        raise ValidationFailed("规则集必须是对象")
    rule_set_id = identifier(raw.get("rule_set_id"), "rule_set_id")
    version = raw.get("version")
    if isinstance(version, bool) or not isinstance(version, int) or version <= 0:
        raise ValidationFailed("version 必须是正整数")
    title = required_text(raw.get("title"), "title", 128)
    area_tolerance_m2 = decimal_value(
        raw.get("area_tolerance_m2"), "area_tolerance_m2", minimum=Decimal(0)
    )
    area_tolerance_ratio = decimal_value(
        raw.get("area_tolerance_ratio"),
        "area_tolerance_ratio",
        minimum=Decimal(0),
        maximum=Decimal(1),
    )
    boundary_offset_tolerance_m = decimal_value(
        raw.get("boundary_offset_tolerance_m"), "boundary_offset_tolerance_m", minimum=Decimal(0)
    )
    return RuleSet(
        rule_set_id=rule_set_id,
        version=version,
        title=title,
        area_tolerance_m2=area_tolerance_m2,
        area_tolerance_ratio=area_tolerance_ratio,
        boundary_offset_tolerance_m=boundary_offset_tolerance_m,
        content_sha256="",
    )


@dataclass(frozen=True, slots=True)
class ShardSpec:
    shard_id: str
    parcels: tuple[dict[str, Any], ...]
    depends_on: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Manifest:
    rule_set_id: str
    rule_set_version: int
    shards: tuple[ShardSpec, ...]
    max_attempts: int
    backoff_seconds: tuple[int, ...]


def parse_manifest(raw: object) -> Manifest:
    if not isinstance(raw, Mapping):
        raise ValidationFailed("输入清单必须是对象")
    rule_set_id = identifier(raw.get("rule_set_id"), "manifest.rule_set_id")
    rule_set_version = raw.get("rule_set_version")
    if isinstance(rule_set_version, bool) or not isinstance(rule_set_version, int) or rule_set_version <= 0:
        raise ValidationFailed("manifest.rule_set_version 必须是正整数")

    retry = raw.get("retry_policy", {})
    if not isinstance(retry, Mapping):
        raise ValidationFailed("retry_policy 必须是对象")
    max_attempts = retry.get("max_attempts", 3)
    if isinstance(max_attempts, bool) or not isinstance(max_attempts, int) or not 1 <= max_attempts <= 10:
        raise ValidationFailed("retry_policy.max_attempts 必须在 1 到 10 之间")
    raw_backoff = retry.get("backoff_seconds", [0])
    if not isinstance(raw_backoff, (list, tuple)) or not raw_backoff:
        raise ValidationFailed("retry_policy.backoff_seconds 必须是非空数组")
    backoff = tuple(
        non_negative_int(item, f"retry_policy.backoff_seconds[{index}]")
        for index, item in enumerate(raw_backoff)
    )

    raw_shards = raw.get("shards")
    if not isinstance(raw_shards, (list, tuple)) or not raw_shards:
        raise ValidationFailed("manifest.shards 必须是非空数组")
    if len(raw_shards) > MAX_SHARDS_PER_JOB:
        raise ValidationFailed(f"manifest.shards 不能超过 {MAX_SHARDS_PER_JOB} 个分片")

    shards: list[ShardSpec] = []
    seen: set[str] = set()
    for index, raw_shard in enumerate(raw_shards):
        field = f"manifest.shards[{index}]"
        if not isinstance(raw_shard, Mapping):
            raise ValidationFailed(f"{field} 必须是对象")
        shard_id = identifier(raw_shard.get("shard_id"), f"{field}.shard_id")
        if shard_id in seen:
            raise ValidationFailed(f"分片编号重复: {shard_id}")
        seen.add(shard_id)
        raw_parcels = raw_shard.get("parcels")
        if not isinstance(raw_parcels, (list, tuple)) or not raw_parcels:
            raise ValidationFailed(f"{field}.parcels 必须是非空数组")
        if len(raw_parcels) > MAX_PARCELS_PER_SHARD:
            raise ValidationFailed(f"{field}.parcels 不能超过 {MAX_PARCELS_PER_SHARD} 个地块")
        parcels: list[dict[str, Any]] = []
        parcel_ids: set[str] = set()
        for parcel_index, raw_parcel in enumerate(raw_parcels):
            parcel = parse_parcel(raw_parcel, f"{field}.parcels[{parcel_index}]")
            if parcel["parcel_id"] in parcel_ids:
                raise ValidationFailed(f"{field}.parcels 内地块编号重复: {parcel['parcel_id']}")
            parcel_ids.add(parcel["parcel_id"])
            parcels.append(parcel)
        raw_depends = raw_shard.get("depends_on", [])
        if not isinstance(raw_depends, (list, tuple)):
            raise ValidationFailed(f"{field}.depends_on 必须是数组")
        depends: list[str] = []
        for dep_index, dependency in enumerate(raw_depends):
            dependency_id = identifier(dependency, f"{field}.depends_on[{dep_index}]")
            if dependency_id == shard_id:
                raise ValidationFailed(f"分片不能依赖自身: {shard_id}")
            if dependency_id in depends:
                raise ValidationFailed(f"{field}.depends_on 存在重复依赖: {dependency_id}")
            depends.append(dependency_id)
        shards.append(ShardSpec(shard_id=shard_id, parcels=tuple(parcels), depends_on=tuple(depends)))

    known = {shard.shard_id for shard in shards}
    for shard in shards:
        for dependency in shard.depends_on:
            if dependency not in known:
                raise ValidationFailed(f"分片 {shard.shard_id} 依赖了不存在的分片: {dependency}")
    _assert_acyclic(shards)

    return Manifest(
        rule_set_id=rule_set_id,
        rule_set_version=rule_set_version,
        shards=tuple(shards),
        max_attempts=max_attempts,
        backoff_seconds=backoff,
    )


def _assert_acyclic(shards: list[ShardSpec]) -> None:
    """拓扑排序校验依赖图无环，保证后续分片总能被解锁。"""

    remaining = {shard.shard_id: set(shard.depends_on) for shard in shards}
    resolved: set[str] = set()
    while remaining:
        ready = [shard_id for shard_id, deps in remaining.items() if deps <= resolved]
        if not ready:
            cycle = "、".join(sorted(remaining))
            raise ValidationFailed(f"分片依赖存在环路: {cycle}")
        for shard_id in ready:
            resolved.add(shard_id)
            del remaining[shard_id]
