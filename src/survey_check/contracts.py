"""测绘校核任务与分片结果的严格数据契约。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence


class ValidationError(ValueError):
    """输入不能满足领域契约。"""


IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$")
SHARD_KINDS = {"boundary-compare", "area-stats", "summary"}
CONCLUSIONS = {"pass", "fail", "incomplete"}


def _require_mapping(value: object, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValidationError(f"{path} 必须是对象")
    return value


def _require_sequence(value: object, path: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValidationError(f"{path} 必须是数组")
    return value


def _required_text(value: object, path: str, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{path} 必须为非空字符串")
    result = value.strip()
    if len(result) > maximum:
        raise ValidationError(f"{path} 不能超过 {maximum} 个字符")
    return result


def _identifier(value: object, path: str) -> str:
    result = _required_text(value, path, 64)
    if not IDENTIFIER.fullmatch(result):
        raise ValidationError(f"{path} 格式不正确")
    return result


def _decimal(value: object, path: str, *, minimum: Decimal | None = None) -> Decimal:
    if isinstance(value, bool):
        raise ValidationError(f"{path} 必须是数值")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValidationError(f"{path} 必须是十进制数值") from exc
    if not result.is_finite():
        raise ValidationError(f"{path} 必须是有限数值")
    if minimum is not None and result < minimum:
        raise ValidationError(f"{path} 不能小于 {minimum}")
    return result


def _integer(value: object, path: str, *, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{path} 必须是整数")
    if not minimum <= value <= maximum:
        raise ValidationError(f"{path} 必须在 {minimum} 到 {maximum} 之间")
    return value


def _boolean(value: object, path: str) -> bool:
    if not isinstance(value, bool):
        raise ValidationError(f"{path} 必须是布尔值")
    return value


def _boundary(value: object, path: str) -> tuple[tuple[Decimal, Decimal], ...]:
    points = _require_sequence(value, path)
    if len(points) < 3:
        raise ValidationError(f"{path} 至少需要 3 个顶点")
    vertices: list[tuple[Decimal, Decimal]] = []
    for index, point in enumerate(points):
        pair = _require_sequence(point, f"{path}[{index}]")
        if len(pair) != 2:
            raise ValidationError(f"{path}[{index}] 必须是 [x, y] 坐标对")
        vertices.append((
            _decimal(pair[0], f"{path}[{index}][0]"),
            _decimal(pair[1], f"{path}[{index}][1]"),
        ))
    return tuple(vertices)


@dataclass(frozen=True, slots=True)
class Rule:
    """一次测绘校核使用的规则版本与执行策略。"""

    rule_version: str
    boundary_tolerance_m: Decimal
    area_tolerance_percent: Decimal
    lease_seconds: int
    max_attempts: int
    retry_delay_seconds: int

    @classmethod
    def from_dict(cls, raw: object) -> "Rule":
        data = _require_mapping(raw, "rule")
        return cls(
            rule_version=_identifier(data.get("rule_version"), "rule.rule_version"),
            boundary_tolerance_m=_decimal(
                data.get("boundary_tolerance_m"), "rule.boundary_tolerance_m", minimum=Decimal(0)
            ),
            area_tolerance_percent=_decimal(
                data.get("area_tolerance_percent"), "rule.area_tolerance_percent", minimum=Decimal(0)
            ),
            lease_seconds=_integer(data.get("lease_seconds"), "rule.lease_seconds", minimum=1, maximum=86400),
            max_attempts=_integer(data.get("max_attempts"), "rule.max_attempts", minimum=1, maximum=20),
            retry_delay_seconds=_integer(
                data.get("retry_delay_seconds"), "rule.retry_delay_seconds", minimum=0, maximum=86400
            ),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "rule_version": self.rule_version,
            "boundary_tolerance_m": self.boundary_tolerance_m,
            "area_tolerance_percent": self.area_tolerance_percent,
            "lease_seconds": self.lease_seconds,
            "max_attempts": self.max_attempts,
            "retry_delay_seconds": self.retry_delay_seconds,
        }


@dataclass(frozen=True, slots=True)
class ParcelEntry:
    """输入清单中的一条地块测绘登记。"""

    parcel_id: str
    zone: str
    declared_area_mu: Decimal
    declared_boundary: tuple[tuple[Decimal, Decimal], ...]
    surveyed_boundary: tuple[tuple[Decimal, Decimal], ...]
    source_revision: str

    @classmethod
    def from_dict(cls, raw: object, path: str = "parcel") -> "ParcelEntry":
        data = _require_mapping(raw, path)
        return cls(
            parcel_id=_identifier(data.get("parcel_id"), f"{path}.parcel_id"),
            zone=_identifier(data.get("zone"), f"{path}.zone"),
            declared_area_mu=_decimal(
                data.get("declared_area_mu"), f"{path}.declared_area_mu", minimum=Decimal("0.0001")
            ),
            declared_boundary=_boundary(data.get("declared_boundary"), f"{path}.declared_boundary"),
            surveyed_boundary=_boundary(data.get("surveyed_boundary"), f"{path}.surveyed_boundary"),
            source_revision=_identifier(data.get("source_revision"), f"{path}.source_revision"),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "parcel_id": self.parcel_id,
            "zone": self.zone,
            "declared_area_mu": self.declared_area_mu,
            "declared_boundary": [[x, y] for x, y in self.declared_boundary],
            "surveyed_boundary": [[x, y] for x, y in self.surveyed_boundary],
            "source_revision": self.source_revision,
        }


@dataclass(frozen=True, slots=True)
class ShardSpec:
    """校核任务中一个分片的声明与其依赖。"""

    shard_key: str
    kind: str
    zone: str | None
    depends_on: tuple[str, ...]

    @classmethod
    def from_dict(cls, raw: object, path: str = "shard") -> "ShardSpec":
        data = _require_mapping(raw, path)
        kind = _required_text(data.get("kind"), f"{path}.kind")
        if kind not in SHARD_KINDS:
            raise ValidationError(f"{path}.kind 必须是 {sorted(SHARD_KINDS)} 之一")
        zone = data.get("zone")
        if kind == "summary":
            if zone is not None:
                raise ValidationError(f"{path}.zone 对 summary 分片必须为空")
            zone_value = None
        else:
            zone_value = _identifier(zone, f"{path}.zone")
        depends_on = tuple(
            _identifier(item, f"{path}.depends_on[{index}]")
            for index, item in enumerate(_require_sequence(data.get("depends_on", []), f"{path}.depends_on"))
        )
        if len(set(depends_on)) != len(depends_on):
            raise ValidationError(f"{path}.depends_on 不能重复")
        return cls(
            shard_key=_identifier(data.get("shard_key"), f"{path}.shard_key"),
            kind=kind,
            zone=zone_value,
            depends_on=depends_on,
        )


@dataclass(frozen=True, slots=True)
class JobDefinition:
    """一次测绘校核任务的完整定义：输入清单、规则版本与分片依赖。"""

    job_id: str
    title: str
    rule: Rule
    parcels: tuple[ParcelEntry, ...]
    shards: tuple[ShardSpec, ...]

    @classmethod
    def from_dict(cls, raw: object) -> "JobDefinition":
        data = _require_mapping(raw, "job")
        rule = Rule.from_dict(data.get("rule"))
        parcels = tuple(
            ParcelEntry.from_dict(item, f"job.parcels[{index}]")
            for index, item in enumerate(_require_sequence(data.get("parcels"), "job.parcels"))
        )
        if not parcels:
            raise ValidationError("job.parcels 不能为空")
        parcel_ids = [item.parcel_id for item in parcels]
        if len(set(parcel_ids)) != len(parcel_ids):
            raise ValidationError("job.parcels.parcel_id 不能重复")
        shards = tuple(
            ShardSpec.from_dict(item, f"job.shards[{index}]")
            for index, item in enumerate(_require_sequence(data.get("shards"), "job.shards"))
        )
        if not shards:
            raise ValidationError("job.shards 不能为空")
        shard_keys = [item.shard_key for item in shards]
        if len(set(shard_keys)) != len(shard_keys):
            raise ValidationError("job.shards.shard_key 不能重复")
        cls._check_dependencies(shards, {item.zone for item in parcels})
        return cls(
            job_id=_identifier(data.get("job_id"), "job.job_id"),
            title=_required_text(data.get("title"), "job.title"),
            rule=rule,
            parcels=parcels,
            shards=shards,
        )

    @staticmethod
    def _check_dependencies(shards: tuple[ShardSpec, ...], zones: set[str]) -> None:
        key_set = {item.shard_key for item in shards}
        for shard in shards:
            if shard.kind != "summary" and shard.zone not in zones:
                raise ValidationError(f"分片 {shard.shard_key} 引用了清单中不存在的片区 {shard.zone}")
            if shard.kind == "summary" and not shard.depends_on:
                raise ValidationError("summary 分片必须至少依赖一个上游分片")
            for dependency in shard.depends_on:
                if dependency == shard.shard_key:
                    raise ValidationError(f"分片 {shard.shard_key} 不能依赖自身")
                if dependency not in key_set:
                    raise ValidationError(f"分片 {shard.shard_key} 依赖了不存在的分片 {dependency}")
        # 深度优先检查依赖图无环，保证重启后调度可以终结。
        visiting: set[str] = set()
        visited: set[str] = set()
        by_key = {item.shard_key: item for item in shards}

        def visit(key: str, trail: tuple[str, ...]) -> None:
            if key in visited:
                return
            if key in visiting:
                raise ValidationError(f"分片依赖存在环: {' -> '.join(trail + (key,))}")
            visiting.add(key)
            for dependency in by_key[key].depends_on:
                visit(dependency, trail + (key,))
            visiting.discard(key)
            visited.add(key)

        for shard in shards:
            visit(shard.shard_key, ())


def _decimal_field(data: Mapping[str, Any], key: str, path: str) -> Decimal:
    if key not in data:
        raise ValidationError(f"{path}.{key} 缺失")
    return _decimal(data[key], f"{path}.{key}")


def _text_field(data: Mapping[str, Any], key: str, path: str) -> str:
    if key not in data:
        raise ValidationError(f"{path}.{key} 缺失")
    return _required_text(data[key], f"{path}.{key}")


def _bool_field(data: Mapping[str, Any], key: str, path: str) -> bool:
    if key not in data:
        raise ValidationError(f"{path}.{key} 缺失")
    return _boolean(data[key], f"{path}.{key}")


def _int_field(data: Mapping[str, Any], key: str, path: str) -> int:
    if key not in data:
        raise ValidationError(f"{path}.{key} 缺失")
    return _integer(data[key], f"{path}.{key}", minimum=0, maximum=10**9)


def _text_list(value: object, path: str) -> list[str]:
    return [
        _required_text(item, f"{path}[{index}]")
        for index, item in enumerate(_require_sequence(value, path))
    ]


def _require_keys(data: Mapping[str, Any], expected: set[str], path: str) -> None:
    missing = sorted(expected - set(data))
    extra = sorted(set(data) - expected)
    if missing or extra:
        raise ValidationError(f"{path} 字段不匹配：缺少 {missing}，多出 {extra}")


def _parcel_result(raw: object, path: str) -> dict[str, object]:
    data = _require_mapping(raw, path)
    _require_keys(data, {
        "parcel_id", "declared_area_sqm", "surveyed_area_sqm", "area_delta_sqm",
        "area_variance_percent", "max_deviation_m", "vertex_count_mismatch",
        "within_boundary_tolerance", "within_area_tolerance", "status",
    }, path)
    status = _text_field(data, "status", path)
    if status not in {"pass", "fail"}:
        raise ValidationError(f"{path}.status 必须是 pass 或 fail")
    return {
        "parcel_id": _identifier(data.get("parcel_id"), f"{path}.parcel_id"),
        "declared_area_sqm": _decimal_field(data, "declared_area_sqm", path),
        "surveyed_area_sqm": _decimal_field(data, "surveyed_area_sqm", path),
        "area_delta_sqm": _decimal_field(data, "area_delta_sqm", path),
        "area_variance_percent": _decimal_field(data, "area_variance_percent", path),
        "max_deviation_m": _decimal_field(data, "max_deviation_m", path),
        "vertex_count_mismatch": _bool_field(data, "vertex_count_mismatch", path),
        "within_boundary_tolerance": _bool_field(data, "within_boundary_tolerance", path),
        "within_area_tolerance": _bool_field(data, "within_area_tolerance", path),
        "status": status,
    }


def _conclusion(data: Mapping[str, Any], path: str) -> str:
    conclusion = _text_field(data, "conclusion", path)
    if conclusion not in CONCLUSIONS:
        raise ValidationError(f"{path}.conclusion 必须是 {sorted(CONCLUSIONS)} 之一")
    return conclusion


def _zone_result(raw: object, path: str) -> dict[str, object]:
    data = _require_mapping(raw, path)
    _require_keys(data, {
        "zone", "parcel_count", "declared_area_sqm", "surveyed_area_sqm",
        "area_variance_percent", "conclusion",
    }, path)
    return {
        "zone": _identifier(data.get("zone"), f"{path}.zone"),
        "parcel_count": _int_field(data, "parcel_count", path),
        "declared_area_sqm": _decimal_field(data, "declared_area_sqm", path),
        "surveyed_area_sqm": _decimal_field(data, "surveyed_area_sqm", path),
        "area_variance_percent": _decimal_field(data, "area_variance_percent", path),
        "conclusion": _conclusion(data, path),
    }


def validate_result(kind: str, raw: object) -> dict[str, object]:
    """校验并规范化工作进程提交的分片结果；返回的副本可安全地规范化存储。"""

    data = _require_mapping(raw, "result")
    if kind == "boundary-compare":
        _require_keys(data, {"algorithm_version", "zone", "parcels", "passed", "failed"}, "result")
        parcels = [
            _parcel_result(item, f"result.parcels[{index}]")
            for index, item in enumerate(_require_sequence(data.get("parcels"), "result.parcels"))
        ]
        if len({item["parcel_id"] for item in parcels}) != len(parcels):
            raise ValidationError("result.parcels.parcel_id 不能重复")
        return {
            "algorithm_version": _text_field(data, "algorithm_version", "result"),
            "zone": _identifier(data.get("zone"), "result.zone"),
            "parcels": parcels,
            "passed": _int_field(data, "passed", "result"),
            "failed": _int_field(data, "failed", "result"),
        }
    if kind == "area-stats":
        _require_keys(data, {
            "algorithm_version", "zone", "parcel_count", "declared_area_sqm", "surveyed_area_sqm",
            "area_delta_sqm", "area_variance_percent", "passed", "failed", "out_of_tolerance",
            "dependency_shards", "conclusion",
        }, "result")
        return {
            "algorithm_version": _text_field(data, "algorithm_version", "result"),
            "zone": _identifier(data.get("zone"), "result.zone"),
            "parcel_count": _int_field(data, "parcel_count", "result"),
            "declared_area_sqm": _decimal_field(data, "declared_area_sqm", "result"),
            "surveyed_area_sqm": _decimal_field(data, "surveyed_area_sqm", "result"),
            "area_delta_sqm": _decimal_field(data, "area_delta_sqm", "result"),
            "area_variance_percent": _decimal_field(data, "area_variance_percent", "result"),
            "passed": _int_field(data, "passed", "result"),
            "failed": _int_field(data, "failed", "result"),
            "out_of_tolerance": _text_list(data.get("out_of_tolerance"), "result.out_of_tolerance"),
            "dependency_shards": _text_list(data.get("dependency_shards"), "result.dependency_shards"),
            "conclusion": _conclusion(data, "result"),
        }
    if kind == "summary":
        _require_keys(data, {
            "algorithm_version", "zones", "parcel_count", "declared_area_sqm", "surveyed_area_sqm",
            "area_variance_percent", "skipped_dependencies", "conclusion",
        }, "result")
        zones = [
            _zone_result(item, f"result.zones[{index}]")
            for index, item in enumerate(_require_sequence(data.get("zones"), "result.zones"))
        ]
        if len({item["zone"] for item in zones}) != len(zones):
            raise ValidationError("result.zones.zone 不能重复")
        return {
            "algorithm_version": _text_field(data, "algorithm_version", "result"),
            "zones": zones,
            "parcel_count": _int_field(data, "parcel_count", "result"),
            "declared_area_sqm": _decimal_field(data, "declared_area_sqm", "result"),
            "surveyed_area_sqm": _decimal_field(data, "surveyed_area_sqm", "result"),
            "area_variance_percent": _decimal_field(data, "area_variance_percent", "result"),
            "skipped_dependencies": _text_list(data.get("skipped_dependencies"), "result.skipped_dependencies"),
            "conclusion": _conclusion(data, "result"),
        }
    raise ValidationError(f"未知分片类型: {kind}")
