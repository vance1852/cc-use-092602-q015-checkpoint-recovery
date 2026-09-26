"""测绘校核分片的确定性计算：地块边界比对、面积统计与汇总。

工作进程领取分片后，使用领取载荷中的输入清单、规则版本和上游成果
调用这些纯函数；同一输入必然得到同一结果，便于重启后核对输出校验值。
"""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Mapping, Sequence


ALGORITHM_VERSION = "survey-check/1"

_AREA_QUANT = Decimal("0.0001")
_DEVIATION_QUANT = Decimal("0.001")
_PERCENT_QUANT = Decimal("0.0001")
_MU_TO_SQM_NUMERATOR = Decimal(2000)
_MU_TO_SQM_DENOMINATOR = Decimal(3)


def _decimal(value: object) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def mu_to_sqm(mu: Decimal) -> Decimal:
    """1 亩 = 2000/3 平方米，按 0.0001 平方米精度确定性地取整。"""

    return (mu * _MU_TO_SQM_NUMERATOR / _MU_TO_SQM_DENOMINATOR).quantize(
        _AREA_QUANT, rounding=ROUND_HALF_UP
    )


def _vertices(raw_boundary: Sequence[Sequence[object]]) -> list[tuple[Decimal, Decimal]]:
    return [(_decimal(point[0]), _decimal(point[1])) for point in raw_boundary]


def polygon_area_sqm(raw_boundary: Sequence[Sequence[object]]) -> Decimal:
    """鞋带公式计算平面多边形面积（平方米）。"""

    vertices = _vertices(raw_boundary)
    if len(vertices) < 3:
        raise ValueError("多边形至少需要 3 个顶点")
    total = Decimal(0)
    for index, (x1, y1) in enumerate(vertices):
        x2, y2 = vertices[(index + 1) % len(vertices)]
        total += x1 * y2 - x2 * y1
    return (abs(total) / 2).quantize(_AREA_QUANT, rounding=ROUND_HALF_UP)


def max_deviation_m(
    declared_boundary: Sequence[Sequence[object]],
    surveyed_boundary: Sequence[Sequence[object]],
) -> tuple[Decimal, bool]:
    """按顶点序号配对的最大偏距（米）；顶点数不一致时返回失配标记。"""

    declared = _vertices(declared_boundary)
    surveyed = _vertices(surveyed_boundary)
    mismatch = len(declared) != len(surveyed)
    worst = Decimal(0)
    for (x1, y1), (x2, y2) in zip(declared, surveyed):
        distance = ((x1 - x2) ** 2 + (y1 - y2) ** 2).sqrt()
        if distance > worst:
            worst = distance
    return worst.quantize(_DEVIATION_QUANT, rounding=ROUND_HALF_UP), mismatch


def compare_parcel(parcel: Mapping[str, object], rule: Mapping[str, object]) -> dict[str, object]:
    """对单块地块做边界比对与面积校核。"""

    declared_sqm = mu_to_sqm(_decimal(parcel["declared_area_mu"]))
    surveyed_sqm = polygon_area_sqm(parcel["surveyed_boundary"])  # type: ignore[arg-type]
    delta = (surveyed_sqm - declared_sqm).quantize(_AREA_QUANT, rounding=ROUND_HALF_UP)
    variance = (abs(delta) / declared_sqm * Decimal(100)).quantize(
        _PERCENT_QUANT, rounding=ROUND_HALF_UP
    )
    deviation, mismatch = max_deviation_m(
        parcel["declared_boundary"],  # type: ignore[arg-type]
        parcel["surveyed_boundary"],  # type: ignore[arg-type]
    )
    within_boundary = (not mismatch) and deviation <= _decimal(rule["boundary_tolerance_m"])
    within_area = variance <= _decimal(rule["area_tolerance_percent"])
    return {
        "parcel_id": str(parcel["parcel_id"]),
        "declared_area_sqm": declared_sqm,
        "surveyed_area_sqm": surveyed_sqm,
        "area_delta_sqm": delta,
        "area_variance_percent": variance,
        "max_deviation_m": deviation,
        "vertex_count_mismatch": mismatch,
        "within_boundary_tolerance": within_boundary,
        "within_area_tolerance": within_area,
        "status": "pass" if within_boundary and within_area else "fail",
    }


def boundary_compare(
    zone: str,
    parcels: Sequence[Mapping[str, object]],
    rule: Mapping[str, object],
) -> dict[str, object]:
    """边界比对分片：逐地块比对登记边界与实测边界。"""

    results = [compare_parcel(parcel, rule) for parcel in parcels]
    passed = sum(1 for item in results if item["status"] == "pass")
    return {
        "algorithm_version": ALGORITHM_VERSION,
        "zone": zone,
        "parcels": results,
        "passed": passed,
        "failed": len(results) - passed,
    }


def area_stats(
    zone: str,
    parcels: Sequence[Mapping[str, object]],
    dependencies: Sequence[Mapping[str, object]],
    rule: Mapping[str, object],
) -> dict[str, object]:
    """面积统计分片：汇总片区面积台账，并记录所依据的上游分片。"""

    comparisons = [compare_parcel(parcel, rule) for parcel in parcels]
    declared_total = sum((item["declared_area_sqm"] for item in comparisons), Decimal(0))
    surveyed_total = sum((item["surveyed_area_sqm"] for item in comparisons), Decimal(0))
    delta = (surveyed_total - declared_total).quantize(_AREA_QUANT, rounding=ROUND_HALF_UP)
    variance = Decimal(0) if declared_total == 0 else (
        abs(delta) / declared_total * Decimal(100)
    ).quantize(_PERCENT_QUANT, rounding=ROUND_HALF_UP)
    out_of_tolerance = sorted(item["parcel_id"] for item in comparisons if item["status"] == "fail")
    skipped = sorted(
        str(dep["shard_key"]) for dep in dependencies if dep["state"] != "succeeded"
    )
    if skipped:
        conclusion = "incomplete"
    else:
        conclusion = "fail" if out_of_tolerance else "pass"
    return {
        "algorithm_version": ALGORITHM_VERSION,
        "zone": zone,
        "parcel_count": len(parcels),
        "declared_area_sqm": declared_total.quantize(_AREA_QUANT, rounding=ROUND_HALF_UP),
        "surveyed_area_sqm": surveyed_total.quantize(_AREA_QUANT, rounding=ROUND_HALF_UP),
        "area_delta_sqm": delta,
        "area_variance_percent": variance,
        "passed": len(comparisons) - len(out_of_tolerance),
        "failed": len(out_of_tolerance),
        "out_of_tolerance": out_of_tolerance,
        "dependency_shards": sorted(str(dep["shard_key"]) for dep in dependencies),
        "conclusion": conclusion,
    }


def summary(
    dependencies: Sequence[Mapping[str, object]],
    rule: Mapping[str, object],
) -> dict[str, object]:
    """汇总分片：合并各片区面积统计成果，形成任务级结论。"""

    del rule  # 汇总只合并上游成果，不重新套用容差
    zones: list[dict[str, object]] = []
    skipped: list[str] = []
    for dep in sorted(dependencies, key=lambda item: str(item["shard_key"])):
        if dep["state"] != "succeeded" or dep["result"] is None:
            skipped.append(str(dep["shard_key"]))
            continue
        if dep["kind"] != "area-stats":
            continue
        result = dep["result"]
        zones.append({
            "zone": str(result["zone"]),
            "parcel_count": int(result["parcel_count"]),
            "declared_area_sqm": _decimal(result["declared_area_sqm"]),
            "surveyed_area_sqm": _decimal(result["surveyed_area_sqm"]),
            "area_variance_percent": _decimal(result["area_variance_percent"]),
            "conclusion": str(result["conclusion"]),
        })
    declared_total = sum((item["declared_area_sqm"] for item in zones), Decimal(0))
    surveyed_total = sum((item["surveyed_area_sqm"] for item in zones), Decimal(0))
    variance = Decimal(0) if declared_total == 0 else (
        abs(surveyed_total - declared_total) / declared_total * Decimal(100)
    ).quantize(_PERCENT_QUANT, rounding=ROUND_HALF_UP)
    if skipped:
        conclusion = "incomplete"
    elif any(item["conclusion"] != "pass" for item in zones):
        conclusion = "fail"
    else:
        conclusion = "pass"
    return {
        "algorithm_version": ALGORITHM_VERSION,
        "zones": zones,
        "parcel_count": sum(item["parcel_count"] for item in zones),
        "declared_area_sqm": declared_total.quantize(_AREA_QUANT, rounding=ROUND_HALF_UP),
        "surveyed_area_sqm": surveyed_total.quantize(_AREA_QUANT, rounding=ROUND_HALF_UP),
        "area_variance_percent": variance,
        "skipped_dependencies": skipped,
        "conclusion": conclusion,
    }


def run_shard(kind: str, claim_input: Mapping[str, object]) -> dict[str, object]:
    """按分片类型执行领取载荷，返回可提交的结果。"""

    rule = claim_input["rule"]
    if kind == "boundary-compare":
        return boundary_compare(
            str(claim_input["zone"]),
            claim_input["parcels"],  # type: ignore[arg-type]
            rule,  # type: ignore[arg-type]
        )
    if kind == "area-stats":
        return area_stats(
            str(claim_input["zone"]),
            claim_input["parcels"],  # type: ignore[arg-type]
            claim_input["dependencies"],  # type: ignore[arg-type]
            rule,  # type: ignore[arg-type]
        )
    if kind == "summary":
        return summary(
            claim_input["dependencies"],  # type: ignore[arg-type]
            rule,  # type: ignore[arg-type]
        )
    raise ValueError(f"未知分片类型: {kind}")
