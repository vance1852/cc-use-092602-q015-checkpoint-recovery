"""地块边界比对与面积统计的确定性计算。

所有计算只依赖 Decimal 与输入顺序，不使用随机数、浮点或挂钟，
同一分片输入在任何进程、任何重启之后都得到逐字节一致的结果。
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Iterable, Mapping, Sequence

from .contracts import RuleSet


ALGORITHM_VERSION = "survey-checkout/1"

Point = tuple[Decimal, Decimal]


def ring_area(ring: Sequence[Sequence[Decimal]]) -> Decimal:
    """鞋带公式求简单多边形面积（取绝对值）。"""

    total = Decimal(0)
    count = len(ring)
    for index in range(count):
        x1, y1 = Decimal(str(ring[index][0])), Decimal(str(ring[index][1]))
        x2, y2 = Decimal(str(ring[(index + 1) % count][0])), Decimal(str(ring[(index + 1) % count][1]))
        total += x1 * y2 - x2 * y1
    return abs(total) / 2


def _point_segment_distance(point: Point, start: Point, end: Point) -> Decimal:
    px, py = point
    x1, y1 = start
    x2, y2 = end
    dx = x2 - x1
    dy = y2 - y1
    length_squared = dx * dx + dy * dy
    if length_squared == 0:
        return ((px - x1) ** 2 + (py - y1) ** 2).sqrt()
    numerator = (px - x1) * dx + (py - y1) * dy
    ratio = numerator / length_squared
    if ratio < 0:
        ratio = Decimal(0)
    elif ratio > 1:
        ratio = Decimal(1)
    closest_x = x1 + ratio * dx
    closest_y = y1 + ratio * dy
    return ((px - closest_x) ** 2 + (py - closest_y) ** 2).sqrt()


def _ring_vertices(ring: Sequence[Sequence[Decimal]]) -> tuple[Point, ...]:
    return tuple((Decimal(str(p[0])), Decimal(str(p[1]))) for p in ring)


def _max_vertex_offset(source: tuple[Point, ...], target: tuple[Point, ...]) -> Decimal:
    """源环各顶点到目标环边界的最短距离中的最大值。"""

    worst = Decimal(0)
    for point in source:
        best: Decimal | None = None
        for index in range(len(target)):
            distance = _point_segment_distance(point, target[index], target[(index + 1) % len(target)])
            if best is None or distance < best:
                best = distance
        if best is not None and best > worst:
            worst = best
    return worst


def boundary_offset(ring_a: Sequence[Sequence[Decimal]], ring_b: Sequence[Sequence[Decimal]]) -> Decimal:
    """双向顶点-边界最大偏移（对称 Hausdorff 距离的顶点采样形式）。"""

    vertices_a = _ring_vertices(ring_a)
    vertices_b = _ring_vertices(ring_b)
    return max(_max_vertex_offset(vertices_a, vertices_b), _max_vertex_offset(vertices_b, vertices_a))


def check_parcel(parcel: Mapping[str, Any], rule_set: RuleSet) -> dict[str, Any]:
    surveyed_area = ring_area(parcel["surveyed_ring"])
    cadastral_area = ring_area(parcel["cadastral_ring"])
    area_difference = abs(surveyed_area - cadastral_area)
    if cadastral_area > 0:
        area_difference_ratio = area_difference / cadastral_area
    else:
        area_difference_ratio = Decimal(0) if area_difference == 0 else Decimal(1)
    offset = boundary_offset(parcel["surveyed_ring"], parcel["cadastral_ring"])
    checks = {
        "area_abs_ok": area_difference <= rule_set.area_tolerance_m2,
        "area_ratio_ok": area_difference_ratio <= rule_set.area_tolerance_ratio,
        "boundary_offset_ok": offset <= rule_set.boundary_offset_tolerance_m,
    }
    return {
        "parcel_id": parcel["parcel_id"],
        "surveyed_area_m2": surveyed_area,
        "cadastral_area_m2": cadastral_area,
        "area_difference_m2": area_difference,
        "area_difference_ratio": area_difference_ratio,
        "boundary_offset_m": offset,
        "checks": checks,
        "passed": all(checks.values()),
    }


def evaluate_shard(
    rule_set: RuleSet,
    parcels: Iterable[Mapping[str, Any]],
    *,
    input_sha256: str,
) -> dict[str, Any]:
    """对单个分片执行全部地块校核并汇总面积统计。"""

    parcel_results = [check_parcel(parcel, rule_set) for parcel in parcels]
    failed = [item["parcel_id"] for item in parcel_results if not item["passed"]]
    return {
        "algorithm_version": ALGORITHM_VERSION,
        "input_sha256": input_sha256,
        "rule_set": {
            "rule_set_id": rule_set.rule_set_id,
            "version": rule_set.version,
            "content_sha256": rule_set.content_sha256,
        },
        "parcel_count": len(parcel_results),
        "passed_count": len(parcel_results) - len(failed),
        "failed_parcels": failed,
        "verdict": "pass" if not failed else "fail",
        "totals": {
            "surveyed_area_m2": sum((item["surveyed_area_m2"] for item in parcel_results), Decimal(0)),
            "cadastral_area_m2": sum((item["cadastral_area_m2"] for item in parcel_results), Decimal(0)),
            "area_difference_m2": sum((item["area_difference_m2"] for item in parcel_results), Decimal(0)),
        },
        "parcels": parcel_results,
    }
