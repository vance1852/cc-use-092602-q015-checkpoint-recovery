"""确定性的 JSON 序列化与内容摘要。"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable


class JsonDataError(ValueError):
    """JSON 文件缺失、损坏或不符合契约。"""


def load_json(path: Path) -> Any:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise JsonDataError(f"无法读取 {path}: {exc}") from exc

    def pairs_hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise JsonDataError(f"{path} 含重复键 {key}")
            result[key] = value
        return result

    try:
        return json.loads(text, parse_float=Decimal, object_pairs_hook=pairs_hook)
    except json.JSONDecodeError as exc:
        raise JsonDataError(f"{path} 不是有效 JSON: {exc.msg}") from exc


def _json_default(value: object) -> object:
    if isinstance(value, Decimal):
        return format(value, "f")
    raise TypeError(f"不能序列化 {type(value).__name__}")


def canonical_json(value: object) -> str:
    """生成跨进程、跨重启一致的紧凑 JSON 文本。"""

    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )


def content_digest(values: Iterable[object]) -> str:
    """按输入顺序计算规范化内容摘要。"""

    digest = hashlib.sha256()
    for value in values:
        digest.update(canonical_json(value).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()
