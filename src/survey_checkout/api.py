"""无第三方依赖的 HTTP JSON 接口。"""

from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse, parse_qs

from .errors import ServiceError, ValidationFailed
from .service import SurveyCheckoutService
from .storage import connect


@dataclass(frozen=True, slots=True)
class Response:
    status: int
    body: Mapping[str, Any]


class JsonApplication:
    """将 HTTP 路由映射到领域服务，便于无网络单元测试。"""

    def __init__(self, service: SurveyCheckoutService) -> None:
        self.service = service

    @staticmethod
    def _actor(headers: Mapping[str, str]) -> str:
        actor = headers.get("x-actor-id", "").strip()
        if not actor:
            raise ValidationFailed("缺少 X-Actor-Id")
        return actor

    @staticmethod
    def _json(body: bytes) -> dict[str, Any]:
        if not body:
            return {}
        try:
            value = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValidationFailed("请求体必须是 UTF-8 JSON 对象") from exc
        if not isinstance(value, dict):
            raise ValidationFailed("请求体必须是 JSON 对象")
        return value

    def handle(
        self, method: str, target: str, headers: Mapping[str, str] | None = None, body: bytes = b""
    ) -> Response:
        normalized_headers = {key.lower(): value for key, value in (headers or {}).items()}
        parsed = urlparse(target)
        path = parsed.path.rstrip("/") or "/"
        parts = [part for part in path.split("/") if part]
        query = parse_qs(parsed.query)
        try:
            if method == "GET" and path == "/health":
                return Response(200, {"status": "ok"})
            payload = self._json(body) if method in {"POST", "PUT", "PATCH"} else {}
            if method == "POST" and path == "/users":
                result = self.service.create_user(payload["user_id"], payload["display_name"], payload["role"])
                return Response(201, result)
            if method == "POST" and path == "/rule-sets":
                result = self.service.publish_rule_set(self._actor(normalized_headers), payload)
                return Response(201, result)
            if method == "POST" and path == "/jobs":
                result = self.service.create_job(
                    self._actor(normalized_headers), payload["job_id"], payload["manifest"]
                )
                return Response(201, result)
            if method == "GET" and path == "/jobs":
                return Response(200, self.service.list_jobs(self._actor(normalized_headers)))
            if method == "GET" and len(parts) == 2 and parts[0] == "jobs":
                return Response(200, self.service.job_status(self._actor(normalized_headers), parts[1]))
            if method == "GET" and len(parts) == 3 and parts[0] == "jobs" and parts[2] == "audit":
                return Response(200, self.service.audit_trail(self._actor(normalized_headers), parts[1]))
            if method == "POST" and len(parts) == 3 and parts[0] == "jobs" and parts[2] == "cancel":
                result = self.service.cancel_job(self._actor(normalized_headers), parts[1], payload["reason"])
                return Response(200, result)
            if method == "POST" and path == "/shards/claim":
                result = self.service.claim_shard(
                    payload["worker_id"],
                    int(payload.get("lease_seconds", 60)),
                    query.get("job_id", [None])[0] or payload.get("job_id"),
                )
                return Response(200, {"claim": result})
            if method == "POST" and len(parts) == 4 and parts[0] == "jobs" and parts[3] == "complete":
                result = self.service.complete_shard(
                    payload["worker_id"], parts[1], parts[2],
                    payload["input_sha256"], payload["rule_set_sha256"],
                )
                return Response(200, result)
            if method == "POST" and len(parts) == 4 and parts[0] == "jobs" and parts[3] == "fail":
                retry = payload.get("retry_seconds")
                result = self.service.fail_shard(
                    payload["worker_id"], parts[1], parts[2], payload["error"],
                    None if retry is None else int(retry),
                )
                return Response(200, result)
            if method == "POST" and len(parts) == 4 and parts[0] == "jobs" and parts[3] == "manual":
                result = self.service.manual_decision(
                    self._actor(normalized_headers), parts[1], parts[2],
                    payload["decision"], payload.get("note", ""),
                )
                return Response(200, result)
            return Response(404, {"error": {"code": "route_not_found", "message": "接口不存在"}})
        except ServiceError as exc:
            return Response(exc.status, {"error": {"code": exc.code, "message": str(exc)}})
        except (KeyError, TypeError, ValueError) as exc:
            return Response(422, {"error": {"code": "invalid_request", "message": str(exc)}})


def make_handler(application: JsonApplication):
    class Handler(BaseHTTPRequestHandler):
        server_version = "SurveyCheckout/1"

        def do_GET(self) -> None:  # noqa: N802
            self._dispatch()

        def do_POST(self) -> None:  # noqa: N802
            self._dispatch()

        def _dispatch(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else b""
            response = application.handle(self.command, self.path, dict(self.headers.items()), body)
            encoded = json.dumps(response.body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            self.send_response(response.status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="启动自然资源测绘校核分片执行 HTTP 服务")
    parser.add_argument("--database", type=Path, default=Path("survey_checkout.sqlite3"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8083)
    args = parser.parse_args(argv)
    connection = connect(args.database)
    application = JsonApplication(SurveyCheckoutService(connection))
    server = ThreadingHTTPServer((args.host, args.port), make_handler(application))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
