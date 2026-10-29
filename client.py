"""Command-line client for the TCP task server."""

from __future__ import annotations

import argparse
import json
import socket
import sys
import uuid
from typing import Any

MAX_RESPONSE_BYTES = 64 * 1024


def send_task(
    host: str,
    port: int,
    task: str,
    payload: Any,
    *,
    timeout: float = 5.0,
    request_id: str | None = None,
) -> dict[str, Any]:
    task_id = request_id or uuid.uuid4().hex
    request = {"id": task_id, "task": task, "payload": payload}
    encoded_request = (
        json.dumps(request, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n"
    ).encode("utf-8")

    with socket.create_connection((host, port), timeout=timeout) as connection:
        connection.settimeout(timeout)
        connection.sendall(encoded_request)
        with connection.makefile("rb") as response_stream:
            raw_response = response_stream.readline(MAX_RESPONSE_BYTES + 1)

    if not raw_response:
        raise RuntimeError("The server closed the connection without a response.")
    if len(raw_response) > MAX_RESPONSE_BYTES:
        raise RuntimeError("The server response exceeded the maximum size.")
    if not raw_response.endswith(b"\n"):
        raise RuntimeError("The server returned an incomplete response frame.")

    try:
        response = json.loads(raw_response.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError("The server returned invalid JSON.") from error

    if not isinstance(response, dict):
        raise RuntimeError("The server returned an invalid response envelope.")
    if response.get("id") != task_id:
        error = response.get("error")
        is_unmatched_overload = (
            response.get("id") is None
            and response.get("ok") is False
            and isinstance(error, dict)
            and error.get("code") == "server_busy"
        )
        if not is_unmatched_overload:
            raise RuntimeError("The response id does not match the request id.")
    if not isinstance(response.get("ok"), bool):
        raise RuntimeError("The server returned an invalid response envelope.")
    return response


def _reject_non_finite_constant(value: str) -> None:
    raise ValueError(f"Non-finite JSON number is not allowed: {value}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Send one task to the TCP task server.")
    parser.add_argument("--host", default="127.0.0.1", help="Server address (default: 127.0.0.1).")
    parser.add_argument("--port", type=int, default=5050, help="Server TCP port (default: 5050).")
    parser.add_argument("--task", required=True, help="Task name: echo, uppercase, or sum.")
    parser.add_argument("--payload", required=True, help="Task payload as JSON, for example '[1, 2, 3]'.")
    parser.add_argument("--timeout", type=float, default=5.0, help="Socket timeout in seconds (default: 5).")
    args = parser.parse_args()

    try:
        payload = json.loads(args.payload, parse_constant=_reject_non_finite_constant)
        response = send_task(args.host, args.port, args.task, payload, timeout=args.timeout)
    except (ValueError, OSError, RuntimeError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1

    print(json.dumps(response, ensure_ascii=False, indent=2, allow_nan=False))
    return 0 if response["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
