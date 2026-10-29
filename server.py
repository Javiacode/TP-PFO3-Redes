"""TCP task server backed by a bounded pool of worker threads."""

from __future__ import annotations

import argparse
import json
import logging
import math
import socket
import socketserver
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any

DEFAULT_MAX_REQUEST_BYTES = 64 * 1024
DEFAULT_CLIENT_TIMEOUT = 5.0
LOGGER = logging.getLogger("task_server")


def error_response(request_id: str | None, code: str, message: str) -> dict[str, Any]:
    return {
        "id": request_id,
        "ok": False,
        "error": {"code": code, "message": message},
    }


def encode_response(response: dict[str, Any]) -> bytes:
    return (
        json.dumps(response, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def execute_task(request: Any) -> dict[str, Any]:
    """Validate and execute one task. This function runs in the worker pool."""
    if not isinstance(request, dict):
        return error_response(None, "invalid_request", "The request must be a JSON object.")

    request_id = request.get("id")
    if not isinstance(request_id, str) or not request_id.strip() or len(request_id) > 128:
        return error_response(None, "invalid_request", "The request id must be a non-empty string of at most 128 characters.")

    task = request.get("task")
    if not isinstance(task, str) or not task:
        return error_response(request_id, "invalid_request", "The task field must be a non-empty string.")

    if "payload" not in request:
        return error_response(request_id, "invalid_request", "The payload field is required.")

    payload = request["payload"]
    if task == "echo":
        result = payload
    elif task == "uppercase":
        if not isinstance(payload, str):
            return error_response(request_id, "invalid_payload", "The uppercase task requires a string payload.")
        result = payload.upper()
    elif task == "sum":
        if not isinstance(payload, list) or any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or (isinstance(value, float) and not math.isfinite(value))
            for value in payload
        ):
            return error_response(request_id, "invalid_payload", "The sum task requires a list of finite numbers.")
        try:
            result = sum(payload)
        except OverflowError:
            return error_response(request_id, "invalid_payload", "The sum exceeds the supported numeric range.")
        if isinstance(result, float) and not math.isfinite(result):
            return error_response(request_id, "invalid_payload", "The sum exceeds the supported numeric range.")
    else:
        return error_response(request_id, "unknown_task", f"Unsupported task: {task}.")

    return {"id": request_id, "ok": True, "result": result}


class TaskRequestHandler(socketserver.StreamRequestHandler):
    """Read one newline-delimited JSON request and return its result."""

    def handle(self) -> None:
        self.request.settimeout(self.server.client_timeout)
        try:
            raw_request = self.rfile.readline(self.server.max_request_bytes + 1)
        except socket.timeout:
            self._write_response(error_response(None, "request_timeout", "Timed out while reading the request."))
            return

        if not raw_request:
            return
        if len(raw_request) > self.server.max_request_bytes:
            self._write_response(error_response(None, "request_too_large", "The request exceeds the maximum size."))
            return
        if not raw_request.endswith(b"\n"):
            self._write_response(error_response(None, "invalid_frame", "The request must end with a newline."))
            return

        try:
            request = json.loads(
                raw_request.decode("utf-8"),
                parse_constant=self._reject_non_finite_constant,
            )
        except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError):
            self._write_response(error_response(None, "invalid_json", "The request must contain valid UTF-8 JSON."))
            return

        request_id = request.get("id") if isinstance(request, dict) else None
        if not isinstance(request_id, str):
            request_id = None

        try:
            future = self.server.task_executor.submit(execute_task, request)
            response = future.result()
        except Exception:
            LOGGER.exception("Unexpected error while processing request %r", request_id)
            response = error_response(request_id, "internal_error", "The server could not process the request.")

        self._write_response(response)

    def _write_response(self, response: dict[str, Any]) -> None:
        try:
            encoded = encode_response(response)
        except (TypeError, ValueError, RecursionError):
            LOGGER.exception("Could not encode task response")
            encoded = encode_response(
                error_response(response.get("id"), "internal_error", "The server could not encode the result.")
            )
        try:
            self.wfile.write(encoded)
        except OSError:
            LOGGER.debug("Client disconnected before receiving the response")

    @staticmethod
    def _reject_non_finite_constant(value: str) -> None:
        raise ValueError(f"Non-finite JSON number is not allowed: {value}")


class BoundedThreadingTCPServer(socketserver.ThreadingTCPServer):
    """Threaded TCP server with bounded connections and a task worker pool."""

    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 64

    def __init__(
        self,
        server_address: tuple[str, int],
        *,
        max_workers: int,
        max_connections: int,
        max_request_bytes: int,
        client_timeout: float = DEFAULT_CLIENT_TIMEOUT,
    ) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be at least 1")
        if max_connections < 1:
            raise ValueError("max_connections must be at least 1")
        if max_request_bytes < 1:
            raise ValueError("max_request_bytes must be at least 1")

        self.max_request_bytes = max_request_bytes
        self.client_timeout = client_timeout
        self.task_executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="task-worker",
        )
        self._connection_slots = threading.BoundedSemaphore(max_connections)
        try:
            super().__init__(server_address, TaskRequestHandler)
        except Exception:
            self.task_executor.shutdown(wait=False, cancel_futures=True)
            raise

    def process_request(self, request: socket.socket, client_address: tuple[str, int]) -> None:
        if not self._connection_slots.acquire(blocking=False):
            try:
                request.settimeout(1.0)
                request.sendall(
                    encode_response(
                        error_response(None, "server_busy", "The server has reached its connection limit.")
                    )
                )
            except OSError:
                pass
            finally:
                self.shutdown_request(request)
            return

        try:
            super().process_request(request, client_address)
        except Exception:
            self._connection_slots.release()
            raise

    def process_request_thread(
        self,
        request: socket.socket,
        client_address: tuple[str, int],
    ) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._connection_slots.release()

    def server_close(self) -> None:
        super().server_close()
        self.task_executor.shutdown(wait=True, cancel_futures=True)


def create_server(
    host: str = "0.0.0.0",
    port: int = 5050,
    *,
    max_workers: int = 4,
    max_connections: int = 32,
    max_request_bytes: int = DEFAULT_MAX_REQUEST_BYTES,
) -> BoundedThreadingTCPServer:
    return BoundedThreadingTCPServer(
        (host, port),
        max_workers=max_workers,
        max_connections=max_connections,
        max_request_bytes=max_request_bytes,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the TCP task-processing server.")
    parser.add_argument("--host", default="0.0.0.0", help="Interface to listen on (default: 0.0.0.0).")
    parser.add_argument("--port", type=int, default=5050, help="TCP port to listen on (default: 5050).")
    parser.add_argument("--workers", type=int, default=4, help="Number of task worker threads (default: 4).")
    parser.add_argument(
        "--max-connections",
        type=int,
        default=32,
        help="Maximum simultaneous client connections (default: 32).",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        with create_server(
            args.host,
            args.port,
            max_workers=args.workers,
            max_connections=args.max_connections,
        ) as server:
            LOGGER.info("TCP task server listening on %s:%s", *server.server_address)
            LOGGER.info("Worker threads: %s", args.workers)
            server.serve_forever()
    except KeyboardInterrupt:
        LOGGER.info("Server stopped")


if __name__ == "__main__":
    main()
