from __future__ import annotations

import concurrent.futures
import json
import socket
import threading
import unittest

from client import send_task
from server import create_server, execute_task


class ExecuteTaskTests(unittest.TestCase):
    def test_echo_returns_payload(self) -> None:
        response = execute_task({"id": "a1", "task": "echo", "payload": {"value": 3}})
        self.assertEqual(response, {"id": "a1", "ok": True, "result": {"value": 3}})

    def test_uppercase_and_sum_tasks(self) -> None:
        self.assertEqual(
            execute_task({"id": "a2", "task": "uppercase", "payload": "hello"})["result"],
            "HELLO",
        )
        self.assertEqual(
            execute_task({"id": "a3", "task": "sum", "payload": [2, 3.5, 4]})["result"],
            9.5,
        )

    def test_invalid_payload_and_unknown_task_return_errors(self) -> None:
        invalid = execute_task({"id": "a4", "task": "sum", "payload": [1, True]})
        unknown = execute_task({"id": "a5", "task": "missing", "payload": None})
        self.assertEqual(invalid["error"]["code"], "invalid_payload")
        self.assertEqual(unknown["error"]["code"], "unknown_task")


class TCPServerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.server = create_server(
            "127.0.0.1",
            0,
            max_workers=2,
            max_connections=16,
            max_request_bytes=1024,
        )
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        self.host, self.port = self.server.server_address

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=2)

    def test_client_server_round_trip(self) -> None:
        response = send_task(self.host, self.port, "sum", [10, 20, 12], request_id="round-trip")
        self.assertEqual(response, {"id": "round-trip", "ok": True, "result": 42})

    def test_multiple_clients_are_served_concurrently(self) -> None:
        requests = [(f"task-{number}", [number, 1]) for number in range(12)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as callers:
            responses = list(
                callers.map(
                    lambda item: send_task(
                        self.host,
                        self.port,
                        "sum",
                        item[1],
                        request_id=item[0],
                    ),
                    requests,
                )
            )

        self.assertEqual([response["result"] for response in responses], list(range(1, 13)))
        self.assertEqual([response["id"] for response in responses], [item[0] for item in requests])

    def test_invalid_json_gets_protocol_error(self) -> None:
        with socket.create_connection((self.host, self.port), timeout=2) as connection:
            connection.sendall(b"{invalid json}\n")
            with connection.makefile("rb") as response_stream:
                response = json.loads(response_stream.readline())

        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], "invalid_json")

    def test_oversized_request_is_rejected(self) -> None:
        request = json.dumps({"id": "large", "task": "echo", "payload": "x" * 2048}).encode() + b"\n"
        with socket.create_connection((self.host, self.port), timeout=2) as connection:
            connection.sendall(request)
            with connection.makefile("rb") as response_stream:
                response = json.loads(response_stream.readline())

        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], "request_too_large")

    def test_non_finite_json_number_is_rejected(self) -> None:
        with socket.create_connection((self.host, self.port), timeout=2) as connection:
            connection.sendall(b'{"id":"bad-number","task":"echo","payload":NaN}\n')
            with connection.makefile("rb") as response_stream:
                response = json.loads(response_stream.readline())

        self.assertEqual(response["error"]["code"], "invalid_json")


if __name__ == "__main__":
    unittest.main()
