# __MARKER_0__
from __future__ import annotations

import socket
from typing import Any

from forge.datatypes import RecordBatch
from forge.execution.result import QueryResult

from .protocol import (
    MSG_QUERY, MSG_RESULT, MSG_SHUTDOWN,
    decode_batches, recv_message, send_message,
)


class Coordinator:
    def __init__(self) -> None:
        self._workers: list[tuple[str, int]] = []

    def add_worker(self, host: str, port: int) -> None:
        self._workers.append((host, port))

    def remove_worker(self, host: str, port: int) -> None:
        self._workers = [(h, p) for h, p in self._workers if (h, p) != (host, port)]

    @property
    def workers(self) -> list[tuple[str, int]]:
        return list(self._workers)

    def execute(self, sql: str) -> QueryResult:
        if not self._workers:
            raise RuntimeError("No workers registered")

        all_batches: list[RecordBatch] = []
        errors: list[str] = []

        for host, port in self._workers:
            try:
                batches = self._send_query(host, port, sql)
                all_batches.extend(batches)
            except Exception as e:
                errors.append(f"{host}:{port} - {e}")

        if errors and not all_batches:
            raise RuntimeError("All workers failed:\n" + "\n".join(errors))

        return QueryResult(all_batches)

    def broadcast(self, sql: str) -> list[QueryResult]:
        results: list[QueryResult] = []
        for host, port in self._workers:
            batches = self._send_query(host, port, sql)
            results.append(QueryResult(batches))
        return results

    def shutdown_workers(self) -> None:
        for host, port in self._workers:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((host, port))
                send_message(sock, MSG_SHUTDOWN, {})
                recv_message(sock)
                sock.close()
            except Exception:
                pass

    def _send_query(self, host: str, port: int, sql: str) -> list[RecordBatch]:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(30.0)
        sock.connect((host, port))
        try:
            send_message(sock, MSG_QUERY, {"sql": sql})
            response = recv_message(sock)
            if response.get("type") == MSG_RESULT:
                return decode_batches(response.get("batches", []))
            else:
                error = response.get("error", "Unknown error")
                raise RuntimeError(f"Worker error: {error}")
        finally:
            sock.close()
