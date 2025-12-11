from __future__ import annotations

import socket
import threading

from forge.execution.context import ExecutionContext

from .protocol import (
    MSG_ACK, MSG_ERROR, MSG_QUERY, MSG_RESULT, MSG_SHUTDOWN,
    encode_batches, recv_message, send_message,
)


class Worker:
    def __init__(self, host: str = "localhost", port: int = 9876) -> None:
        self._host = host
        self._port = port
        self._ctx = ExecutionContext()
        self._running = False
        self._server_socket: socket.socket | None = None

    @property
    def context(self) -> ExecutionContext:
        return self._ctx

    def register_csv(self, name: str, path: str) -> None:
        self._ctx.register_csv(name, path)

    def register_memory(self, name: str, data: dict[str, list]) -> None:
        self._ctx.register_memory(name, data)

    def start(self) -> None:
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self._host, self._port))
        self._server_socket.listen(5)
        self._running = True
        print(f"Worker listening on {self._host}:{self._port}")

        while self._running:
            try:
                self._server_socket.settimeout(1.0)
                try:
                    conn, addr = self._server_socket.accept()
                except socket.timeout:
                    continue
                self._handle_connection(conn)
            except OSError:
                break

    def start_background(self) -> threading.Thread:
        t = threading.Thread(target=self.start, daemon=True)
        t.start()
        return t

    def stop(self) -> None:
        self._running = False
        if self._server_socket:
            self._server_socket.close()

    def _handle_connection(self, conn: socket.socket) -> None:
        try:
            msg = recv_message(conn)
            msg_type = msg.get("type")

            if msg_type == MSG_QUERY:
                sql = msg.get("sql", "")
                try:
                    result = self._ctx.sql(sql)
                    batches_data = encode_batches(result.batches)
                    send_message(conn, MSG_RESULT, {"batches": batches_data})
                except Exception as e:
                    send_message(conn, MSG_ERROR, {"error": str(e)})

            elif msg_type == MSG_SHUTDOWN:
                send_message(conn, MSG_ACK, {})
                self.stop()
            else:
                send_message(conn, MSG_ERROR, {"error": f"Unknown message type: {msg_type}"})
        finally:
            conn.close()
