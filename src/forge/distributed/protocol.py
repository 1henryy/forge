from __future__ import annotations

import json
import struct
import socket
from typing import Any

MSG_QUERY = "query"
MSG_RESULT = "result"
MSG_ERROR = "error"
MSG_SHUTDOWN = "shutdown"
MSG_ACK = "ack"


def send_message(sock: socket.socket, msg_type: str, payload: dict[str, Any]) -> None:
    data = json.dumps({"type": msg_type, **payload}).encode("utf-8")
    header = struct.pack("!I", len(data))
    sock.sendall(header + data)


def recv_message(sock: socket.socket) -> dict[str, Any]:
    header = _recv_exact(sock, 4)
    if not header:
        raise ConnectionError("Connection closed")
    length = struct.unpack("!I", header)[0]
    data = _recv_exact(sock, length)
    return json.loads(data.decode("utf-8"))


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    parts: list[bytes] = []
    remaining = n
    while remaining > 0:
        chunk = sock.recv(min(remaining, 65536))
        if not chunk:
            raise ConnectionError("Connection closed unexpectedly")
        parts.append(chunk)
        remaining -= len(chunk)
    return b"".join(parts)


def encode_batches(batches: list) -> list[dict]:
    result = []
    for batch in batches:
        arrow_batch = batch.to_arrow()
        rows: dict[str, list] = {}
        for i, name in enumerate(arrow_batch.schema.names):
            rows[name] = [v.as_py() for v in arrow_batch.column(i)]
        result.append({"schema_names": list(arrow_batch.schema.names), "rows": rows})
    return result


def decode_batches(data: list[dict]) -> list:
    import pyarrow as pa
    from forge.datatypes import RecordBatch

    batches = []
    for item in data:
        arrays = [pa.array(item["rows"][name]) for name in item["schema_names"]]
        names = item["schema_names"]
        arrow_batch = pa.RecordBatch.from_arrays(arrays, names=names)
        batches.append(RecordBatch.from_arrow(arrow_batch))
    return batches
