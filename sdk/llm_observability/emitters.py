"""Where span records go.

* KinesisEmitter  – production: buffers spans and ships them to Kinesis Data Streams
* StdoutEmitter   – local dev: one JSON line per span
* InMemoryEmitter – tests: keeps spans in a list
"""
from __future__ import annotations

import atexit
import json
import logging
import sys
import threading
from typing import Any, Callable

log = logging.getLogger("llm_observability")

Listener = Callable[[dict[str, Any]], None]


class Emitter:
    def __init__(self) -> None:
        self._listeners: list[Listener] = []

    def add_listener(self, fn: Listener) -> None:
        self._listeners.append(fn)

    def remove_listener(self, fn: Listener) -> None:
        if fn in self._listeners:
            self._listeners.remove(fn)

    def emit(self, record: dict[str, Any]) -> None:
        for fn in list(self._listeners):
            try:
                fn(record)
            except Exception:  # listeners must never break the app
                log.exception("span listener failed")
        try:
            self._send(record)
        except Exception:  # observability must never break the app
            log.exception("failed to emit span")

    def _send(self, record: dict[str, Any]) -> None:
        raise NotImplementedError

    def flush(self) -> None:
        pass


class StdoutEmitter(Emitter):
    def _send(self, record: dict[str, Any]) -> None:
        sys.stdout.write(json.dumps(record, default=str) + "\n")


class InMemoryEmitter(Emitter):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[dict[str, Any]] = []

    def _send(self, record: dict[str, Any]) -> None:
        self.records.append(record)

    def by_kind(self, kind: str) -> list[dict[str, Any]]:
        return [r for r in self.records if r["kind"] == kind]


class KinesisEmitter(Emitter):
    """Batches spans and sends them with PutRecords (max 500 per call).

    Partition key = trace_id, so every span of a trace lands on the same shard
    and is processed in order.
    """

    MAX_BATCH = 500

    def __init__(self, stream_name: str, kinesis_client: Any = None,
                 batch_size: int = 25, region_name: str | None = None) -> None:
        super().__init__()
        if kinesis_client is None:
            import boto3  # imported lazily so the SDK has no hard dependency
            kinesis_client = boto3.client("kinesis", region_name=region_name)
        self.client = kinesis_client
        self.stream_name = stream_name
        self.batch_size = min(batch_size, self.MAX_BATCH)
        self._buffer: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self.failed_records = 0
        atexit.register(self.flush)

    def _send(self, record: dict[str, Any]) -> None:
        with self._lock:
            self._buffer.append(record)
            ready = len(self._buffer) >= self.batch_size
        if ready:
            self.flush()

    def flush(self) -> None:
        with self._lock:
            batch, self._buffer = self._buffer, []
        for start in range(0, len(batch), self.MAX_BATCH):
            chunk = batch[start:start + self.MAX_BATCH]
            entries = [{"Data": json.dumps(r, default=str).encode("utf-8"),
                        "PartitionKey": r.get("trace_id") or "none"} for r in chunk]
            resp = self.client.put_records(StreamName=self.stream_name, Records=entries)
            failed = resp.get("FailedRecordCount", 0)
            if failed:
                # retry the failed entries once (throttling is the usual cause)
                retry = [e for e, res in zip(entries, resp["Records"]) if "ErrorCode" in res]
                resp2 = self.client.put_records(StreamName=self.stream_name, Records=retry)
                self.failed_records += resp2.get("FailedRecordCount", 0)
