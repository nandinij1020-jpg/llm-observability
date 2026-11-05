"""Shared fixtures: fake AWS clients and a mock Bedrock endpoint.

No AWS account or network is needed to run the suite.
"""
from __future__ import annotations

import base64
import importlib.util
import json
import pathlib
import sys
from typing import Any

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "sdk"))
sys.path.insert(0, str(ROOT / "lambdas" / "processor"))


def load_module(name: str, path: pathlib.Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def processor(monkeypatch):
    monkeypatch.setenv("USAGE_TABLE", "usage")
    monkeypatch.setenv("ALERT_TOPIC_ARN", "arn:aws:sns:us-east-1:123:alerts")
    monkeypatch.setenv("ENVIRONMENT", "test")
    return load_module("processor_handler", ROOT / "lambdas" / "processor" / "handler.py")


@pytest.fixture
def canary(monkeypatch):
    monkeypatch.setenv("TRACE_STREAM_NAME", "traces")
    monkeypatch.setenv("USAGE_TABLE", "usage")
    return load_module("canary_handler", ROOT / "lambdas" / "canary" / "handler.py")


class MockBedrockEndpoint:
    """Behaves like bedrock-runtime.converse; records every request it receives."""

    def __init__(self, input_tokens: int = 100, output_tokens: int = 50, fail: Exception | None = None,
                 stop_reason: str = "end_turn", text: str = "hello from mock bedrock"):
        self.calls: list[dict[str, Any]] = []
        self.input_tokens, self.output_tokens = input_tokens, output_tokens
        self.fail, self.stop_reason, self.text = fail, stop_reason, text

    def converse(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if self.fail:
            raise self.fail
        return {
            "output": {"message": {"role": "assistant", "content": [{"text": self.text}]}},
            "stopReason": self.stop_reason,
            "usage": {"inputTokens": self.input_tokens, "outputTokens": self.output_tokens,
                      "totalTokens": self.input_tokens + self.output_tokens},
            "metrics": {"latencyMs": 123},
        }

    def list_foundation_models(self) -> str:  # proves pass-through works
        return "passthrough"


class FakeKinesis:
    def __init__(self, fail_first: int = 0):
        self.records: list[dict[str, Any]] = []
        self.calls = 0
        self.fail_first = fail_first

    def put_records(self, StreamName: str, Records: list[dict[str, Any]]) -> dict[str, Any]:
        self.calls += 1
        out = []
        failed = 0
        for r in Records:
            if self.fail_first > 0:
                self.fail_first -= 1
                failed += 1
                out.append({"ErrorCode": "ProvisionedThroughputExceededException"})
            else:
                self.records.append(r)
                out.append({"SequenceNumber": str(len(self.records))})
        return {"FailedRecordCount": failed, "Records": out}

    def as_event(self) -> dict[str, Any]:
        return {"Records": [{"kinesis": {"sequenceNumber": str(i),
                                         "data": base64.b64encode(r["Data"]).decode()}}
                            for i, r in enumerate(self.records)]}


class FakeDynamo:
    """Implements just enough of update_item(ADD/SET), get_item and put_item."""

    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict[str, Any]] = {}
        self.update_calls = 0

    @staticmethod
    def _key(Key: dict[str, Any]) -> tuple[str, str]:
        return Key["pk"]["S"], Key["sk"]["S"]

    def update_item(self, TableName: str, Key: dict, UpdateExpression: str,
                    ExpressionAttributeNames: dict, ExpressionAttributeValues: dict) -> dict:
        self.update_calls += 1
        item = self.items.setdefault(self._key(Key), {"pk": Key["pk"], "sk": Key["sk"]})
        add_part, set_part = UpdateExpression[4:].split(" SET ")
        for clause in add_part.split(", "):
            n, v = clause.split(" ")
            field, val = ExpressionAttributeNames[n], ExpressionAttributeValues[v]["N"]
            item[field] = {"N": str(float(item.get(field, {"N": "0"})["N"]) + float(val))}
        for clause in set_part.split(", "):
            n, v = clause.split(" = ")
            item[ExpressionAttributeNames[n]] = ExpressionAttributeValues[v]
        return {}

    def get_item(self, TableName: str, Key: dict, ConsistentRead: bool = False) -> dict:
        item = self.items.get(self._key(Key))
        return {"Item": item} if item else {}

    def put_item(self, TableName: str, Item: dict) -> dict:
        self.items[(Item["pk"]["S"], Item["sk"]["S"])] = Item
        return {}

    def num(self, pk: str, sk: str, field: str) -> float:
        return float(self.items[(pk, sk)][field]["N"])


class FakeCloudWatch:
    def __init__(self) -> None:
        self.data: list[dict[str, Any]] = []

    def put_metric_data(self, Namespace: str, MetricData: list[dict[str, Any]]) -> dict:
        assert len(MetricData) <= 1000
        self.data.extend({**d, "Namespace": Namespace} for d in MetricData)
        return {}

    def find(self, name: str, agent: str | None = None) -> list[dict[str, Any]]:
        return [d for d in self.data if d["MetricName"] == name and
                (agent is None or {"Name": "AgentId", "Value": agent} in d.get("Dimensions", []))]


class FakeSNS:
    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []

    def publish(self, TopicArn: str, Subject: str, Message: str) -> dict:
        assert len(Subject) <= 100
        self.messages.append({"Subject": Subject, "Message": json.loads(Message)})
        return {}


class FakeOpenSearch:
    def __init__(self, fail_ids: set[str] | None = None, raise_exc: bool = False):
        self.docs: dict[str, dict[str, Any]] = {}
        self.fail_ids = fail_ids or set()
        self.raise_exc = raise_exc

    def bulk(self, docs: list[dict[str, Any]]) -> set[str]:
        if self.raise_exc:
            raise RuntimeError("cluster unavailable")
        failed = set()
        for d in docs:
            if d["span_id"] in self.fail_ids:
                failed.add(d["span_id"])
            else:
                self.docs[d["span_id"]] = d
        return failed


@pytest.fixture
def mock_bedrock() -> MockBedrockEndpoint:
    return MockBedrockEndpoint()


@pytest.fixture
def fakes() -> dict[str, Any]:
    return {"kinesis": FakeKinesis(), "ddb": FakeDynamo(), "cw": FakeCloudWatch(),
            "sns": FakeSNS(), "os": FakeOpenSearch()}
