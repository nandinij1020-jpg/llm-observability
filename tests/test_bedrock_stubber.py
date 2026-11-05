"""Same wrapper, but against a *real* boto3 bedrock-runtime client whose HTTP
layer is replaced by botocore's Stubber. This proves the wrapper works with
the actual Bedrock request/response shapes boto3 validates. Runs in CI
(boto3 installed); skipped locally if boto3 is missing.
"""
from __future__ import annotations

import pytest

boto3 = pytest.importorskip("boto3")
from botocore.stub import Stubber  # noqa: E402

from llm_observability import InMemoryEmitter, TracedBedrock, Tracer  # noqa: E402


def test_converse_against_stubbed_bedrock_runtime():
    client = boto3.client("bedrock-runtime", region_name="us-east-1",
                          aws_access_key_id="x", aws_secret_access_key="x")
    stub = Stubber(client)
    request = {"modelId": "amazon.nova-micro-v1:0",
               "messages": [{"role": "user", "content": [{"text": "ping"}]}]}
    stub.add_response("converse", {
        "output": {"message": {"role": "assistant", "content": [{"text": "pong"}]}},
        "stopReason": "end_turn",
        "usage": {"inputTokens": 9, "outputTokens": 2, "totalTokens": 11},
        "metrics": {"latencyMs": 42},
    }, request)
    stub.add_client_error("converse", service_error_code="ThrottlingException",
                          service_message="Rate exceeded", http_status_code=429)

    em = InMemoryEmitter()
    br = TracedBedrock(client, Tracer(service="ci", agent_id="ci-agent", emitter=em))
    with stub:
        assert br.converse(**request)["output"]["message"]["content"][0]["text"] == "pong"
        with pytest.raises(client.exceptions.ThrottlingException):
            br.converse(**request)
        stub.assert_no_pending_responses()

    ok, err = em.records
    assert (ok["input_tokens"], ok["output_tokens"], ok["status"]) == (9, 2, "ok")
    assert ok["attributes"]["bedrock_latency_ms"] == 42
    assert err["status"] == "error" and err["error_type"] == "ThrottlingException"
