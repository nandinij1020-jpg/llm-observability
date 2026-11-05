"""SDK instrumentation tests — validated against a mock Bedrock endpoint."""
from __future__ import annotations

import io
import json

import pytest
from conftest import FakeKinesis, MockBedrockEndpoint

from llm_observability import InMemoryEmitter, KinesisEmitter, TracedBedrock, Tracer, compute_cost
from llm_observability.pricing import price_for


def make(emitter=None, **kw):
    emitter = emitter or InMemoryEmitter()
    return Tracer(service="svc", agent_id="agent-a", environment="test", emitter=emitter, **kw), emitter


def converse(br, model="amazon.nova-micro-v1:0", text="hi"):
    return br.converse(modelId=model, messages=[{"role": "user", "content": [{"text": text}]}])


# ----------------------------------------------------------------- LLM spans
def test_converse_emits_llm_span_with_tokens_cost_and_latency(mock_bedrock):
    tracer, em = make()
    br = TracedBedrock(mock_bedrock, tracer)
    resp = converse(br, text="What is 2+2?")

    assert resp["output"]["message"]["content"][0]["text"] == "hello from mock bedrock"
    (span,) = em.by_kind("llm")
    assert span["model_id"] == "amazon.nova-micro-v1:0"
    assert span["input_tokens"] == 100 and span["output_tokens"] == 50 and span["total_tokens"] == 150
    assert span["cost_usd"] == pytest.approx(compute_cost("amazon.nova-micro-v1:0", 100, 50))
    assert span["latency_ms"] >= 0
    assert span["status"] == "ok" and span["stop_reason"] == "end_turn"
    assert "What is 2+2?" in span["prompt"] and span["completion"] == "hello from mock bedrock"
    assert span["attributes"]["bedrock_latency_ms"] == 123
    assert len(span["trace_id"]) == 32 and len(span["span_id"]) == 16
    assert span["schema_version"] == "1.0"


def test_request_is_forwarded_unchanged(mock_bedrock):
    tracer, _ = make()
    br = TracedBedrock(mock_bedrock, tracer)
    kwargs = dict(modelId="m", messages=[{"role": "user", "content": [{"text": "x"}]}],
                  inferenceConfig={"maxTokens": 7})
    br.converse(**kwargs)
    assert mock_bedrock.calls == [kwargs]


def test_other_client_methods_pass_through(mock_bedrock):
    tracer, _ = make()
    assert TracedBedrock(mock_bedrock, tracer).list_foundation_models() == "passthrough"


def test_bedrock_error_is_recorded_and_reraised():
    tracer, em = make()
    br = TracedBedrock(MockBedrockEndpoint(fail=RuntimeError("ThrottlingException")), tracer)
    with pytest.raises(RuntimeError):
        converse(br)
    (span,) = em.by_kind("llm")
    assert span["status"] == "error"
    assert span["error_type"] == "RuntimeError" and "Throttling" in span["error_message"]
    assert span["cost_usd"] == 0


def test_capture_content_off_redacts_prompts(mock_bedrock):
    tracer, em = make(capture_content=False)
    converse(TracedBedrock(mock_bedrock, tracer), text="secret")
    (span,) = em.by_kind("llm")
    assert span["prompt"] is None and span["completion"] is None
    assert span["input_tokens"] == 100  # metrics still recorded


def test_long_prompts_are_truncated(mock_bedrock):
    tracer, em = make()
    converse(TracedBedrock(mock_bedrock, tracer), text="x" * 10_000)
    assert len(em.by_kind("llm")[0]["prompt"]) < 2_200


def test_invoke_model_reads_token_headers():
    class InvokeClient:
        def invoke_model(self, **kw):
            return {"body": io.BytesIO(b'{"outputText": "ok"}'),
                    "ResponseMetadata": {"HTTPHeaders": {
                        "x-amzn-bedrock-input-token-count": "11",
                        "x-amzn-bedrock-output-token-count": "4",
                        "x-amzn-bedrock-invocation-latency": "90"}}}

    tracer, em = make()
    resp = TracedBedrock(InvokeClient(), tracer).invoke_model(
        modelId="amazon.titan-text-express-v1", body=json.dumps({"inputText": "hi"}))
    assert json.loads(resp["body"].read()) == {"outputText": "ok"}  # caller can still read body
    (span,) = em.by_kind("llm")
    assert (span["input_tokens"], span["output_tokens"]) == (11, 4)
    assert span["cost_usd"] > 0 and span["attributes"]["bedrock_latency_ms"] == 90


# ------------------------------------------------------ traces / agent chains
def test_multi_step_agent_chain_builds_a_tree_and_rolls_up_cost(mock_bedrock):
    tracer, em = make()
    br = TracedBedrock(mock_bedrock, tracer)

    @tracer.tool()
    def lookup(order_id):
        return {"order_id": order_id, "status": "shipped"}

    with tracer.agent_run(user_id="u1", session_id="s1") as root:
        converse(br)
        lookup("A-1")
        converse(br)

    agent = em.by_kind("agent")[0]
    llm = em.by_kind("llm")
    tool = em.by_kind("tool")[0]
    assert agent["span_id"] == root.span_id and agent["parent_span_id"] is None
    assert {s["trace_id"] for s in em.records} == {agent["trace_id"]}
    assert all(s["parent_span_id"] == agent["span_id"] for s in llm + [tool])
    assert all(s["user_id"] == "u1" and s["session_id"] == "s1" for s in em.records)
    assert tool["tool_name"] == "lookup" and "shipped" in tool["tool_output"]
    a = agent["attributes"]
    assert a["llm_calls"] == 2 and a["tool_calls"] == 1
    assert a["rollup_input_tokens"] == 200 and a["rollup_output_tokens"] == 100
    assert a["rollup_cost_usd"] == pytest.approx(sum(s["cost_usd"] for s in llm))
    # the root span is emitted last (it closes last)
    assert em.records[-1]["kind"] == "agent"


def test_nested_spans_and_separate_runs_get_separate_traces(mock_bedrock):
    tracer, em = make()
    br = TracedBedrock(mock_bedrock, tracer)
    with tracer.agent_run():
        with tracer.span("planner") as planner:
            converse(br)
    with tracer.agent_run():
        converse(br)
    llm1, llm2 = em.by_kind("llm")
    assert llm1["parent_span_id"] == planner.span_id
    assert llm1["trace_id"] != llm2["trace_id"]


def test_tool_error_marks_span_and_agent_run(mock_bedrock):
    tracer, em = make()

    @tracer.tool("flaky")
    def flaky():
        raise TimeoutError("downstream timeout")

    with pytest.raises(TimeoutError):
        with tracer.agent_run():
            flaky()
    tool = em.by_kind("tool")[0]
    agent = em.by_kind("agent")[0]
    assert tool["status"] == "error" and tool["error_type"] == "TimeoutError"
    assert agent["status"] == "error" and "stacktrace" in agent["attributes"]


def test_context_propagation_across_processes(mock_bedrock):
    caller, em = make()
    remote = Tracer(service="tool-svc", agent_id="agent-a", emitter=em)
    with caller.agent_run() as root:
        carrier = caller.inject()
    assert carrier["traceparent"] == f"00-{root.trace_id}-{root.span_id}-01"
    with remote.extract(carrier):
        with remote.span("remote_tool", kind="tool"):
            pass
    remote_span = [s for s in em.records if s["name"] == "remote_tool"][0]
    assert remote_span["trace_id"] == root.trace_id
    assert remote_span["parent_span_id"] == root.span_id


def test_emitter_failure_never_breaks_the_app(mock_bedrock):
    class Broken(InMemoryEmitter):
        def _send(self, record):
            raise ConnectionError("kinesis down")

    tracer, _ = make(emitter=Broken())
    assert converse(TracedBedrock(mock_bedrock, tracer))["stopReason"] == "end_turn"


# ------------------------------------------------------------------ emitters
def test_kinesis_emitter_batches_and_uses_trace_id_partition_key(mock_bedrock):
    kin = FakeKinesis()
    tracer, _ = make(emitter=KinesisEmitter("traces", kinesis_client=kin, batch_size=3))
    br = TracedBedrock(mock_bedrock, tracer)
    with tracer.agent_run():
        converse(br)
        converse(br)
    assert kin.calls == 1 and len(kin.records) == 3  # 2 llm + 1 agent -> one batch
    tracer.flush()
    payloads = [json.loads(r["Data"]) for r in kin.records]
    assert {r["PartitionKey"] for r in kin.records} == {payloads[0]["trace_id"]}


def test_kinesis_emitter_retries_throttled_records(mock_bedrock):
    kin = FakeKinesis(fail_first=1)
    em = KinesisEmitter("traces", kinesis_client=kin, batch_size=100)
    tracer, _ = make(emitter=em)
    converse(TracedBedrock(mock_bedrock, tracer))
    converse(TracedBedrock(mock_bedrock, tracer))
    tracer.flush()
    assert len(kin.records) == 2 and kin.calls == 2 and em.failed_records == 0


# ------------------------------------------------------------------- pricing
def test_cost_formula():
    p = price_for("amazon.nova-micro-v1:0")
    expected = 1000 / 1000 * p.input_per_1k + 2000 / 1000 * p.output_per_1k
    assert compute_cost("amazon.nova-micro-v1:0", 1000, 2000) == pytest.approx(expected)


def test_inference_profile_prefix_is_normalised():
    assert price_for("us.amazon.nova-micro-v1:0") == price_for("amazon.nova-micro-v1:0")


def test_pricing_override_from_env(monkeypatch):
    monkeypatch.setenv("LLM_OBS_PRICING_JSON", json.dumps({"my.model": [1.0, 2.0]}))
    assert compute_cost("my.model", 1000, 1000) == pytest.approx(3.0)
