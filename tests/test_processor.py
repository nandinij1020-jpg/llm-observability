"""Processor Lambda: SDK spans -> Kinesis event -> OpenSearch / DynamoDB / CloudWatch / SNS."""
from __future__ import annotations

import base64
import json

import pytest
from conftest import FakeKinesis, FakeOpenSearch, MockBedrockEndpoint

from llm_observability import KinesisEmitter, TracedBedrock, Tracer


def produce(runs: int = 3, agent: str = "support", user: str = "u1", tokens=(100, 50),
            tool_error: bool = False, kinesis: FakeKinesis | None = None) -> FakeKinesis:
    """Run real instrumented agent code and capture what it sends to Kinesis."""
    kin = kinesis or FakeKinesis()
    tracer = Tracer(service="svc", agent_id=agent, emitter=KinesisEmitter("s", kinesis_client=kin))
    br = TracedBedrock(MockBedrockEndpoint(*tokens), tracer)

    @tracer.tool()
    def lookup():
        if tool_error:
            raise TimeoutError("boom")
        return "ok"

    for i in range(runs):
        try:
            with tracer.agent_run(user_id=user, session_id=f"s{i}"):
                br.converse(modelId="amazon.nova-micro-v1:0", messages=[{"role": "user", "content": [{"text": "q"}]}])
                lookup()
        except TimeoutError:
            pass
    tracer.flush()
    return kin


def run(processor, fakes, event):
    deps = processor.Deps(ddb=fakes["ddb"], cloudwatch=fakes["cw"], sns=fakes["sns"], opensearch=fakes["os"])
    return processor.process(event, deps)


def today_key(fakes, pk):
    return next(sk for (p, sk) in fakes["ddb"].items if p == pk and sk.startswith("day#"))


def test_spans_are_indexed_rolled_up_and_measured(processor, fakes):
    kin = produce(runs=3)
    assert run(processor, fakes, kin.as_event()) == {"batchItemFailures": []}

    # OpenSearch: every span, with @timestamp
    assert len(fakes["os"].docs) == 9  # 3 x (agent + llm + tool)
    assert all("@timestamp" in d for d in fakes["os"].docs.values())

    # DynamoDB: per user / agent / model / session, per day and total
    ddb = fakes["ddb"]
    for pk in ("agent#support", "user#u1"):
        assert ddb.num(pk, "total", "requests") == 3
        assert ddb.num(pk, "total", "llm_calls") == 3
        assert ddb.num(pk, "total", "input_tokens") == 300
        assert ddb.num(pk, "total", "output_tokens") == 150
        assert ddb.num(pk, "total", "tool_calls") == 3
        assert ddb.num(pk, today_key(fakes, pk), "requests") == 3
    assert ddb.num("model#amazon.nova-micro-v1:0", "total", "llm_calls") == 3
    assert ddb.num("session#s0", "total", "requests") == 1
    assert ddb.num("agent#support", "total", "cost_usd") > 0
    assert ddb.items[("agent#support", "total")]["last_trace_id"]["S"]

    # CloudWatch: per agent and ALL
    cw = fakes["cw"]
    assert sum(d["Value"] for d in cw.find("Requests", "support")) == 3
    assert sum(d["Value"] for d in cw.find("Requests", "ALL")) == 3
    assert sum(d["Value"] for d in cw.find("TotalTokens", "ALL")) == 450
    lat = cw.find("AgentRunLatencyMs", "support")[0]
    assert len(lat["Values"]) == 3 and lat["Counts"] == [1.0, 1.0, 1.0]
    assert {d["Namespace"] for d in cw.data} == {"LLMObservability/test"}


def test_errors_are_counted(processor, fakes):
    kin = produce(runs=4, tool_error=True)
    run(processor, fakes, kin.as_event())
    assert fakes["ddb"].num("agent#support", "total", "errors") == 4
    assert fakes["ddb"].num("agent#support", "total", "tool_errors") == 4
    assert sum(d["Value"] for d in fakes["cw"].find("Errors", "ALL")) == 4


def test_users_and_agents_are_attributed_separately(processor, fakes):
    kin = produce(runs=2, agent="a1", user="alice", tokens=(10, 10))
    produce(runs=1, agent="a2", user="bob", tokens=(1000, 500), kinesis=kin)
    run(processor, fakes, kin.as_event())
    ddb = fakes["ddb"]
    assert ddb.num("user#alice", "total", "input_tokens") == 20
    assert ddb.num("user#bob", "total", "input_tokens") == 1000
    assert ddb.num("agent#a2", "total", "cost_usd") > ddb.num("agent#a1", "total", "cost_usd")


def test_opensearch_failure_retries_batch_before_counting(processor, fakes):
    kin = produce(runs=2)
    fakes["os"] = FakeOpenSearch(raise_exc=True)
    with pytest.raises(RuntimeError):
        run(processor, fakes, kin.as_event())
    assert fakes["ddb"].update_calls == 0 and fakes["cw"].data == []  # nothing double-counted on retry


def test_partial_index_failure_also_retries(processor, fakes):
    kin = produce(runs=1)
    first_span = json.loads(kin.records[0]["Data"])["span_id"]
    fakes["os"] = FakeOpenSearch(fail_ids={first_span})
    with pytest.raises(RuntimeError):
        run(processor, fakes, kin.as_event())


def test_malformed_records_are_skipped(processor, fakes):
    kin = produce(runs=1)
    event = kin.as_event()
    event["Records"].append({"kinesis": {"sequenceNumber": "bad1", "data": base64.b64encode(b"not json").decode()}})
    event["Records"].append({"kinesis": {"sequenceNumber": "bad2", "data": base64.b64encode(b'{"x": 1}').decode()}})
    run(processor, fakes, event)
    assert len(fakes["os"].docs) == 3


def test_latency_anomaly_triggers_sns_alert(processor, fakes):
    # 30 normal runs establish the baseline
    base = []
    for i in range(30):
        base.append({"trace_id": f"t{i}", "span_id": f"s{i}", "parent_span_id": None, "kind": "agent",
                     "name": "run", "agent_id": "support", "user_id": "u1", "status": "ok",
                     "start_time_ms": 1_700_000_000_000 + i * 1000, "latency_ms": 1000 + (i % 5) * 20,
                     "attributes": {"rollup_cost_usd": 0.001}})
    spike = dict(base[-1], trace_id="spike", span_id="spike-span", start_time_ms=1_700_000_100_000,
                 latency_ms=9000)
    event = {"Records": [{"kinesis": {"sequenceNumber": str(i),
                                      "data": base64.b64encode(json.dumps(s).encode()).decode()}}
                         for i, s in enumerate(base + [spike])]}
    run(processor, fakes, event)

    msgs = fakes["sns"].messages
    assert len(msgs) == 1
    alert = msgs[0]["Message"]
    assert alert["metric"] == "latency_ms" and alert["trace_id"] == "spike" and alert["z_score"] > 3
    assert "support" in msgs[0]["Subject"]
    assert fakes["os"].docs["spike-span"]["anomaly"] is True
    # state persisted for the next invocation
    assert fakes["ddb"].items[("anomaly#support#latency_ms", "ewma")]["count"]["N"] == "31"


def test_canary_traffic_does_not_train_baseline(processor, fakes):
    kin = produce(runs=3, agent="canary")
    run(processor, fakes, kin.as_event())
    assert not any(pk.startswith("anomaly#canary") for pk, _ in fakes["ddb"].items)
