"""Canary end-to-end check, wired to the real processor running on fake AWS."""
from __future__ import annotations

from conftest import FakeKinesis


class PipelineKinesis(FakeKinesis):
    """Fake Kinesis that immediately runs the processor, like the real event source mapping."""

    def __init__(self, processor, deps):
        super().__init__()
        self.processor, self.deps = processor, deps

    def put_records(self, StreamName, Records):
        start = len(self.records)
        resp = super().put_records(StreamName, Records)
        new = FakeKinesis()
        new.records = self.records[start:]
        self.processor.process(new.as_event(), self.deps)
        return resp


def test_canary_succeeds_when_pipeline_is_healthy(canary, processor, fakes):
    deps = processor.Deps(ddb=fakes["ddb"], cloudwatch=fakes["cw"], sns=fakes["sns"], opensearch=fakes["os"])
    kin = PipelineKinesis(processor, deps)
    result = canary.run_canary(canary.MockBedrock(), kin, fakes["ddb"], fakes["cw"],
                               wait_seconds=2, poll_interval=0.01)
    assert result["success"] is True
    assert fakes["ddb"].items[("agent#canary", "total")]["last_trace_id"]["S"] == result["trace_id"]
    success = [d for d in fakes["cw"].data if d["MetricName"] == "CanarySuccess"]
    assert success[-1]["Value"] == 1.0
    assert len(fakes["os"].docs) == 3  # agent + llm + tool spans


def test_canary_fails_when_processor_is_broken(canary, fakes):
    lost = FakeKinesis()  # records go in, nothing ever processes them
    result = canary.run_canary(canary.MockBedrock(), lost, fakes["ddb"], fakes["cw"],
                               wait_seconds=0.2, poll_interval=0.05)
    assert result["success"] is False and "not visible" in result["error"]
    assert [d["Value"] for d in fakes["cw"].data if d["MetricName"] == "CanarySuccess"] == [0.0]


def test_canary_fails_when_bedrock_is_down(canary, fakes):
    class DownBedrock:
        def converse(self, **kw):
            raise ConnectionError("bedrock unreachable")

    result = canary.run_canary(DownBedrock(), FakeKinesis(), fakes["ddb"], fakes["cw"], wait_seconds=0.1)
    assert result["success"] is False and "ConnectionError" in result["error"]
