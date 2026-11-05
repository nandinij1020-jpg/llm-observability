"""Synthetic canary: proves the whole pipeline works end to end.

Runs every few minutes (EventBridge schedule). Each run:
  1. performs a tiny 2-step "agent" run through the instrumented SDK
     (one Bedrock call + one tool call) with agent_id = "canary"
  2. flushes the spans to Kinesis
  3. polls DynamoDB until the processor has written that run's trace_id onto
     the `agent#canary` summary row (proves Kinesis -> Lambda -> DynamoDB works)
  4. publishes CanarySuccess = 1/0 and CanaryE2ELatencyMs to CloudWatch;
     an alarm on CanarySuccess pages SNS if the pipeline is broken.

Set CANARY_MOCK_BEDROCK=true to skip the real Bedrock call (zero model cost);
the rest of the pipeline is still exercised.
"""
from __future__ import annotations

import json
import os
import time
from typing import Any

from llm_observability import KinesisEmitter, TracedBedrock, Tracer

try:  # X-Ray subsegments for the Bedrock / Kinesis / DynamoDB calls when bundled
    from aws_xray_sdk.core import patch_all  # type: ignore
    patch_all()
except Exception:  # pragma: no cover
    pass

ENV = os.environ.get("ENVIRONMENT", "dev")
STREAM = os.environ.get("TRACE_STREAM_NAME", "")
TABLE = os.environ.get("USAGE_TABLE", "")
NAMESPACE = os.environ.get("METRIC_NAMESPACE", f"LLMObservability/{ENV}")
MODEL_ID = os.environ.get("CANARY_MODEL_ID", "amazon.nova-micro-v1:0")
MOCK = os.environ.get("CANARY_MOCK_BEDROCK", "false").lower() == "true"
WAIT_SECONDS = int(os.environ.get("CANARY_WAIT_SECONDS", "45"))


class MockBedrock:
    """Returns a Converse-shaped response without calling Bedrock."""

    def converse(self, **kwargs: Any) -> dict[str, Any]:
        time.sleep(0.05)
        return {
            "output": {"message": {"role": "assistant", "content": [{"text": "pong"}]}},
            "stopReason": "end_turn",
            "usage": {"inputTokens": 12, "outputTokens": 3, "totalTokens": 15},
            "metrics": {"latencyMs": 50},
        }


def run_canary(bedrock_client: Any, kinesis_client: Any, ddb_client: Any, cw_client: Any,
               wait_seconds: int = WAIT_SECONDS, poll_interval: float = 2.0) -> dict[str, Any]:
    emitter = KinesisEmitter(STREAM, kinesis_client=kinesis_client, batch_size=50)
    tracer = Tracer(service="canary", agent_id="canary", environment=ENV, emitter=emitter)
    bedrock = TracedBedrock(bedrock_client, tracer)

    @tracer.tool("health_lookup")
    def health_lookup(component: str) -> dict[str, str]:
        return {"component": component, "status": "green"}

    started = time.time()
    success, error, trace_id = 0, None, None
    try:
        with tracer.agent_run("canary_run", user_id="canary", session_id=f"canary-{int(started)}") as root:
            trace_id = root.trace_id
            bedrock.converse(
                modelId=MODEL_ID,
                messages=[{"role": "user", "content": [{"text": "Reply with the single word: pong"}]}],
                inferenceConfig={"maxTokens": 5, "temperature": 0},
            )
            health_lookup("pipeline")
        tracer.flush()

        deadline = started + wait_seconds
        while time.time() < deadline:
            item = ddb_client.get_item(
                TableName=TABLE, Key={"pk": {"S": "agent#canary"}, "sk": {"S": "total"}},
                ConsistentRead=True).get("Item") or {}
            if item.get("last_trace_id", {}).get("S") == trace_id:
                success = 1
                break
            time.sleep(poll_interval)
        if not success:
            error = f"trace {trace_id} not visible in DynamoDB after {wait_seconds}s"
    except Exception as exc:  # any failure = canary failure
        error = f"{type(exc).__name__}: {exc}"

    e2e_ms = (time.time() - started) * 1000
    cw_client.put_metric_data(Namespace=NAMESPACE, MetricData=[
        {"MetricName": "CanarySuccess", "Value": float(success), "Unit": "Count"},
        {"MetricName": "CanaryE2ELatencyMs", "Value": e2e_ms, "Unit": "Milliseconds"},
    ])
    result = {"success": bool(success), "trace_id": trace_id, "e2e_ms": round(e2e_ms), "error": error}
    print(json.dumps(result))
    return result


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    import boto3
    bedrock = MockBedrock() if MOCK else boto3.client("bedrock-runtime")
    result = run_canary(bedrock, boto3.client("kinesis"), boto3.client("dynamodb"),
                        boto3.client("cloudwatch"))
    if not result["success"]:
        # raising also marks the invocation as failed in Lambda metrics / X-Ray
        raise RuntimeError(result["error"])
    return result
