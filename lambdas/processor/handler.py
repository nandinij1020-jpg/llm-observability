"""Trace processor: Kinesis -> OpenSearch + DynamoDB + CloudWatch (+ SNS on anomalies).

Triggered by the Kinesis event source mapping. For each batch of span records:

1. Bulk-index every span into OpenSearch (daily index, doc id = span_id so a
   retried batch is idempotent)                         -> trace search & dashboards
2. Roll up tokens / cost / requests / errors per user, agent, session and model
   per day, with atomic ADD updates                     -> DynamoDB summaries
3. Publish CloudWatch custom metrics per agent (+ an "ALL" roll-up)
                                                        -> alarms & dashboards
4. Score agent-run latency and cost with an EWMA z-score detector and publish
   an SNS alert when a run is anomalous                 -> real-time anomaly alerts

X-Ray: the function runs with active tracing; if aws-xray-sdk is packaged, the
boto3/HTTP calls appear as subsegments and each batch is annotated with the
LLM trace ids so you can search X-Ray by `annotation.llm_trace_id`.

Retry semantics: OpenSearch indexing (idempotent) runs first; if it fails the
whole batch is retried before any counters are updated, so a retry does not
double-count tokens or cost. Malformed records are logged and skipped.
"""
from __future__ import annotations

import base64
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable

try:  # local import path differs between Lambda and tests
    from anomaly import EwmaState, update as ewma_update
except ImportError:  # pragma: no cover
    from .anomaly import EwmaState, update as ewma_update

log = logging.getLogger()
log.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

try:  # optional: richer X-Ray traces when the SDK is bundled
    from aws_xray_sdk.core import patch_all, xray_recorder  # type: ignore
    patch_all()
    XRAY = True
except Exception:  # pragma: no cover
    XRAY = False

ENV = os.environ.get("ENVIRONMENT", "dev")
NAMESPACE = os.environ.get("METRIC_NAMESPACE", f"LLMObservability/{ENV}")
TABLE = os.environ.get("USAGE_TABLE", "")
OS_ENDPOINT = os.environ.get("OPENSEARCH_ENDPOINT", "")
OS_INDEX_PREFIX = os.environ.get("OPENSEARCH_INDEX_PREFIX", "llm-traces")
SNS_TOPIC = os.environ.get("ALERT_TOPIC_ARN", "")
Z_THRESHOLD = float(os.environ.get("ANOMALY_Z_THRESHOLD", "3.0"))
WARMUP = int(os.environ.get("ANOMALY_WARMUP", "20"))
REGION = os.environ.get("AWS_REGION", "us-east-1")


# --------------------------------------------------------------------------- deps
class Deps:
    """AWS clients, created lazily so tests can inject fakes."""

    def __init__(self, ddb: Any = None, cloudwatch: Any = None, sns: Any = None,
                 opensearch: "OpenSearchClient | None" = None):
        self._ddb, self._cw, self._sns, self._os = ddb, cloudwatch, sns, opensearch

    @property
    def ddb(self) -> Any:
        if self._ddb is None:
            import boto3
            self._ddb = boto3.client("dynamodb")
        return self._ddb

    @property
    def cloudwatch(self) -> Any:
        if self._cw is None:
            import boto3
            self._cw = boto3.client("cloudwatch")
        return self._cw

    @property
    def sns(self) -> Any:
        if self._sns is None:
            import boto3
            self._sns = boto3.client("sns")
        return self._sns

    @property
    def opensearch(self) -> "OpenSearchClient | None":
        if self._os is None and OS_ENDPOINT:
            self._os = OpenSearchClient(OS_ENDPOINT, REGION)
        return self._os


class OpenSearchClient:
    """Tiny SigV4-signed OpenSearch client (no extra dependencies)."""

    def __init__(self, endpoint: str, region: str):
        import boto3
        import urllib3
        self.base = endpoint if endpoint.startswith("http") else f"https://{endpoint}"
        self.region = region
        self.session = boto3.Session()
        self.http = urllib3.PoolManager(timeout=urllib3.Timeout(connect=3, read=15))
        self._template_ready = False

    def request(self, method: str, path: str, body: str | None = None,
                content_type: str = "application/json") -> dict[str, Any]:
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest
        url = f"{self.base}{path}"
        req = AWSRequest(method=method, url=url, data=body,
                         headers={"Content-Type": content_type})
        SigV4Auth(self.session.get_credentials(), "es", self.region).add_auth(req)
        resp = self.http.request(method, url, body=body, headers=dict(req.headers))
        data = json.loads(resp.data or b"{}")
        if resp.status >= 300:
            raise RuntimeError(f"OpenSearch {method} {path} -> {resp.status}: {str(data)[:500]}")
        return data

    def ensure_template(self) -> None:
        if self._template_ready:
            return
        self.request("PUT", f"/_index_template/{OS_INDEX_PREFIX}", json.dumps(INDEX_TEMPLATE))
        self._template_ready = True

    def bulk(self, docs: list[dict[str, Any]]) -> set[str]:
        """Index docs; returns the span_ids that failed."""
        if not docs:
            return set()
        self.ensure_template()
        lines = []
        for d in docs:
            day = datetime.fromtimestamp(d["start_time_ms"] / 1000, tz=timezone.utc).strftime("%Y.%m.%d")
            lines.append(json.dumps({"index": {"_index": f"{OS_INDEX_PREFIX}-{day}", "_id": d["span_id"]}}))
            lines.append(json.dumps(d, default=str))
        result = self.request("POST", "/_bulk", "\n".join(lines) + "\n", "application/x-ndjson")
        failed = set()
        if result.get("errors"):
            for item in result.get("items", []):
                op = item.get("index", {})
                if op.get("status", 200) >= 300:
                    failed.add(op.get("_id"))
                    log.warning("index failed: %s", op.get("error"))
        return failed


INDEX_TEMPLATE = {
    "index_patterns": [f"{OS_INDEX_PREFIX}-*"],
    "template": {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "dynamic_templates": [{"strings_as_keywords": {
                "match_mapping_type": "string",
                "mapping": {"type": "keyword", "ignore_above": 512}}}],
            "properties": {
                "@timestamp": {"type": "date"},
                "start_time_ms": {"type": "date", "format": "epoch_millis"},
                "end_time_ms": {"type": "date", "format": "epoch_millis"},
                "trace_id": {"type": "keyword"}, "span_id": {"type": "keyword"},
                "parent_span_id": {"type": "keyword"}, "name": {"type": "keyword"},
                "kind": {"type": "keyword"}, "status": {"type": "keyword"},
                "service": {"type": "keyword"}, "environment": {"type": "keyword"},
                "agent_id": {"type": "keyword"}, "user_id": {"type": "keyword"},
                "session_id": {"type": "keyword"}, "model_id": {"type": "keyword"},
                "tool_name": {"type": "keyword"}, "error_type": {"type": "keyword"},
                "error_message": {"type": "text"},
                "latency_ms": {"type": "float"}, "cost_usd": {"type": "double"},
                "input_tokens": {"type": "integer"}, "output_tokens": {"type": "integer"},
                "total_tokens": {"type": "integer"},
                "prompt": {"type": "text"}, "completion": {"type": "text"},
                "tool_input": {"type": "text"}, "tool_output": {"type": "text"},
                "attributes": {"type": "object", "properties": {
                    "stacktrace": {"type": "text"}}},
                "anomaly": {"type": "boolean"},
            },
        },
    },
}


# ----------------------------------------------------------------------- helpers
REQUIRED = ("trace_id", "span_id", "kind", "agent_id", "start_time_ms", "status")


def decode(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Decode Kinesis records; malformed ones are logged and skipped (poison pills)."""
    spans = []
    for r in records:
        seq = r["kinesis"]["sequenceNumber"]
        try:
            span = json.loads(base64.b64decode(r["kinesis"]["data"]))
            missing = [k for k in REQUIRED if k not in span]
            if missing:
                raise ValueError(f"missing fields {missing}")
            spans.append(span)
        except Exception:
            log.exception("malformed record %s (skipped)", seq)
    return spans


def day_of(span: dict[str, Any]) -> str:
    return datetime.fromtimestamp(span["start_time_ms"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def build_rollups(spans: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, float]]:
    """(entity, period) -> counters. Periods: day#YYYY-MM-DD and 'total'."""
    agg: dict[tuple[str, str], dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for s in spans:
        entities = [f"agent#{s['agent_id']}"]
        if s.get("user_id"):
            entities.append(f"user#{s['user_id']}")
        if s.get("session_id"):
            entities.append(f"session#{s['session_id']}")
        if s.get("kind") == "llm" and s.get("model_id"):
            entities.append(f"model#{s['model_id']}")
        for entity in entities:
            for period in (f"day#{day_of(s)}", "total"):
                c = agg[(entity, period)]
                if s["kind"] == "agent":
                    c["requests"] += 1
                    c["errors"] += 1 if s["status"] == "error" else 0
                    c["latency_ms_sum"] += s.get("latency_ms") or 0
                elif s["kind"] == "llm":
                    c["llm_calls"] += 1
                    c["llm_errors"] += 1 if s["status"] == "error" else 0
                    c["input_tokens"] += s.get("input_tokens") or 0
                    c["output_tokens"] += s.get("output_tokens") or 0
                    c["cost_usd"] += s.get("cost_usd") or 0.0
                elif s["kind"] == "tool":
                    c["tool_calls"] += 1
                    c["tool_errors"] += 1 if s["status"] == "error" else 0
    return agg


def write_rollups(deps: Deps, spans: list[dict[str, Any]]) -> None:
    if not TABLE:
        return
    rollups = build_rollups(spans)
    last_trace: dict[str, tuple[int, str]] = {}
    for s in spans:
        if s["kind"] == "agent":
            key = f"agent#{s['agent_id']}"
            if s["start_time_ms"] >= last_trace.get(key, (0, ""))[0]:
                last_trace[key] = (s["start_time_ms"], s["trace_id"])
    now = int(time.time() * 1000)
    for (entity, period), counters in rollups.items():
        names, values, adds = {}, {}, []
        for i, (field, val) in enumerate(counters.items()):
            names[f"#f{i}"] = field
            values[f":v{i}"] = {"N": repr(round(val, 10))}
            adds.append(f"#f{i} :v{i}")
        sets = ["#upd = :now"]
        names["#upd"] = "updated_ms"
        values[":now"] = {"N": str(now)}
        if entity in last_trace and period == "total":
            names["#lt"], names["#ls"] = "last_trace_id", "last_seen_ms"
            values[":lt"] = {"S": last_trace[entity][1]}
            values[":ls"] = {"N": str(last_trace[entity][0])}
            sets += ["#lt = :lt", "#ls = :ls"]
        deps.ddb.update_item(
            TableName=TABLE,
            Key={"pk": {"S": entity}, "sk": {"S": period}},
            UpdateExpression=f"ADD {', '.join(adds)} SET {', '.join(sets)}",
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
        )


def build_metrics(spans: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate spans into PutMetricData entries, per agent and for ALL."""
    by_agent: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "run_latency": [], "llm_latency": [], "cost": 0.0, "in": 0, "out": 0,
        "requests": 0, "errors": 0, "llm_calls": 0, "llm_errors": 0,
        "tool_calls": 0, "tool_errors": 0})
    ts = None
    for s in spans:
        for key in (s["agent_id"], "ALL"):
            a = by_agent[key]
            if s["kind"] == "agent":
                a["requests"] += 1
                a["errors"] += s["status"] == "error"
                a["run_latency"].append(s.get("latency_ms") or 0)
            elif s["kind"] == "llm":
                a["llm_calls"] += 1
                a["llm_errors"] += s["status"] == "error"
                a["llm_latency"].append(s.get("latency_ms") or 0)
                a["cost"] += s.get("cost_usd") or 0
                a["in"] += s.get("input_tokens") or 0
                a["out"] += s.get("output_tokens") or 0
            elif s["kind"] == "tool":
                a["tool_calls"] += 1
                a["tool_errors"] += s["status"] == "error"
        ts = max(ts or 0, s.get("end_time_ms") or s["start_time_ms"])
    timestamp = datetime.fromtimestamp((ts or time.time() * 1000) / 1000, tz=timezone.utc)

    data = []
    for agent, a in by_agent.items():
        dims = [{"Name": "AgentId", "Value": agent}]

        def m(name: str, value: float, unit: str = "Count") -> None:
            data.append({"MetricName": name, "Dimensions": dims, "Timestamp": timestamp,
                         "Value": float(value), "Unit": unit})

        def dist(name: str, values: list[float]) -> None:
            # Values/Counts keep the raw distribution so CloudWatch can compute p50/p90/p99
            for i in range(0, len(values), 150):
                chunk = values[i:i + 150]
                data.append({"MetricName": name, "Dimensions": dims, "Timestamp": timestamp,
                             "Values": [float(v) for v in chunk], "Counts": [1.0] * len(chunk),
                             "Unit": "Milliseconds"})

        counters = {
            "Requests": a["requests"], "Errors": a["errors"],
            "LLMCalls": a["llm_calls"], "LLMErrors": a["llm_errors"],
            "ToolCalls": a["tool_calls"], "ToolErrors": a["tool_errors"],
            "InputTokens": a["in"], "OutputTokens": a["out"], "TotalTokens": a["in"] + a["out"],
        }
        for name, value in counters.items():
            m(name, value)
        m("CostUSD", a["cost"], "None")
        dist("AgentRunLatencyMs", a["run_latency"])
        dist("LLMLatencyMs", a["llm_latency"])
    return data


def put_metrics(deps: Deps, spans: list[dict[str, Any]]) -> None:
    data = build_metrics(spans)
    for i in range(0, len(data), 20):
        deps.cloudwatch.put_metric_data(Namespace=NAMESPACE, MetricData=data[i:i + 20])


def detect_anomalies(deps: Deps, spans: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """EWMA z-score on agent-run latency and cost. State lives in DynamoDB."""
    runs = sorted((s for s in spans if s["kind"] == "agent" and s["status"] == "ok"),
                  key=lambda s: s["start_time_ms"])
    if not runs or not TABLE:
        return []
    states: dict[str, EwmaState] = {}

    def load(key: str) -> EwmaState:
        if key not in states:
            item = deps.ddb.get_item(TableName=TABLE, Key={"pk": {"S": key}, "sk": {"S": "ewma"}}).get("Item")
            states[key] = EwmaState(
                count=int(item["count"]["N"]), mean=float(item["mean"]["N"]),
                var=float(item["var"]["N"]), last_alert_ms=int(item["last_alert_ms"]["N"]),
            ) if item else EwmaState()
        return states[key]

    alerts = []
    for s in runs:
        if s["agent_id"].startswith("canary"):
            continue  # synthetic traffic must not shape the baseline
        observations = {
            "latency_ms": s.get("latency_ms") or 0.0,
            "cost_usd": (s.get("attributes") or {}).get("rollup_cost_usd", 0.0),
        }
        for metric, value in observations.items():
            key = f"anomaly#{s['agent_id']}#{metric}"
            v = ewma_update(load(key), float(value), s["start_time_ms"],
                            z_threshold=Z_THRESHOLD, warmup=WARMUP)
            if v.is_anomaly:
                s["anomaly"] = True
                alerts.append({"agent_id": s["agent_id"], "metric": metric, "value": value,
                               "z_score": v.z_score, "baseline_mean": round(v.baseline_mean, 6),
                               "baseline_std": round(v.baseline_std, 6), "trace_id": s["trace_id"],
                               "user_id": s.get("user_id"), "environment": ENV})

    for key, st in states.items():
        deps.ddb.put_item(TableName=TABLE, Item={
            "pk": {"S": key}, "sk": {"S": "ewma"}, "count": {"N": str(st.count)},
            "mean": {"N": repr(st.mean)}, "var": {"N": repr(st.var)},
            "last_alert_ms": {"N": str(st.last_alert_ms)}})

    if SNS_TOPIC:
        for a in alerts:
            deps.sns.publish(
                TopicArn=SNS_TOPIC,
                Subject=f"[LLM-Obs {ENV}] {a['metric']} anomaly on {a['agent_id']}"[:100],
                Message=json.dumps(a, indent=2),
            )
    return alerts


def to_document(span: dict[str, Any]) -> dict[str, Any]:
    doc = dict(span)
    doc["@timestamp"] = datetime.fromtimestamp(
        span["start_time_ms"] / 1000, tz=timezone.utc).isoformat()
    return doc


# ----------------------------------------------------------------------- handler
_DEPS = Deps()


def process(event: dict[str, Any], deps: Deps) -> dict[str, Any]:
    spans = decode(event.get("Records", []))
    if not spans:
        return {"batchItemFailures": []}

    if XRAY:  # pragma: no cover
        try:
            sub = xray_recorder.current_subsegment() or xray_recorder.current_segment()
            sub.put_annotation("llm_trace_id", spans[0]["trace_id"])
            sub.put_metadata("llm_trace_ids", sorted({s["trace_id"] for s in spans}))
        except Exception:
            pass

    # 1) OpenSearch first: indexing is idempotent (doc id = span_id), so if it
    #    fails we raise and Lambda retries the whole batch BEFORE any of the
    #    non-idempotent counters below have been touched.
    if deps.opensearch is not None:
        failed_ids = deps.opensearch.bulk([to_document(s) for s in spans])
        if failed_ids:
            raise RuntimeError(f"{len(failed_ids)} spans failed to index; retrying batch")

    # 2) anomaly detection, rollups and metrics
    alerts = detect_anomalies(deps, spans)
    write_rollups(deps, spans)
    put_metrics(deps, spans)

    # 3) re-index the anomalous agent spans so `anomaly: true` is searchable
    flagged = [to_document(s) for s in spans if s.get("anomaly")]
    if flagged and deps.opensearch is not None:
        try:
            deps.opensearch.bulk(flagged)
        except Exception:
            log.exception("could not flag anomalous spans (alert was still sent)")

    log.info(json.dumps({"msg": "batch processed", "spans": len(spans),
                         "anomalies": len(alerts)}))
    return {"batchItemFailures": []}


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    return process(event, _DEPS)
