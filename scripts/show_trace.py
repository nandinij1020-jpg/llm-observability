"""Print one agent trace as a tree (like the LangSmith / Langfuse trace view).

    export LLM_OBS_OPENSEARCH=$(terraform -chdir=infra output -raw opensearch_endpoint)
    python scripts/show_trace.py                 # latest trace
    python scripts/show_trace.py <trace_id>

Example output:
    agent_run  [agent] support-agent  user=u-1  2312.4 ms  $0.000412  ok
    ├── llm:amazon.nova-micro-v1:0  [llm]  812.1 ms  in=153 out=48  $0.000012  ok
    ├── tool:get_order_status  [tool]  140.2 ms  ok
    └── llm:amazon.nova-micro-v1:0  [llm]  960.0 ms  in=230 out=61  $0.000017  ok
"""
from __future__ import annotations

import pathlib
import sys
from collections import defaultdict

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from _opensearch import call  # noqa: E402


def latest_trace_id() -> str:
    res = call("POST", "/llm-traces-*/_search", {
        "size": 1, "query": {"term": {"kind": "agent"}},
        "sort": [{"start_time_ms": "desc"}], "_source": ["trace_id"]})
    hits = res["hits"]["hits"]
    if not hits:
        raise SystemExit("no traces yet")
    return hits[0]["_source"]["trace_id"]


def line(s: dict) -> str:
    parts = [s["name"], f"[{s['kind']}]"]
    if s["kind"] == "agent":
        parts += [s["agent_id"], f"user={s.get('user_id')}"]
    parts.append(f"{s.get('latency_ms', 0):.1f} ms")
    if s["kind"] == "llm":
        parts.append(f"in={s.get('input_tokens')} out={s.get('output_tokens')}")
    cost = s.get("cost_usd") or (s.get("attributes") or {}).get("rollup_cost_usd")
    if cost:
        parts.append(f"${cost:.6f}")
    parts.append(s["status"] if s["status"] == "ok" else f"ERROR {s.get('error_type')}: {s.get('error_message')}")
    if s.get("anomaly"):
        parts.append("ANOMALY")
    return "  ".join(parts)


def main() -> None:
    trace_id = sys.argv[1] if len(sys.argv) > 1 else latest_trace_id()
    res = call("POST", "/llm-traces-*/_search", {
        "size": 500, "query": {"term": {"trace_id": trace_id}},
        "sort": [{"start_time_ms": "asc"}]})
    spans = [h["_source"] for h in res["hits"]["hits"]]
    if not spans:
        raise SystemExit(f"trace {trace_id} not found")
    children = defaultdict(list)
    ids = {s["span_id"] for s in spans}
    roots = []
    for s in spans:
        (children[s["parent_span_id"]] if s.get("parent_span_id") in ids else roots).append(s)

    print(f"trace {trace_id}  ({len(spans)} spans)\n")

    def walk(span: dict, prefix: str, last: bool, top: bool) -> None:
        print((prefix + ("└── " if last else "├── ") if not top else "") + line(span))
        kids = children.get(span["span_id"], [])
        new_prefix = prefix + ("    " if last else "│   ") if not top else ""
        for i, k in enumerate(kids):
            walk(k, new_prefix, i == len(kids) - 1, False)

    for r in roots:
        walk(r, "", True, True)


if __name__ == "__main__":
    main()
