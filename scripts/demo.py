"""Generate realistic agent traffic through the instrumented SDK.

Three demo agents, five users, real multi-step tool-calling loops on Bedrock:

  support-agent    – Converse tool-use loop: LLM -> tool(s) -> LLM
  summarizer-agent – single LLM call
  research-agent   – plan (LLM) -> search tool -> answer (LLM)

It also injects the failure modes the platform must catch:
  * a flaky tool that raises ~15% of the time      -> error rate
  * an occasional very slow tool                    -> latency anomaly
  * (phase "spike") long, expensive prompts         -> cost anomaly

Usage
  export LLM_OBS_STREAM=$(terraform -chdir=infra output -raw trace_stream_name)
  python scripts/demo.py                      # 40 runs against real Bedrock
  python scripts/demo.py --runs 60 --spike    # baseline, then an anomaly burst
  python scripts/demo.py --mock               # no Bedrock cost, still ships to Kinesis
  python scripts/demo.py --mock --stdout      # no AWS at all: print spans
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import random
import sys
import time
import uuid
from typing import Any

sys.path.insert(0, str(pathlib.Path(__file__).parents[1] / "sdk"))
from llm_observability import KinesisEmitter, StdoutEmitter, TracedBedrock, Tracer  # noqa: E402

MODEL = os.environ.get("LLM_OBS_MODEL", "amazon.nova-micro-v1:0")
USERS = ["u-anita", "u-rahul", "u-meera", "u-john", "u-li"]

TOOLS = {"tools": [
    {"toolSpec": {"name": "get_order_status", "description": "Look up the status of an order by id",
                  "inputSchema": {"json": {"type": "object", "properties": {"order_id": {"type": "string"}},
                                           "required": ["order_id"]}}}},
    {"toolSpec": {"name": "get_refund_policy", "description": "Return the refund policy text",
                  "inputSchema": {"json": {"type": "object", "properties": {}}}}},
]}

QUESTIONS = [
    "Where is my order A-1042?", "Can I get a refund on order B-77?",
    "What's your refund policy?", "Order C-3 hasn't arrived, what's going on?",
]
DOCS = [
    "Kinesis Data Streams is a serverless streaming service. Producers write records to shards; "
    "consumers such as Lambda read them in order per shard.",
    "OpenTelemetry defines traces made of spans. Each span has a trace id, span id and parent id, "
    "allowing distributed systems to reconstruct a request's path.",
]


class MockBedrock:
    """Converse-compatible fake with realistic token counts and latency."""

    def __init__(self, slow_factor: float = 1.0):
        self.slow_factor = slow_factor

    def converse(self, **kw: Any) -> dict[str, Any]:
        prompt_chars = sum(len(b.get("text", "")) for m in kw["messages"] for b in m["content"])
        in_tok = 40 + prompt_chars // 4
        out_tok = random.randint(20, 120) * (3 if self.slow_factor > 1 else 1)
        time.sleep(random.uniform(0.15, 0.6) * self.slow_factor)
        wants_tool = kw.get("toolConfig") and not any(
            "toolResult" in b for m in kw["messages"] for b in m["content"])
        if wants_tool:
            content = [{"toolUse": {"toolUseId": uuid.uuid4().hex[:8], "name": "get_order_status",
                                    "input": {"order_id": "A-1042"}}}]
            stop = "tool_use"
        else:
            content, stop = [{"text": "Here is the answer. " * (out_tok // 5)}], "end_turn"
        return {"output": {"message": {"role": "assistant", "content": content}}, "stopReason": stop,
                "usage": {"inputTokens": in_tok, "outputTokens": out_tok, "totalTokens": in_tok + out_tok},
                "metrics": {"latencyMs": 300}}


def make_agents(emitter: Any, bedrock_client: Any) -> dict[str, Any]:
    support = Tracer(service="demo-app", agent_id="support-agent", emitter=emitter)
    summarizer = Tracer(service="demo-app", agent_id="summarizer-agent", emitter=emitter)
    research = Tracer(service="demo-app", agent_id="research-agent", emitter=emitter)

    # ---------------- tools (traced with the decorator)
    @support.tool()
    def get_order_status(order_id: str) -> dict:
        if random.random() < 0.15:
            raise TimeoutError("orders-service timed out")
        if random.random() < 0.05:
            time.sleep(4)  # rare slow dependency -> latency anomaly
        time.sleep(random.uniform(0.05, 0.2))
        return {"order_id": order_id, "status": random.choice(["shipped", "processing", "delivered"])}

    @support.tool()
    def get_refund_policy() -> str:
        return "Refunds within 30 days of delivery, unused items only."

    @research.tool("web_search")
    def web_search(query: str) -> list[str]:
        time.sleep(random.uniform(0.1, 0.4))
        return random.sample(DOCS, 1)

    tool_impls = {"get_order_status": get_order_status, "get_refund_policy": get_refund_policy}

    def run_support(user: str, session: str, question: str) -> None:
        br = TracedBedrock(bedrock_client, support)
        with support.agent_run("support_request", user_id=user, session_id=session):
            messages = [{"role": "user", "content": [{"text": question}]}]
            for _ in range(4):  # agent loop with a hard step limit
                resp = br.converse(modelId=MODEL, messages=messages, toolConfig=TOOLS,
                                   system=[{"text": "You are a concise customer support agent. Use tools."}],
                                   inferenceConfig={"maxTokens": 300, "temperature": 0.2})
                msg = resp["output"]["message"]
                messages.append(msg)
                if resp["stopReason"] != "tool_use":
                    return
                results = []
                for block in msg["content"]:
                    if "toolUse" in block:
                        tu = block["toolUse"]
                        out = tool_impls[tu["name"]](**(tu.get("input") or {}))
                        results.append({"toolResult": {"toolUseId": tu["toolUseId"],
                                                       "content": [{"json": {"result": out}}]}})
                messages.append({"role": "user", "content": results})

    def run_summarizer(user: str, session: str, long: bool = False) -> None:
        br = TracedBedrock(bedrock_client, summarizer)
        text = " ".join(DOCS) * (30 if long else 1)
        with summarizer.agent_run("summarize", user_id=user, session_id=session):
            br.converse(modelId=MODEL, inferenceConfig={"maxTokens": 600 if long else 150},
                        messages=[{"role": "user", "content": [{"text": f"Summarise:\n{text}"}]}])

    def run_research(user: str, session: str, question: str) -> None:
        br = TracedBedrock(bedrock_client, research)
        with research.agent_run("research", user_id=user, session_id=session):
            plan = br.converse(modelId=MODEL, inferenceConfig={"maxTokens": 100}, messages=[
                {"role": "user", "content": [{"text": f"Write one search query for: {question}"}]}])
            query = (plan["output"]["message"]["content"][0].get("text") or question)[:200]
            docs = web_search(query)
            br.converse(modelId=MODEL, inferenceConfig={"maxTokens": 250}, messages=[
                {"role": "user", "content": [{"text": f"Using these notes: {docs}\nAnswer: {question}"}]}])

    return {"support": run_support, "summarizer": run_summarizer, "research": run_research,
            "tracers": [support, summarizer, research]}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=40)
    ap.add_argument("--mock", action="store_true", help="fake Bedrock (no model cost)")
    ap.add_argument("--stdout", action="store_true", help="print spans instead of sending to Kinesis")
    ap.add_argument("--spike", action="store_true", help="after the baseline, send an anomalous burst")
    ap.add_argument("--delay", type=float, default=0.3, help="seconds between runs")
    args = ap.parse_args()

    if args.stdout:
        emitter = StdoutEmitter()
    else:
        stream = os.environ.get("LLM_OBS_STREAM")
        if not stream:
            raise SystemExit("set LLM_OBS_STREAM (terraform output trace_stream_name) or use --stdout")
        emitter = KinesisEmitter(stream, batch_size=20)

    if args.mock:
        client = MockBedrock()
    else:
        import boto3
        client = boto3.client("bedrock-runtime")

    agents = make_agents(emitter, client)
    ok = failed = 0

    def one(i: int) -> None:
        nonlocal ok, failed
        user = random.choice(USERS)
        session = f"s-{user}-{i // 5}"
        kind = random.choices(["support", "summarizer", "research"], weights=[5, 3, 2])[0]
        try:
            if kind == "support":
                agents["support"](user, session, random.choice(QUESTIONS))
            elif kind == "summarizer":
                agents["summarizer"](user, session)
            else:
                agents["research"](user, session, "How does distributed tracing work for LLM agents?")
            ok += 1
        except Exception as exc:  # errors are recorded on the span; keep going
            failed += 1
            print(f"run {i} ({kind}) failed: {type(exc).__name__}: {exc}")

    print(f"baseline: {args.runs} runs ({'mock' if args.mock else MODEL})")
    for i in range(args.runs):
        one(i)
        if i % 10 == 9:
            print(f"  {i + 1} runs sent")
        time.sleep(args.delay)

    if args.spike:
        print("spike: 5 unusually long/expensive summarizer runs (should trigger cost + latency anomalies)")
        if args.mock:
            agents_spike = make_agents(emitter, MockBedrock(slow_factor=8))
        else:
            agents_spike = agents
        for i in range(5):
            try:
                agents_spike["summarizer"](random.choice(USERS), f"s-spike-{i}", long=True)
            except Exception as exc:
                print(f"spike run failed: {exc}")
            time.sleep(args.delay)

    for t in agents["tracers"]:
        t.flush()
    print(json.dumps({"ok": ok, "failed": failed}))


if __name__ == "__main__":
    main()
