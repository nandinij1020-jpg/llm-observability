"""Instrumentation wrapper around the Bedrock Runtime client.

    tracer = Tracer(service="support-bot", agent_id="support-agent",
                    emitter=KinesisEmitter("llm-obs-dev-traces"))
    bedrock = TracedBedrock(boto3.client("bedrock-runtime"), tracer)

    with tracer.agent_run(user_id="u-42", session_id="s-1"):
        resp = bedrock.converse(modelId="amazon.nova-micro-v1:0", messages=[...])

Every call emits one `llm` span with model, tokens, cost, latency, status,
prompt/completion (truncated) and stop reason.  All other client methods are
passed through untouched, so it is a drop-in replacement.
"""
from __future__ import annotations

import io
import json
from typing import Any

from .pricing import compute_cost
from .tracer import Tracer, truncate


def _messages_text(messages: list[dict[str, Any]] | None) -> str | None:
    if not messages:
        return None
    parts = []
    for m in messages:
        for block in m.get("content", []):
            if "text" in block:
                parts.append(f"{m.get('role')}: {block['text']}")
            elif "toolResult" in block:
                parts.append(f"{m.get('role')}: [toolResult]")
            elif "toolUse" in block:
                parts.append(f"{m.get('role')}: [toolUse {block['toolUse'].get('name')}]")
    return "\n".join(parts)


def _output_text(resp: dict[str, Any]) -> str | None:
    msg = (resp.get("output") or {}).get("message") or {}
    parts = []
    for block in msg.get("content", []):
        if "text" in block:
            parts.append(block["text"])
        elif "toolUse" in block:
            parts.append(f"[toolUse {block['toolUse'].get('name')}]")
    return "\n".join(parts) or None


class TracedBedrock:
    def __init__(self, client: Any, tracer: Tracer):
        self._client = client
        self._tracer = tracer

    def __getattr__(self, item: str) -> Any:  # pass-through for everything else
        return getattr(self._client, item)

    # ------------------------------------------------------------ Converse API
    def converse(self, **kwargs: Any) -> dict[str, Any]:
        model_id = kwargs.get("modelId", "unknown")
        with self._tracer.span(f"llm:{model_id}", kind="llm") as span:
            span.model_id = model_id
            if self._tracer.capture_content:
                system = kwargs.get("system")
                prompt = _messages_text(kwargs.get("messages"))
                if system:
                    prompt = f"system: {json.dumps(system)}\n{prompt}"
                span.prompt = truncate(prompt)
            if kwargs.get("toolConfig"):
                span.attributes["tools_offered"] = [
                    t.get("toolSpec", {}).get("name") for t in kwargs["toolConfig"].get("tools", [])]

            resp = self._client.converse(**kwargs)

            usage = resp.get("usage") or {}
            span.input_tokens = int(usage.get("inputTokens", 0))
            span.output_tokens = int(usage.get("outputTokens", 0))
            span.total_tokens = int(usage.get("totalTokens", span.input_tokens + span.output_tokens))
            span.cost_usd = compute_cost(model_id, span.input_tokens, span.output_tokens)
            span.stop_reason = resp.get("stopReason")
            server_latency = (resp.get("metrics") or {}).get("latencyMs")
            if server_latency is not None:
                span.attributes["bedrock_latency_ms"] = server_latency
            if self._tracer.capture_content:
                span.completion = truncate(_output_text(resp))
            return resp

    # --------------------------------------------------------- InvokeModel API
    def invoke_model(self, **kwargs: Any) -> dict[str, Any]:
        """Works for any model: token counts come from Bedrock response headers."""
        model_id = kwargs.get("modelId", "unknown")
        with self._tracer.span(f"llm:{model_id}", kind="llm") as span:
            span.model_id = model_id
            if self._tracer.capture_content:
                span.prompt = truncate(kwargs.get("body"))
            resp = self._client.invoke_model(**kwargs)

            headers = (resp.get("ResponseMetadata") or {}).get("HTTPHeaders") or {}
            span.input_tokens = int(headers.get("x-amzn-bedrock-input-token-count", 0))
            span.output_tokens = int(headers.get("x-amzn-bedrock-output-token-count", 0))
            span.total_tokens = span.input_tokens + span.output_tokens
            span.cost_usd = compute_cost(model_id, span.input_tokens, span.output_tokens)
            if "x-amzn-bedrock-invocation-latency" in headers:
                span.attributes["bedrock_latency_ms"] = int(headers["x-amzn-bedrock-invocation-latency"])

            body = resp.get("body")
            if body is not None and hasattr(body, "read"):
                raw = body.read()
                resp["body"] = io.BytesIO(raw)  # give the caller a fresh stream
                if self._tracer.capture_content:
                    span.completion = truncate(raw.decode("utf-8", errors="replace"))
            return resp
