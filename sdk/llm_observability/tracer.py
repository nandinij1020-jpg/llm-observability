"""Minimal OpenTelemetry-style tracer for LLM agents.

Concepts (same as OpenTelemetry):
  * trace   – one end-to-end agent run, identified by a 32-hex-char trace_id
  * span    – one unit of work inside the trace (an LLM call, a tool call,
              the agent run itself), identified by a 16-hex-char span_id
  * parent  – each span records parent_span_id, which lets the dashboard
              rebuild the call tree of a multi-step agent chain
  * context propagation – the "current span" is kept in a contextvar, so a
              nested call automatically becomes a child of whatever span is
              active, including across asyncio tasks.  To continue a trace in
              another process (e.g. a tool running in another Lambda) pass
              `tracer.inject()` along and call `tracer.extract(...)` there.
"""
from __future__ import annotations

import contextvars
import functools
import json
import secrets
import time
import traceback
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Iterator

from .emitters import Emitter, StdoutEmitter

SCHEMA_VERSION = "1.0"
MAX_TEXT_CHARS = 2000  # truncate prompts/completions in logs

_current_span: contextvars.ContextVar["Span | None"] = contextvars.ContextVar(
    "llm_obs_current_span", default=None
)


def _new_trace_id() -> str:
    return secrets.token_hex(16)


def _new_span_id() -> str:
    return secrets.token_hex(8)


def truncate(value: Any, limit: int = MAX_TEXT_CHARS) -> str | None:
    if value is None:
        return None
    text = value if isinstance(value, str) else json.dumps(value, default=str)
    return text if len(text) <= limit else text[:limit] + f"...[truncated {len(text) - limit} chars]"


@dataclass
class Span:
    """One structured log record. Every field is flat so it indexes cleanly."""

    name: str
    kind: str                      # "agent" | "llm" | "tool" | "custom"
    trace_id: str
    span_id: str
    parent_span_id: str | None
    service: str
    environment: str
    agent_id: str
    user_id: str | None = None
    session_id: str | None = None
    start_time_ms: int = 0
    end_time_ms: int | None = None
    latency_ms: float | None = None
    status: str = "ok"             # "ok" | "error"
    error_type: str | None = None
    error_message: str | None = None
    # LLM fields
    model_id: str | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    cost_usd: float = 0.0
    prompt: str | None = None
    completion: str | None = None
    stop_reason: str | None = None
    # Tool fields
    tool_name: str | None = None
    tool_input: str | None = None
    tool_output: str | None = None
    # Free-form
    attributes: dict[str, Any] = field(default_factory=dict)
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def set_error(self, exc: BaseException) -> None:
        self.status = "error"
        self.error_type = type(exc).__name__
        self.error_message = truncate(str(exc), 500)
        self.attributes.setdefault("stacktrace", truncate("".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)), 1500))


class Tracer:
    def __init__(self, service: str, agent_id: str, environment: str = "dev",
                 emitter: Emitter | None = None, capture_content: bool = True):
        self.service = service
        self.agent_id = agent_id
        self.environment = environment
        self.emitter = emitter or StdoutEmitter()
        self.capture_content = capture_content
        # user/session for the current run (also contextvars so concurrent
        # requests in one process don't mix them up)
        self._user: contextvars.ContextVar[str | None] = contextvars.ContextVar("user", default=None)
        self._session: contextvars.ContextVar[str | None] = contextvars.ContextVar("session", default=None)
        self._remote_parent: contextvars.ContextVar[tuple[str, str] | None] = contextvars.ContextVar(
            "remote_parent", default=None)

    # ------------------------------------------------------------------ spans
    @contextmanager
    def span(self, name: str, kind: str = "custom", **attrs: Any) -> Iterator[Span]:
        parent = _current_span.get()
        remote = self._remote_parent.get()
        if parent is not None:
            trace_id, parent_id = parent.trace_id, parent.span_id
        elif remote is not None:
            trace_id, parent_id = remote
        else:
            trace_id, parent_id = _new_trace_id(), None

        span = Span(
            name=name, kind=kind, trace_id=trace_id, span_id=_new_span_id(),
            parent_span_id=parent_id, service=self.service, environment=self.environment,
            agent_id=self.agent_id, user_id=self._user.get(), session_id=self._session.get(),
            start_time_ms=int(time.time() * 1000), attributes=dict(attrs),
        )
        token = _current_span.set(span)
        started = time.perf_counter()
        try:
            yield span
        except BaseException as exc:
            span.set_error(exc)
            raise
        finally:
            span.latency_ms = round((time.perf_counter() - started) * 1000, 3)
            span.end_time_ms = span.start_time_ms + int(span.latency_ms)
            _current_span.reset(token)
            self.emitter.emit(span.to_dict())

    @contextmanager
    def agent_run(self, name: str = "agent_run", user_id: str | None = None,
                  session_id: str | None = None, **attrs: Any) -> Iterator[Span]:
        """Root span for one request to the agent. Rolls up child tokens/cost."""
        u_token = self._user.set(user_id)
        s_token = self._session.set(session_id)
        collector = _ChildCollector()
        self.emitter.add_listener(collector)
        try:
            with self.span(name, kind="agent", **attrs) as root:
                collector.trace_id = root.trace_id
                try:
                    yield root
                finally:
                    # Roll-up so the root span alone answers "what did this request cost?"
                    root.attributes["llm_calls"] = collector.llm_calls
                    root.attributes["tool_calls"] = collector.tool_calls
                    root.attributes["rollup_input_tokens"] = collector.input_tokens
                    root.attributes["rollup_output_tokens"] = collector.output_tokens
                    root.attributes["rollup_cost_usd"] = round(collector.cost, 8)
        finally:
            self.emitter.remove_listener(collector)
            self._user.reset(u_token)
            self._session.reset(s_token)

    def tool(self, name: str | None = None) -> Callable:
        """Decorator: `@tracer.tool()` records a tool-call span."""
        def decorator(fn: Callable) -> Callable:
            tool_name = name or fn.__name__

            @functools.wraps(fn)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                with self.span(f"tool:{tool_name}", kind="tool") as span:
                    span.tool_name = tool_name
                    if self.capture_content:
                        span.tool_input = truncate({"args": args, "kwargs": kwargs})
                    result = fn(*args, **kwargs)
                    if self.capture_content:
                        span.tool_output = truncate(result)
                    return result
            return wrapper
        return decorator

    # ---------------------------------------------------- context propagation
    def inject(self) -> dict[str, str]:
        """W3C traceparent header for the current span (send it to remote tools)."""
        span = _current_span.get()
        if span is None:
            return {}
        return {"traceparent": f"00-{span.trace_id}-{span.span_id}-01"}

    @contextmanager
    def extract(self, carrier: dict[str, str]) -> Iterator[None]:
        """Continue a trace started in another process."""
        header = carrier.get("traceparent", "")
        parts = header.split("-")
        token = self._remote_parent.set((parts[1], parts[2]) if len(parts) == 4 else None)
        try:
            yield
        finally:
            self._remote_parent.reset(token)

    def flush(self) -> None:
        self.emitter.flush()


def current_span() -> Span | None:
    return _current_span.get()


class _ChildCollector:
    """Listener that sums LLM usage of every span in one trace."""

    def __init__(self) -> None:
        self.trace_id: str | None = None
        self.llm_calls = self.tool_calls = self.input_tokens = self.output_tokens = 0
        self.cost = 0.0

    def __call__(self, record: dict[str, Any]) -> None:
        if record.get("trace_id") != self.trace_id or record.get("kind") == "agent":
            return
        if record.get("kind") == "llm":
            self.llm_calls += 1
            self.input_tokens += record.get("input_tokens") or 0
            self.output_tokens += record.get("output_tokens") or 0
            self.cost += record.get("cost_usd") or 0.0
        elif record.get("kind") == "tool":
            self.tool_calls += 1
