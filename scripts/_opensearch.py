"""Shared SigV4-signed OpenSearch helper for the CLI scripts."""
from __future__ import annotations

import json
import os
from typing import Any

import boto3
import urllib3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest


def call(method: str, path: str, body: Any = None, endpoint: str | None = None,
         headers: dict[str, str] | None = None) -> Any:
    endpoint = endpoint or os.environ["LLM_OBS_OPENSEARCH"]
    base = endpoint if endpoint.startswith("http") else f"https://{endpoint}"
    region = os.environ.get("AWS_REGION", boto3.Session().region_name or "us-east-1")
    data = body if isinstance(body, (str, type(None))) else json.dumps(body)
    hdrs = {"Content-Type": "application/json", **(headers or {})}
    req = AWSRequest(method=method, url=base + path, data=data, headers=hdrs)
    SigV4Auth(boto3.Session().get_credentials(), "es", region).add_auth(req)
    resp = urllib3.PoolManager().request(method, base + path, body=data, headers=dict(req.headers))
    payload = json.loads(resp.data or b"{}")
    if resp.status >= 300:
        raise SystemExit(f"{method} {path} -> {resp.status}: {json.dumps(payload)[:800]}")
    return payload
