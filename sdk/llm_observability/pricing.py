"""Token cost model.

Prices are USD per 1,000 tokens (input, output). They are approximate
on-demand Bedrock list prices and are meant as a starting point — check the
current AWS Bedrock pricing page and update this table (or override it with
the LLM_OBS_PRICING_JSON environment variable) before relying on the numbers.

Cost attribution formula used everywhere in this project:

    cost_usd = input_tokens / 1000 * input_price + output_tokens / 1000 * output_price
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ModelPrice:
    input_per_1k: float
    output_per_1k: float


DEFAULT_PRICING: dict[str, ModelPrice] = {
    # Amazon Nova
    "amazon.nova-micro-v1:0": ModelPrice(0.000035, 0.00014),
    "amazon.nova-lite-v1:0": ModelPrice(0.00006, 0.00024),
    "amazon.nova-pro-v1:0": ModelPrice(0.0008, 0.0032),
    # Amazon Titan
    "amazon.titan-text-express-v1": ModelPrice(0.0002, 0.0006),
    # Anthropic Claude on Bedrock
    "anthropic.claude-3-haiku-20240307-v1:0": ModelPrice(0.00025, 0.00125),
    "anthropic.claude-3-5-haiku-20241022-v1:0": ModelPrice(0.0008, 0.004),
    "anthropic.claude-3-5-sonnet-20240620-v1:0": ModelPrice(0.003, 0.015),
    # Meta Llama
    "meta.llama3-8b-instruct-v1:0": ModelPrice(0.0003, 0.0006),
    # Mock model used by tests and the canary in mock mode
    "mock.model": ModelPrice(0.001, 0.002),
}

FALLBACK_PRICE = ModelPrice(0.001, 0.002)


def _normalise(model_id: str) -> str:
    """Strip cross-region inference profile prefixes like 'us.' or 'eu.'."""
    for prefix in ("us.", "eu.", "apac.", "global."):
        if model_id.startswith(prefix):
            return model_id[len(prefix):]
    return model_id


def load_pricing() -> dict[str, ModelPrice]:
    pricing = dict(DEFAULT_PRICING)
    override = os.environ.get("LLM_OBS_PRICING_JSON")
    if override:
        for model, prices in json.loads(override).items():
            pricing[model] = ModelPrice(float(prices[0]), float(prices[1]))
    return pricing


def price_for(model_id: str, pricing: dict[str, ModelPrice] | None = None) -> ModelPrice:
    pricing = pricing or load_pricing()
    return pricing.get(model_id) or pricing.get(_normalise(model_id)) or FALLBACK_PRICE


def compute_cost(model_id: str, input_tokens: int, output_tokens: int,
                 pricing: dict[str, ModelPrice] | None = None) -> float:
    p = price_for(model_id, pricing)
    cost = input_tokens / 1000.0 * p.input_per_1k + output_tokens / 1000.0 * p.output_per_1k
    return round(cost, 8)
