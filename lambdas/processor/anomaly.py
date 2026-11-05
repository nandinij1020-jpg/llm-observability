"""Streaming anomaly detection (statistical) for per-agent latency and cost.

Uses an exponentially weighted moving average (EWMA) of the mean and variance
and flags a point whose z-score exceeds a threshold:

    mean_t = mean_{t-1} + alpha * (x - mean_{t-1})
    var_t  = (1 - alpha) * (var_{t-1} + alpha * (x - mean_{t-1})^2)
    z      = (x - mean_{t-1}) / sqrt(var_{t-1})

EWMA adapts to slow drift (e.g. a new model version) while still catching
sudden spikes. A warm-up period avoids alerting before the baseline is
meaningful, and a cooldown stops one incident from paging repeatedly.

This complements the CloudWatch ANOMALY_DETECTION_BAND alarms (an ML model
trained by AWS on two weeks of metric history) defined in Terraform: the EWMA
detector reacts per event in seconds; the CloudWatch band is better at
seasonality (daily/weekly traffic patterns).
"""
from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass
class EwmaState:
    count: int = 0
    mean: float = 0.0
    var: float = 0.0
    last_alert_ms: int = 0


@dataclass
class Verdict:
    is_anomaly: bool
    z_score: float
    baseline_mean: float
    baseline_std: float


def update(state: EwmaState, x: float, now_ms: int, alpha: float = 0.1,
           z_threshold: float = 3.0, warmup: int = 20,
           cooldown_ms: int = 10 * 60 * 1000, min_std: float = 1e-9) -> Verdict:
    """Score x against the current baseline, then fold x into the baseline."""
    if state.count == 0:
        state.count, state.mean, state.var = 1, x, 0.0
        return Verdict(False, 0.0, x, 0.0)

    std = math.sqrt(max(state.var, 0.0))
    diff = x - state.mean
    z = diff / max(std, min_std) if std > 0 else 0.0
    # Only alert on increases (high latency / high cost); also require the
    # value to be meaningfully above the mean so tiny baselines don't page.
    anomalous = (state.count >= warmup and z > z_threshold
                 and diff > 0.05 * abs(state.mean)
                 and now_ms - state.last_alert_ms > cooldown_ms)
    verdict = Verdict(anomalous, round(z, 3), state.mean, std)

    # Don't let a single outlier poison the baseline: dampen its weight.
    a = alpha if not anomalous else alpha * 0.1
    state.mean += a * diff
    state.var = (1 - a) * (state.var + a * diff * diff)
    state.count += 1
    if anomalous:
        state.last_alert_ms = now_ms
    return verdict
