import random

from anomaly import EwmaState, update


def feed(state, values, start_ms=0, **kw):
    return [update(state, v, start_ms + i * 1000, **kw) for i, v in enumerate(values)]


def test_no_alert_during_warmup():
    st = EwmaState()
    verdicts = feed(st, [100] * 5 + [10_000], warmup=20)
    assert not any(v.is_anomaly for v in verdicts)


def test_spike_after_stable_baseline_is_flagged():
    random.seed(1)
    st = EwmaState()
    feed(st, [random.gauss(1000, 50) for _ in range(50)])
    v = update(st, 5000, 10**9)
    assert v.is_anomaly and v.z_score > 3


def test_normal_noise_is_not_flagged():
    random.seed(2)
    st = EwmaState()
    verdicts = feed(st, [random.gauss(1000, 50) for _ in range(300)])
    assert sum(v.is_anomaly for v in verdicts) <= 2  # ~0.1% false positives at z=3


def test_drops_are_not_alerted():
    st = EwmaState()
    feed(st, [1000 + (i % 3) for i in range(40)])
    assert not update(st, 1, 10**9).is_anomaly


def test_cooldown_suppresses_repeat_alerts():
    st = EwmaState()
    feed(st, [1000 + (i % 7) * 10 for i in range(40)])
    first = update(st, 9000, 10**9)
    second = update(st, 9000, 10**9 + 1000)
    assert first.is_anomaly and not second.is_anomaly


def test_outlier_does_not_poison_baseline():
    st = EwmaState()
    feed(st, [1000 + (i % 7) * 10 for i in range(40)])
    mean_before = st.mean
    update(st, 50_000, 10**9)
    assert abs(st.mean - mean_before) < 0.02 * 50_000
