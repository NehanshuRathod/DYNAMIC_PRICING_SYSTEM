from prometheus_client import Counter, Histogram, Gauge, Summary

# ── Request metrics ───────────────────────────────────────────────────────────

PRICING_REQUESTS_TOTAL = Counter(
    "dpe_pricing_requests_total",
    "Total pricing requests received",
    ["sku_id", "variant_id", "is_fallback"],
)

PRICING_LATENCY_SECONDS = Histogram(
    "dpe_pricing_latency_seconds",
    "End-to-end pricing request latency",
    ["variant_id"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1.0],
)

# ── Model metrics ─────────────────────────────────────────────────────────────

MODEL_INFERENCE_LATENCY = Histogram(
    "dpe_model_inference_latency_seconds",
    "GBR ONNX inference latency",
    buckets=[0.0005, 0.001, 0.002, 0.005, 0.01, 0.05],
)

MODEL_VERSION = Gauge(
    "dpe_model_version_info",
    "Current loaded model version",
    ["version"],
)

# ── Redis metrics ─────────────────────────────────────────────────────────────

REDIS_FEATURE_HIT = Counter(
    "dpe_redis_feature_hit_total",
    "Redis feature fetch hits",
    ["key_type"],   # user | sku | rules
)

REDIS_FEATURE_MISS = Counter(
    "dpe_redis_feature_miss_total",
    "Redis feature fetch misses",
    ["key_type"],
)

REDIS_LATENCY_SECONDS = Histogram(
    "dpe_redis_latency_seconds",
    "Redis fetch latency",
    buckets=[0.001, 0.002, 0.005, 0.01, 0.05],
)

# ── Validation metrics ────────────────────────────────────────────────────────

MATH_VALIDATION_FAILURES = Counter(
    "dpe_math_validation_failures_total",
    "Math validation layer failures",
    ["check_name"],  # margin_floor | monotonicity | convexity | fairness | sigma
)

RULES_ENGINE_CLAMPS = Counter(
    "dpe_rules_engine_clamps_total",
    "Business rule clamps applied",
    ["rule_type"],   # margin_floor | discount_cap | parity | competitor_delta
)

# ── Fairness metrics ──────────────────────────────────────────────────────────

FAIRNESS_SPREAD = Gauge(
    "dpe_fairness_segment_price_spread",
    "Current price spread across segments for last request",
    ["sku_id"],
)

# ── Kafka metrics ─────────────────────────────────────────────────────────────

KAFKA_EMIT_SUCCESS = Counter(
    "dpe_kafka_emit_success_total",
    "Kafka events emitted successfully",
    ["topic"],
)

KAFKA_EMIT_FAILURE = Counter(
    "dpe_kafka_emit_failure_total",
    "Kafka emit failures",
    ["topic"],
)
