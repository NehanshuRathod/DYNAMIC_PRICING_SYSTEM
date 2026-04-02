"""
flink_pipeline/main.py

Apache Flink (PyFlink) streaming pipeline.
Reads raw clickstream events from Kafka.
Computes features in real-time using 3 parallel operators.
Writes computed feature vectors into Redis for DPE consumption.

Topology:
  Kafka source (user-events topic)
      ↓
  Event deserializer + schema validator
      ↓
  ┌────────────────────────────────────────┐
  │         KeyedStream by user_id         │
  ├──────────────┬────────────┬────────────┤
  │ Enrichment   │  Feature   │  Session   │
  │ (AsyncIO     │  compute   │  tracking  │
  │  user profile│  EWMA +    │  gap win   │
  │  join)       │  velocity  │  30min     │
  └──────────────┴────────────┴────────────┘
      ↓
  Redis sink (msgpack serialized)
      ↓
  Session summary → Kafka (for offline training)

Usage:
    flink run -py flink_pipeline/main.py \\
        --kafka-bootstrap localhost:9092 \\
        --redis-host localhost \\
        --redis-port 6379
"""

import argparse
import json
import logging
import math
import time
from datetime import datetime, timezone

# PyFlink imports — guarded for environments without Flink installed
try:
    from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
    from pyflink.datastream.connectors.kafka import (
        KafkaSource, KafkaOffsetsInitializer,
    )
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.common import WatermarkStrategy, Duration
    from pyflink.datastream.functions import (
        MapFunction, KeyedProcessFunction, RuntimeContext
    )
    from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
    from pyflink.common.typeinfo import Types
    FLINK_AVAILABLE = True
except ImportError:
    FLINK_AVAILABLE = False
    logging.warning("PyFlink not installed — running in simulation mode")

import redis
import msgpack

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

ACTION_WEIGHTS = {
    "page_view":    1,
    "search":       2,
    "filter_apply": 2,
    "product_click": 3,
    "cart_add":     5,
    "cart_remove":  -2,
    "wishlist_add": 4,
    "checkout":     8,
    "purchase":     10,
}

SESSION_GAP_MS   = 30 * 60 * 1000   # 30 minute gap = new session
AFFINITY_ALPHA   = 0.3              # EWMA alpha for category affinity
USER_FEATURE_TTL = 1800             # 30 min TTL in Redis
SKU_FEATURE_TTL  = 300              # 5 min TTL in Redis


# ─────────────────────────────────────────────────────────────────────────────
# Event schema
# ─────────────────────────────────────────────────────────────────────────────

def parse_event(raw: str) -> dict | None:
    """Deserialize and validate a raw Kafka event string."""
    try:
        event = json.loads(raw)
        required = {"user_id", "action", "ts"}
        if not required.issubset(event.keys()):
            logger.warning(f"Event missing required fields: {event.keys()}")
            return None
        event.setdefault("product_id", None)
        event.setdefault("category",   None)
        event.setdefault("session_id", None)
        event.setdefault("device_type", "unknown")
        event.setdefault("referral_source", "direct")
        return event
    except (json.JSONDecodeError, TypeError) as e:
        logger.warning(f"Failed to parse event: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Redis writer — shared across all sink functions
# ─────────────────────────────────────────────────────────────────────────────

class RedisWriter:
    """
    Lazy-initialized Redis connection per Flink task slot.
    Writes computed feature vectors as msgpack bytes.
    """

    def __init__(self, host: str, port: int):
        self._host = host
        self._port = port
        self._client = None

    def _ensure_connected(self):
        if self._client is None:
            self._client = redis.Redis(
                host=self._host,
                port=self._port,
                db=0,
                socket_connect_timeout=3,
                socket_timeout=1,
                retry_on_timeout=True,
            )

    def write_user_features(self, user_id: str, features: dict):
        self._ensure_connected()
        try:
            self._client.setex(
                f"user:{user_id}",
                USER_FEATURE_TTL,
                msgpack.packb(features, use_bin_type=True),
            )
        except Exception as e:
            logger.error(f"Redis write_user_features error: {e}")

    def write_sku_features(self, sku_id: str, features: dict):
        self._ensure_connected()
        try:
            self._client.setex(
                f"sku:{sku_id}",
                SKU_FEATURE_TTL,
                msgpack.packb(features, use_bin_type=True),
            )
        except Exception as e:
            logger.error(f"Redis write_sku_features error: {e}")

    def write_price_stats(self, sku_id: str, stats: dict):
        self._ensure_connected()
        try:
            self._client.setex(
                f"price_stats:{sku_id}",
                86400,
                msgpack.packb(stats, use_bin_type=True),
            )
        except Exception as e:
            logger.error(f"Redis write_price_stats error: {e}")

    def update_demand_velocity(self, sku_id: str, delta: float = 1.0):
        """
        Increment demand velocity counter for this SKU.
        Uses Redis INCRBYFLOAT for atomic updates across parallel tasks.
        """
        self._ensure_connected()
        try:
            key = f"demand_raw:{sku_id}"
            self._client.incrbyfloat(key, delta)
            self._client.expire(key, 3600)  # rolling 1-hour window
        except Exception as e:
            logger.error(f"Redis demand_velocity error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Operator 1: Event enrichment
# Joins raw event with user profile data
# ─────────────────────────────────────────────────────────────────────────────

class EventEnrichmentFunction(MapFunction):
    """
    Attaches device_type, referral_source, user_segment
    to each raw event. In production: async lookup from
    user profile service or Redis user-profile namespace.
    
    For now: rule-based segment assignment as placeholder.
    """

    def map(self, event_str: str) -> str:
        event = parse_event(event_str)
        if event is None:
            return ""

        # Enrich with computed context
        now = datetime.now(timezone.utc)
        event["hour_of_day"] = now.hour
        event["day_of_week"] = now.weekday()
        event["time_of_day_sin"] = math.sin(2 * math.pi * now.hour / 24)
        event["time_of_day_cos"] = math.cos(2 * math.pi * now.hour / 24)
        event["day_of_week_sin"] = math.sin(2 * math.pi * now.weekday() / 7)
        event["day_of_week_cos"] = math.cos(2 * math.pi * now.weekday() / 7)

        # Segment assignment (replace with Redis lookup in prod)
        device = event.get("device_type", "unknown")
        referral = event.get("referral_source", "direct")
        if referral in ("google_ads", "facebook_ads"):
            event["user_segment"] = "paid_acquisition"
        elif device == "mobile":
            event["user_segment"] = "mobile_browser"
        else:
            event["user_segment"] = "organic"

        return json.dumps(event)


# ─────────────────────────────────────────────────────────────────────────────
# Operator 2: Feature compute (KeyedProcessFunction)
# Maintains stateful session features per user
# ─────────────────────────────────────────────────────────────────────────────

class FeatureComputeFunction(KeyedProcessFunction):
    """
    Stateful operator keyed by user_id.
    Computes and updates:
      - session_engagement_score  (weighted action sum)
      - category_affinity         (EWMA per category)
      - purchase_intent_prob      (logistic scoring)
      - demand_velocity_1h        (per SKU, via Redis atomic inc)

    Writes updated feature vector to Redis after every event.
    """

    def __init__(self, redis_host: str, redis_port: int):
        self._redis_host = redis_host
        self._redis_port = redis_port
        self._redis: RedisWriter | None = None

        # Flink managed state — survives task restarts
        self._engagement_state = None
        self._affinity_state   = None
        self._session_ts_state = None
        self._action_count_state = None

    def open(self, runtime_context: RuntimeContext):
        self._redis = RedisWriter(self._redis_host, self._redis_port)

        self._engagement_state = runtime_context.get_state(
            ValueStateDescriptor("engagement_score", Types.FLOAT())
        )
        self._affinity_state = runtime_context.get_map_state(
            MapStateDescriptor("category_affinity", Types.STRING(), Types.FLOAT())
        )
        self._session_ts_state = runtime_context.get_state(
            ValueStateDescriptor("last_event_ts", Types.LONG())
        )
        self._action_count_state = runtime_context.get_state(
            ValueStateDescriptor("action_count", Types.INT())
        )

    def process_element(self, event_str: str, ctx):
        if not event_str:
            return

        event = json.loads(event_str)
        user_id  = event["user_id"]
        action   = event.get("action", "page_view")
        category = event.get("category")
        sku_id   = event.get("product_id")
        now_ms   = int(time.time() * 1000)

        # ── Session gap check ────────────────────────────────────────────
        last_ts = self._session_ts_state.value() or 0
        if now_ms - last_ts > SESSION_GAP_MS:
            # New session — reset engagement score
            self._engagement_state.update(0.0)
            self._action_count_state.update(0)

        self._session_ts_state.update(now_ms)

        # ── Engagement score ─────────────────────────────────────────────
        weight = ACTION_WEIGHTS.get(action, 1)
        current_score = self._engagement_state.value() or 0.0
        new_score = min(1.0, current_score + weight * 0.05)
        self._engagement_state.update(new_score)

        # ── Category affinity (EWMA) ─────────────────────────────────────
        if category:
            current_affinity = self._affinity_state.get(category) or 0.0
            new_affinity = (
                AFFINITY_ALPHA * 1.0 + (1 - AFFINITY_ALPHA) * current_affinity
            )
            self._affinity_state.put(category, new_affinity)

        # ── Action count ──────────────────────────────────────────────────
        count = (self._action_count_state.value() or 0) + 1
        self._action_count_state.update(count)

        # ── Purchase intent probability (logistic) ───────────────────────
        # Simple logistic: sigmoid of weighted feature sum
        cart_signal  = 1.0 if action in ("cart_add", "wishlist_add", "checkout") else 0.0
        intent_logit = (
            0.5 * new_score
            + 1.5 * cart_signal
            + 0.2 * min(count / 20.0, 1.0)
            - 0.3
        )
        intent_prob = 1.0 / (1.0 + math.exp(-intent_logit))

        # ── Category affinity for top category ───────────────────────────
        top_affinity = 0.0
        if category:
            top_affinity = self._affinity_state.get(category) or 0.0

        # ── Segment WTP proxy (based on device + referral) ───────────────
        segment = event.get("user_segment", "organic")
        wtp_map = {
            "paid_acquisition": 0.75,
            "mobile_browser":   0.55,
            "organic":          0.60,
        }
        segment_wtp = wtp_map.get(segment, 0.60)

        # ── Write user features to Redis ──────────────────────────────────
        user_features = {
            "engagement_score":  round(new_score, 4),
            "intent_prob":       round(intent_prob, 4),
            "category_affinity": round(top_affinity, 4),
            "segment_wtp":       round(segment_wtp, 4),
        }
        self._redis.write_user_features(user_id, user_features)

        # ── Update SKU demand velocity ────────────────────────────────────
        if sku_id and action in ("page_view", "cart_add", "product_click"):
            self._redis.update_demand_velocity(sku_id, delta=ACTION_WEIGHTS.get(action, 1))


# ─────────────────────────────────────────────────────────────────────────────
# Operator 3: SKU demand aggregator
# Reads Redis demand counters and writes full SKU feature vector
# ─────────────────────────────────────────────────────────────────────────────

class SKUFeatureAggregator(MapFunction):
    """
    For every event that touches a product, reads the current
    demand_raw counter from Redis and builds a full SKU feature vector.
    
    In production: competitor_price_delta comes from the
    competitor-price-feed Kafka topic (separate consumer).
    inventory_ratio comes from the inventory-delta topic.
    Here we read them from Redis where those pipelines write them.
    """

    def __init__(self, redis_host: str, redis_port: int):
        self._redis_host = redis_host
        self._redis_port = redis_port
        self._r: redis.Redis | None = None

    def open(self, runtime_context: RuntimeContext):
        self._r = redis.Redis(
            host=self._redis_host,
            port=self._redis_port,
            db=0,
            decode_responses=True,
        )

    def map(self, event_str: str) -> str:
        if not event_str:
            return ""

        event = json.loads(event_str)
        sku_id = event.get("product_id")
        if not sku_id:
            return ""

        try:
            # Raw demand counter (incremented by FeatureComputeFunction)
            raw_demand = float(self._r.get(f"demand_raw:{sku_id}") or 0)
            # Normalize to per-hour velocity (approximate)
            demand_velocity_1h = min(raw_demand, 500.0)

            # Inventory data — written by inventory-delta consumer
            inv_ratio = float(self._r.get(f"inv_ratio:{sku_id}") or 0.5)
            days_restock = float(self._r.get(f"days_restock:{sku_id}") or 7.0)

            # Competitor delta — written by competitor-feed consumer
            comp_delta = float(self._r.get(f"comp_delta:{sku_id}") or 0.0)

            # Category elasticity — static, loaded from product catalog
            elasticity = float(self._r.get(f"elasticity:{sku_id}") or -1.2)

            sku_features = {
                "demand_velocity_1h":     round(demand_velocity_1h, 2),
                "competitor_price_delta": round(comp_delta, 4),
                "inventory_ratio":        round(inv_ratio, 4),
                "days_to_restock":        round(days_restock, 1),
                "category_elasticity":    round(elasticity, 4),
            }

            # Write to Redis
            writer = RedisWriter(self._redis_host, self._redis_port)
            writer.write_sku_features(sku_id, sku_features)

            # Update price stats rolling window
            # (In prod: computed from actual transaction prices)
            existing_raw = self._r.get(f"price_stats:{sku_id}")
            if existing_raw is None:
                # Bootstrap from business rules if available
                rules_raw = self._r.get(f"rules:{sku_id}")
                if rules_raw:
                    rules = msgpack.unpackb(rules_raw.encode("latin-1")
                                           if isinstance(rules_raw, str)
                                           else rules_raw, raw=False)
                    base = rules.get("base_price", 1000.0)
                    stats = {
                        "mean_24h": round(base * 0.92, 2),
                        "std_24h":  round(base * 0.04, 2),
                        "min_24h":  round(base * 0.70, 2),
                        "max_24h":  round(base * 1.05, 2),
                    }
                    writer.write_price_stats(sku_id, stats)

        except Exception as e:
            logger.error(f"SKUFeatureAggregator error for {sku_id}: {e}")

        return event_str


# ─────────────────────────────────────────────────────────────────────────────
# Main Flink job
# ─────────────────────────────────────────────────────────────────────────────

def build_job(
    kafka_bootstrap: str,
    kafka_topic: str,
    kafka_group: str,
    redis_host: str,
    redis_port: int,
    parallelism: int = 4,
):
    if not FLINK_AVAILABLE:
        logger.error("PyFlink not available. Install apache-flink package.")
        return None

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(parallelism)

    # Enable checkpointing — at-least-once guarantees
    env.enable_checkpointing(60_000)   # checkpoint every 60 seconds

    # ── Kafka source ──────────────────────────────────────────────────────
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_topics(kafka_topic)
        .set_group_id(kafka_group)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)),
        "Kafka clickstream source",
    )

    # ── Operator 1: Event enrichment ──────────────────────────────────────
    enriched = (
        stream
        .filter(lambda x: x and len(x) > 0)
        .map(EventEnrichmentFunction(), output_type=Types.STRING())
        .name("event_enrichment")
    )

    # ── Operator 2: Feature compute (keyed by user_id) ───────────────────
    keyed = enriched.key_by(
        lambda s: json.loads(s).get("user_id", "unknown")
        if s else "unknown"
    )
    keyed.process(
        FeatureComputeFunction(redis_host, redis_port),
        output_type=Types.STRING(),
    ).name("feature_compute")

    # ── Operator 3: SKU demand aggregation ───────────────────────────────
    (
        enriched
        .filter(lambda s: s and json.loads(s).get("product_id") is not None)
        .map(SKUFeatureAggregator(redis_host, redis_port), output_type=Types.STRING())
        .name("sku_feature_aggregator")
    )

    return env


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DPE Flink pipeline")
    parser.add_argument("--kafka-bootstrap", default="localhost:9092")
    parser.add_argument("--kafka-topic",     default="user-events")
    parser.add_argument("--kafka-group",     default="dpe-flink-consumer")
    parser.add_argument("--redis-host",      default="localhost")
    parser.add_argument("--redis-port",      type=int, default=6379)
    parser.add_argument("--parallelism",     type=int, default=4)
    args = parser.parse_args()

    logger.info(f"Starting Flink pipeline — Kafka: {args.kafka_bootstrap}")
    env = build_job(
        kafka_bootstrap=args.kafka_bootstrap,
        kafka_topic=args.kafka_topic,
        kafka_group=args.kafka_group,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        parallelism=args.parallelism,
    )
    if env:
        env.execute("DPE Feature Pipeline")
