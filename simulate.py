"""
flink_pipeline/simulate.py

Simulates the Flink pipeline locally without Apache Flink installed.
Reads from Kafka (or generates synthetic events) and writes
computed features to Redis — exactly what Flink does in production.

Use this for local dev when you don't have a Flink cluster.

Usage:
    # Synthetic events (no Kafka needed)
    python flink_pipeline/simulate.py --mode synthetic --events 500

    # Real Kafka
    python flink_pipeline/simulate.py --mode kafka
"""

import argparse
import asyncio
import json
import math
import random
import time
import logging
import msgpack
import redis.asyncio as aioredis

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

ACTION_WEIGHTS = {
    "page_view": 1, "search": 2, "filter_apply": 2,
    "product_click": 3, "cart_add": 5, "cart_remove": -2,
    "wishlist_add": 4, "checkout": 8, "purchase": 10,
}

CATEGORIES = ["shoes", "electronics", "clothing", "books", "home", "sports"]
DEVICES    = ["mobile", "desktop", "tablet"]
REFERRALS  = ["organic", "google_ads", "facebook_ads", "direct", "email"]


# ─────────────────────────────────────────────────────────────────────────────
# Synthetic event generator
# ─────────────────────────────────────────────────────────────────────────────

def generate_event(user_id: str, sku_id: str, rng: random.Random) -> dict:
    return {
        "user_id":        user_id,
        "product_id":     sku_id,
        "action":         rng.choice(list(ACTION_WEIGHTS.keys())),
        "category":       rng.choice(CATEGORIES),
        "device_type":    rng.choice(DEVICES),
        "referral_source": rng.choice(REFERRALS),
        "session_id":     f"sess_{user_id}_{int(time.time() // 1800)}",
        "ts":             int(time.time() * 1000),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Stateful feature computation (mirrors Flink operators)
# ─────────────────────────────────────────────────────────────────────────────

class InMemoryUserState:
    """Mirrors Flink's keyed ValueState per user_id."""
    def __init__(self):
        self.engagement_score = 0.0
        self.category_affinity: dict[str, float] = {}
        self.action_count = 0
        self.last_event_ts = 0


def compute_user_features(
    event: dict, state: InMemoryUserState
) -> dict:
    SESSION_GAP_MS = 30 * 60 * 1000
    ALPHA = 0.3

    now_ms = event["ts"]
    action = event.get("action", "page_view")
    category = event.get("category")

    # Session gap reset
    if now_ms - state.last_event_ts > SESSION_GAP_MS:
        state.engagement_score = 0.0
        state.action_count = 0
    state.last_event_ts = now_ms

    # Engagement score
    weight = ACTION_WEIGHTS.get(action, 1)
    state.engagement_score = min(1.0, state.engagement_score + weight * 0.05)

    # Category affinity EWMA
    if category:
        current = state.category_affinity.get(category, 0.0)
        state.category_affinity[category] = ALPHA * 1.0 + (1 - ALPHA) * current

    state.action_count += 1

    # Purchase intent logistic
    cart_signal = 1.0 if action in ("cart_add", "wishlist_add", "checkout") else 0.0
    logit = (
        0.5 * state.engagement_score
        + 1.5 * cart_signal
        + 0.2 * min(state.action_count / 20.0, 1.0)
        - 0.3
    )
    intent_prob = 1.0 / (1.0 + math.exp(-logit))

    top_affinity = max(state.category_affinity.values(), default=0.0)

    # Segment WTP
    referral = event.get("referral_source", "direct")
    segment_wtp = {"google_ads": 0.75, "facebook_ads": 0.72}.get(referral, 0.60)

    return {
        "engagement_score":  round(state.engagement_score, 4),
        "intent_prob":       round(intent_prob, 4),
        "category_affinity": round(top_affinity, 4),
        "segment_wtp":       round(segment_wtp, 4),
    }


def compute_sku_features(
    sku_id: str, demand_counter: dict
) -> dict:
    velocity = demand_counter.get(sku_id, 0.0)
    rng = random.Random(hash(sku_id) % 10000)
    return {
        "demand_velocity_1h":     round(min(velocity, 500.0), 2),
        "competitor_price_delta": round(rng.uniform(-0.10, 0.08), 4),
        "inventory_ratio":        round(rng.uniform(0.1, 0.9), 4),
        "days_to_restock":        round(rng.uniform(1, 20), 1),
        "category_elasticity":    round(rng.uniform(-2.5, -0.3), 4),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Simulation runner
# ─────────────────────────────────────────────────────────────────────────────

async def run_synthetic(
    redis_host: str, redis_port: int,
    n_users: int, n_skus: int, n_events: int,
):
    r = await aioredis.from_url(
        f"redis://{redis_host}:{redis_port}/0",
        decode_responses=False,
    )
    logger.info(f"Simulating {n_events} events for {n_users} users / {n_skus} SKUs")

    rng = random.Random(42)
    user_ids = [f"user_{i:04d}" for i in range(n_users)]
    sku_ids  = [f"sku_{i:04d}"  for i in range(n_skus)]

    user_states: dict[str, InMemoryUserState] = {}
    demand_counter: dict[str, float] = {}

    for i in range(n_events):
        user_id = rng.choice(user_ids)
        sku_id  = rng.choice(sku_ids)
        event   = generate_event(user_id, sku_id, rng)

        # Compute user features (stateful)
        if user_id not in user_states:
            user_states[user_id] = InMemoryUserState()
        user_feat = compute_user_features(event, user_states[user_id])

        # Update demand counter
        action = event.get("action", "page_view")
        if action in ("page_view", "cart_add", "product_click"):
            demand_counter[sku_id] = demand_counter.get(sku_id, 0) + ACTION_WEIGHTS.get(action, 1)

        # Compute SKU features
        sku_feat = compute_sku_features(sku_id, demand_counter)

        # Write to Redis (pipeline for efficiency)
        pipe = r.pipeline()
        pipe.setex(f"user:{user_id}", 1800, msgpack.packb(user_feat))
        pipe.setex(f"sku:{sku_id}",   300,  msgpack.packb(sku_feat))
        await pipe.execute()

        if (i + 1) % 100 == 0:
            logger.info(f"  Processed {i+1}/{n_events} events")

    await r.aclose()
    logger.info(f"Simulation complete — {n_users} users and {n_skus} SKUs written to Redis")


async def run_kafka(
    kafka_bootstrap: str, kafka_topic: str,
    redis_host: str, redis_port: int,
):
    try:
        from aiokafka import AIOKafkaConsumer
    except ImportError:
        logger.error("aiokafka not installed: pip install aiokafka")
        return

    r = await aioredis.from_url(
        f"redis://{redis_host}:{redis_port}/0",
        decode_responses=False,
    )

    consumer = AIOKafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_bootstrap,
        group_id="dpe-simulator",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    await consumer.start()
    logger.info(f"Consuming from {kafka_topic} at {kafka_bootstrap}")

    user_states: dict[str, InMemoryUserState] = {}
    demand_counter: dict[str, float] = {}

    try:
        async for msg in consumer:
            event = msg.value
            user_id = event.get("user_id")
            sku_id  = event.get("product_id")
            if not user_id:
                continue

            if user_id not in user_states:
                user_states[user_id] = InMemoryUserState()

            user_feat = compute_user_features(event, user_states[user_id])

            action = event.get("action", "page_view")
            if sku_id and action in ("page_view", "cart_add", "product_click"):
                demand_counter[sku_id] = demand_counter.get(sku_id, 0) + ACTION_WEIGHTS.get(action, 1)

            pipe = r.pipeline()
            pipe.setex(f"user:{user_id}", 1800, msgpack.packb(user_feat))
            if sku_id:
                sku_feat = compute_sku_features(sku_id, demand_counter)
                pipe.setex(f"sku:{sku_id}", 300, msgpack.packb(sku_feat))
            await pipe.execute()

    finally:
        await consumer.stop()
        await r.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode",      choices=["synthetic", "kafka"], default="synthetic")
    parser.add_argument("--redis-host", default="localhost")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--users",      type=int, default=100)
    parser.add_argument("--skus",       type=int, default=50)
    parser.add_argument("--events",     type=int, default=1000)
    parser.add_argument("--kafka-bootstrap", default="localhost:9092")
    parser.add_argument("--kafka-topic",     default="user-events")
    args = parser.parse_args()

    if args.mode == "synthetic":
        asyncio.run(run_synthetic(
            args.redis_host, args.redis_port,
            args.users, args.skus, args.events,
        ))
    else:
        asyncio.run(run_kafka(
            args.kafka_bootstrap, args.kafka_topic,
            args.redis_host, args.redis_port,
        ))
