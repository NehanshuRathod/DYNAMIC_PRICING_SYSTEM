"""
event_ingestion_api/app/main.py

Thin FastAPI service that sits between the API Gateway and Kafka.
Validates, rate-limits, and produces clickstream events to Kafka.

This is the ONLY service that writes to Kafka's user-events topic.
The frontend/gateway never touches Kafka directly.

Endpoints:
  POST /v1/events          — single event
  POST /v1/events/batch    — batch up to 50 events
  GET  /health
"""

import asyncio
import json
import time
import structlog
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings
from functools import lru_cache


logger = structlog.get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Settings
# ─────────────────────────────────────────────────────────────────────────────

class Settings(BaseSettings):
    kafka_bootstrap:     str = "localhost:9092"
    kafka_topic_events:  str = "user-events"
    kafka_topic_comp:    str = "competitor-price-feed"
    kafka_topic_inv:     str = "inventory-delta"
    rate_limit_per_min:  int = 600   # per user_id
    max_batch_size:      int = 50

    class Config:
        env_file = ".env"

@lru_cache()
def get_settings() -> Settings:
    return Settings()


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic event models
# ─────────────────────────────────────────────────────────────────────────────

VALID_ACTIONS = {
    "page_view", "search", "filter_apply", "product_click",
    "cart_add", "cart_remove", "wishlist_add", "checkout",
    "purchase",
}

class ClickEvent(BaseModel):
    user_id:         str = Field(min_length=1, max_length=64)
    session_id:      str = Field(min_length=1, max_length=64)
    action:          str
    product_id:      str | None = None
    category:        str | None = None
    device_type:     str = "unknown"
    referral_source: str = "direct"
    ts:              int | None = None   # epoch ms; auto-filled if missing

    @field_validator("action")
    @classmethod
    def validate_action(cls, v):
        if v not in VALID_ACTIONS:
            raise ValueError(f"Invalid action '{v}'. Valid: {VALID_ACTIONS}")
        return v

    @field_validator("ts", mode="before")
    @classmethod
    def set_ts(cls, v):
        return v or int(time.time() * 1000)


class BatchEventsRequest(BaseModel):
    events: list[ClickEvent] = Field(min_length=1, max_length=50)


class EventResponse(BaseModel):
    accepted: int
    rejected: int = 0


# ─────────────────────────────────────────────────────────────────────────────
# Kafka producer
# ─────────────────────────────────────────────────────────────────────────────

class KafkaProducer:
    def __init__(self):
        self._producer = None
        self._available = False

    async def start(self, bootstrap: str):
        try:
            from aiokafka import AIOKafkaProducer
            self._producer = AIOKafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                compression_type="gzip",
                acks=1,
                max_batch_size=65536,
                linger_ms=5,       # micro-batching: wait 5ms to fill batch
            )
            await self._producer.start()
            self._available = True
            logger.info("kafka_producer.started", bootstrap=bootstrap)
        except ImportError:
            logger.warning("aiokafka not installed — events logged only")
        except Exception as e:
            logger.error("kafka_producer.start_failed", error=str(e))

    async def stop(self):
        if self._producer:
            await self._producer.stop()

    async def produce(self, topic: str, event: dict, key: str | None = None):
        if self._available:
            try:
                key_bytes = key.encode() if key else None
                await self._producer.send(topic, event, key=key_bytes)
            except Exception as e:
                logger.error("kafka_producer.send_failed", topic=topic, error=str(e))
        else:
            logger.debug("kafka.logged_only", topic=topic, user_id=event.get("user_id"))

    async def produce_batch(self, topic: str, events: list[dict]):
        if self._available:
            try:
                for event in events:
                    key = event.get("user_id", "").encode()
                    await self._producer.send(topic, event, key=key)
                await self._producer.flush()
            except Exception as e:
                logger.error("kafka_producer.batch_failed", error=str(e))


producer = KafkaProducer()


# ─────────────────────────────────────────────────────────────────────────────
# Simple in-memory rate limiter
# In production: use Redis with sliding window counter
# ─────────────────────────────────────────────────────────────────────────────

class RateLimiter:
    def __init__(self, limit_per_min: int):
        self._limit = limit_per_min
        self._counts: dict[str, tuple[int, float]] = {}

    def is_allowed(self, user_id: str) -> bool:
        now = time.time()
        count, window_start = self._counts.get(user_id, (0, now))

        if now - window_start > 60:
            self._counts[user_id] = (1, now)
            return True

        if count >= self._limit:
            return False

        self._counts[user_id] = (count + 1, window_start)
        return True


rate_limiter = RateLimiter(limit_per_min=600)


# ─────────────────────────────────────────────────────────────────────────────
# App
# ─────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    await producer.start(settings.kafka_bootstrap)
    yield
    await producer.stop()


app = FastAPI(
    title="Event Ingestion API",
    description="Validates and produces clickstream events to Kafka.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.post("/v1/events", response_model=EventResponse, status_code=202)
async def ingest_event(event: ClickEvent):
    """Single event ingestion. Partitioned by user_id for ordering."""
    settings = get_settings()

    if not rate_limiter.is_allowed(event.user_id):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    await producer.produce(
        settings.kafka_topic_events,
        event.model_dump(),
        key=event.user_id,
    )
    return EventResponse(accepted=1)


@app.post("/v1/events/batch", response_model=EventResponse, status_code=202)
async def ingest_batch(request: BatchEventsRequest):
    """
    Batch event ingestion. Used by frontend to flush buffered events.
    Client should buffer for 1-2 seconds then POST batch.
    """
    settings = get_settings()
    accepted = []
    rejected = 0

    for event in request.events:
        if not rate_limiter.is_allowed(event.user_id):
            rejected += 1
            continue
        accepted.append(event.model_dump())

    if accepted:
        await producer.produce_batch(settings.kafka_topic_events, accepted)

    return EventResponse(accepted=len(accepted), rejected=rejected)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "kafka_available": producer._available,
    }
