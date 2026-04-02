"""
event_ingestion_api/app/competitor_consumer.py

Async Kafka consumer for the competitor-price-feed topic.
Computes competitor_price_delta for each SKU and writes to Redis.

Written by: scraper service (external)
Consumed by: this consumer → writes comp_delta:{sku_id} to Redis
Read by: Flink SKUFeatureAggregator → SKU feature vector → DPE

Run alongside the event ingestion API:
    python -m event_ingestion_api.app.competitor_consumer
"""

import asyncio
import json
import logging
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


async def run(
    kafka_bootstrap: str = "localhost:9092",
    kafka_topic: str = "competitor-price-feed",
    redis_host: str = "localhost",
    redis_port: int = 6379,
):
    try:
        from aiokafka import AIOKafkaConsumer
    except ImportError:
        logger.error("aiokafka required: pip install aiokafka")
        return

    r = await aioredis.from_url(
        f"redis://{redis_host}:{redis_port}/0",
        decode_responses=True,
    )

    consumer = AIOKafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_bootstrap,
        group_id="competitor-feed-consumer",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    await consumer.start()
    logger.info(f"Competitor feed consumer started on {kafka_topic}")

    try:
        async for msg in consumer:
            data = msg.value
            sku_id        = data.get("sku_id")
            our_price     = data.get("our_price")
            comp_price    = data.get("competitor_price")

            if not all([sku_id, our_price, comp_price]):
                continue

            # delta = (our_price - comp_price) / our_price
            # positive = we are more expensive than competitor
            # negative = we are cheaper
            delta = (our_price - comp_price) / our_price

            await r.setex(
                f"comp_delta:{sku_id}",
                300,    # 5 min TTL
                str(round(delta, 4)),
            )
            logger.debug(f"comp_delta:{sku_id} = {delta:.4f}")

    finally:
        await consumer.stop()
        await r.aclose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())
