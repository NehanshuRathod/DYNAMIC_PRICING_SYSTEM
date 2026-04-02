"""
event_ingestion_api/app/inventory_consumer.py

Async Kafka consumer for the inventory-delta topic.
Maintains current inventory_ratio and days_to_restock per SKU in Redis.

Written by: inventory management system (external ERP/WMS)
Consumed by: this consumer → writes inv_ratio:{sku_id} to Redis
Read by: Flink SKUFeatureAggregator → SKU feature vector → DPE

Run:
    python -m event_ingestion_api.app.inventory_consumer
"""

import asyncio
import json
import logging
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


async def run(
    kafka_bootstrap: str = "localhost:9092",
    kafka_topic: str = "inventory-delta",
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
        group_id="inventory-delta-consumer",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    await consumer.start()
    logger.info(f"Inventory consumer started on {kafka_topic}")

    try:
        async for msg in consumer:
            data = msg.value
            sku_id        = data.get("sku_id")
            current_stock = data.get("current_stock")
            reorder_point = data.get("reorder_point")
            days_restock  = data.get("days_to_restock", 7.0)

            if not all([sku_id, current_stock is not None, reorder_point]):
                continue

            inv_ratio = min(1.0, current_stock / max(reorder_point, 1))

            pipe = r.pipeline()
            pipe.setex(f"inv_ratio:{sku_id}",   600, str(round(inv_ratio, 4)))
            pipe.setex(f"days_restock:{sku_id}", 600, str(round(days_restock, 1)))
            await pipe.execute()

            logger.debug(
                f"inventory:{sku_id} ratio={inv_ratio:.3f} "
                f"days_restock={days_restock}"
            )

    finally:
        await consumer.stop()
        await r.aclose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())
