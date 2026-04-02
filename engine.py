import asyncio
import time
import structlog
from typing import Optional

from app.models.schemas import (
    PricingRequest, PricingResponse, PricingVariant,
    FallbackReason, SKUBusinessRules
)
from app.core.redis_client import redis_client
from app.core.model import gbr_model
from app.core.kafka_emitter import kafka_emitter
from app.core.metrics import (
    PRICING_REQUESTS_TOTAL, PRICING_LATENCY_SECONDS,
    MODEL_INFERENCE_LATENCY, MATH_VALIDATION_FAILURES,
    RULES_ENGINE_CLAMPS,
)
from app.services.rules_engine import rules_engine
from app.services.explainer import shap_explainer
from app.services.ab_router import ab_router
from app.validators.math_validation import math_validator
from config.settings import get_settings

logger = structlog.get_logger(__name__)
settings = get_settings()


class DynamicPricingEngine:
    """
    Black-box orchestrator. Callers only interact via:
      POST /v1/price {user_id, sku_id} -> PricingResponse

    Pipeline:
      1. A/B variant assign
      2. Redis feature fetch    (parallel asyncio.gather, ~3ms)
      3. GBR ONNX inference     (~1ms)
      4. Business rules engine  (deterministic clamp)
      5. Math validation        (5 numpy checks)
      6. SHAP explanation       (~0.5ms)
      7. Response
      8. Kafka emit             (fire-and-forget task)

    Always returns HTTP 200. Failures serve base_price + emit DLQ.
    """

    async def price(self, request: PricingRequest) -> PricingResponse:
        t_start = time.perf_counter()
        log = logger.bind(user_id=request.user_id, sku_id=request.sku_id)

        # Step 1: A/B assignment
        exp_config = None
        if request.experiment_id:
            exp_config = await redis_client.get_experiment(request.experiment_id)
        variant = ab_router.assign(
            user_id=request.user_id,
            experiment_id=request.experiment_id,
            experiment_config=exp_config,
        )

        # Step 2: Parallel Redis fetch — feature vector + SKU rules + price stats
        features, rules, price_stats = await asyncio.gather(
            redis_client.get_feature_vector(request.user_id, request.sku_id),
            redis_client.get_sku_rules(request.sku_id),
            redis_client.get_price_stats(request.sku_id),
        )

        if features is None:
            log.warning("dpe.feature_miss_fallback")
            return await self._fallback(
                request, rules, variant, FallbackReason.FEATURE_FETCH_FAILED, t_start
            )

        if rules is None:
            log.warning("dpe.rules_miss_using_defaults")
            rules = self._default_rules(request.sku_id)

        # Step 3: GBR ONNX inference
        try:
            t_infer = time.perf_counter()
            raw_prediction = gbr_model.predict(features)
            MODEL_INFERENCE_LATENCY.observe(time.perf_counter() - t_infer)
            log.info("dpe.gbr_done", raw_price=round(raw_prediction.raw_price, 2))
        except Exception as e:
            log.error("dpe.gbr_failed", error=str(e))
            return await self._fallback(
                request, rules, variant, FallbackReason.MODEL_INFERENCE_FAILED, t_start
            )

        # Step 4: Business rules engine
        try:
            adjusted = rules_engine.apply(
                raw_price=raw_prediction.raw_price,
                rules=rules,
                competitor_price=(
                    (1 + features.sku.competitor_price_delta) * rules.base_price
                ),
            )
            if adjusted.margin_floor_applied:
                RULES_ENGINE_CLAMPS.labels(rule_type="margin_floor").inc()
            if adjusted.discount_cap_applied:
                RULES_ENGINE_CLAMPS.labels(rule_type="discount_cap").inc()
            if adjusted.parity_applied:
                RULES_ENGINE_CLAMPS.labels(rule_type="parity").inc()
        except Exception as e:
            log.error("dpe.rules_failed", error=str(e))
            return await self._fallback(
                request, rules, variant, FallbackReason.RULES_ENGINE_FAILED, t_start
            )

        # Step 5: Math validation
        validation = math_validator.validate(
            price=adjusted.price,
            features=features,
            rules=rules,
            price_stats=price_stats,
        )
        if not validation.passed:
            MATH_VALIDATION_FAILURES.labels(check_name=validation.failed_check).inc()
            asyncio.create_task(kafka_emitter.emit_dlq_event(
                user_id=request.user_id,
                sku_id=request.sku_id,
                failed_check=validation.failed_check,
                message=validation.message,
                topic=settings.kafka_dlq_topic,
            ))
            return await self._fallback(
                request, rules, variant, FallbackReason.MATH_VALIDATION_FAILED, t_start
            )

        final_price = adjusted.price

        # Step 6: SHAP explanation
        explanations = shap_explainer.explain(
            features=features,
            base_price=rules.base_price,
            final_price=final_price,
        )

        # Step 7: Build response
        discount_pct = max(
            0.0,
            round((rules.base_price - final_price) / rules.base_price * 100, 2)
        )
        response = PricingResponse(
            user_id=request.user_id,
            sku_id=request.sku_id,
            final_price=final_price,
            base_price=rules.base_price,
            discount_pct=discount_pct,
            explanations=explanations,
            variant_id=variant,
            model_version=raw_prediction.model_version,
            is_fallback=False,
            ttl_sec=60,
        )

        latency = time.perf_counter() - t_start
        PRICING_LATENCY_SECONDS.labels(variant_id=variant.value).observe(latency)
        PRICING_REQUESTS_TOTAL.labels(
            sku_id=request.sku_id, variant_id=variant.value, is_fallback="false"
        ).inc()
        log.info("dpe.price_served", final_price=final_price,
                 latency_ms=round(latency * 1000, 2))

        # Step 8: Fire-and-forget async tasks
        asyncio.create_task(kafka_emitter.emit_pricing_event(
            user_id=request.user_id, sku_id=request.sku_id,
            final_price=final_price, base_price=rules.base_price,
            variant_id=variant.value, model_version=raw_prediction.model_version,
            is_fallback=False, topic=settings.kafka_pricing_topic,
        ))
        asyncio.create_task(redis_client.cache_price(
            request.user_id, request.sku_id, final_price, ttl_sec=60
        ))

        return response

    async def _fallback(
        self,
        request: PricingRequest,
        rules: Optional[SKUBusinessRules],
        variant: PricingVariant,
        reason: FallbackReason,
        t_start: float,
    ) -> PricingResponse:
        base_price = rules.base_price if rules else 0.0
        PRICING_REQUESTS_TOTAL.labels(
            sku_id=request.sku_id, variant_id=variant.value, is_fallback="true"
        ).inc()
        PRICING_LATENCY_SECONDS.labels(variant_id=variant.value).observe(
            time.perf_counter() - t_start
        )
        asyncio.create_task(kafka_emitter.emit_pricing_event(
            user_id=request.user_id, sku_id=request.sku_id,
            final_price=base_price, base_price=base_price,
            variant_id=variant.value, model_version=gbr_model.version,
            is_fallback=True, topic=settings.kafka_pricing_topic,
        ))
        return PricingResponse(
            user_id=request.user_id, sku_id=request.sku_id,
            final_price=base_price, base_price=base_price,
            discount_pct=0.0, explanations=[],
            variant_id=variant, model_version=gbr_model.version,
            is_fallback=True, fallback_reason=reason, ttl_sec=30,
        )

    def _default_rules(self, sku_id: str) -> SKUBusinessRules:
        return SKUBusinessRules(
            sku_id=sku_id,
            cost_price=100.0,
            base_price=150.0,
            min_margin_pct=settings.min_margin_pct,
            max_discount_pct=settings.max_discount_pct,
        )


dpe = DynamicPricingEngine()
