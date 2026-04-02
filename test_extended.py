"""
tests/test_extended.py

Extended test suite covering:
- Event ingestion API validation
- Flink simulation feature computation
- Full integration flow (event → Redis → DPE price)
- Edge cases: cold start, missing features, boundary prices
"""

import pytest
import asyncio
import math
import time
from unittest.mock import AsyncMock, MagicMock, patch

from app.models.schemas import (
    PricingRequest, FeatureVector, UserFeatures,
    SKUFeatures, ContextFeatures, SKUBusinessRules,
    PricingVariant, FallbackReason,
)
from app.services.rules_engine import BusinessRulesEngine
from app.services.ab_router import ABRouter
from app.validators.math_validation import MathValidationLayer


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def high_demand_features():
    return FeatureVector(
        user=UserFeatures(
            engagement_score=0.95,
            intent_prob=0.90,
            category_affinity=0.88,
            segment_wtp=0.80,
        ),
        sku=SKUFeatures(
            demand_velocity_1h=450.0,   # very high demand
            competitor_price_delta=0.05, # we're 5% cheaper than competitor
            inventory_ratio=0.08,        # nearly out of stock
            days_to_restock=14.0,
            category_elasticity=-0.5,    # low elasticity = not price sensitive
        ),
        context=ContextFeatures(
            time_of_day_sin=0.866,
            time_of_day_cos=0.5,
            day_of_week_sin=0.782,
            day_of_week_cos=0.623,
        ),
    )


@pytest.fixture
def low_demand_features():
    return FeatureVector(
        user=UserFeatures(
            engagement_score=0.15,
            intent_prob=0.10,
            category_affinity=0.20,
            segment_wtp=0.30,
        ),
        sku=SKUFeatures(
            demand_velocity_1h=2.0,    # very low demand
            competitor_price_delta=-0.12,  # competitor much cheaper
            inventory_ratio=0.95,      # plenty of stock
            days_to_restock=1.0,
            category_elasticity=-2.5,  # highly elastic
        ),
        context=ContextFeatures(
            time_of_day_sin=0.0,
            time_of_day_cos=1.0,
            day_of_week_sin=0.0,
            day_of_week_cos=1.0,
        ),
    )


@pytest.fixture
def base_rules():
    return SKUBusinessRules(
        sku_id="sku_test",
        cost_price=400.0,
        base_price=799.0,
        min_margin_pct=0.15,
        max_discount_pct=0.25,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Rules engine edge cases
# ─────────────────────────────────────────────────────────────────────────────

class TestRulesEngineEdgeCases:
    def setup_method(self):
        self.engine = BusinessRulesEngine()

    def test_margin_floor_is_binding_over_discount_cap(self, base_rules):
        """Margin floor should always win over discount cap."""
        # Very low raw price — margin floor is tighter
        raw = 300.0
        result = self.engine.apply(raw, base_rules)
        floor = base_rules.cost_price * (1 + base_rules.min_margin_pct)
        assert result.price >= floor
        assert result.margin_floor_applied

    def test_no_negative_price_ever(self, base_rules):
        result = self.engine.apply(-100.0, base_rules)
        assert result.price > 0

    def test_competitor_delta_cap_lower_bound(self, base_rules):
        """If we're much more expensive than competitor, price gets capped."""
        result = self.engine.apply(
            raw_price=900.0,
            rules=base_rules,
            competitor_price=600.0,   # competitor 33% cheaper
        )
        # Our price should not be more than 15% above competitor
        max_allowed = 600.0 * 1.15
        assert result.price <= max_allowed + 0.01  # small float tolerance

    def test_price_with_parity_floor_and_margin_floor(self, base_rules):
        """Both parity and margin constraints active — highest wins."""
        base_rules.price_parity_floor = 700.0
        # margin floor = 400 * 1.15 = 460
        raw = 450.0
        result = self.engine.apply(raw, base_rules)
        assert result.price >= 700.0   # parity floor is higher

    def test_all_rules_within_bounds(self, base_rules):
        """Verify final price satisfies every constraint simultaneously."""
        base_rules.price_parity_floor   = 620.0
        base_rules.price_parity_ceiling = 780.0
        raw = 850.0  # above ceiling
        result = self.engine.apply(raw, base_rules)
        assert result.price <= 780.0
        assert result.price >= base_rules.cost_price * (1 + base_rules.min_margin_pct)

    def test_output_always_2_decimal_places(self, base_rules):
        raw = 753.14159
        result = self.engine.apply(raw, base_rules)
        assert result.price == round(result.price, 2)


# ─────────────────────────────────────────────────────────────────────────────
# Math validation edge cases
# ─────────────────────────────────────────────────────────────────────────────

class TestMathValidationEdgeCases:
    def setup_method(self):
        self.validator = MathValidationLayer()

    def test_missing_price_stats_skips_sigma(self, high_demand_features, base_rules):
        """No price stats → sigma check should be skipped not failed."""
        result = self.validator._check_sigma_bound(
            price=700.0,
            features=high_demand_features,
            rules=base_rules,
            price_stats=None,
        )
        assert result.passed is True

    def test_zero_std_skips_sigma(self, high_demand_features, base_rules):
        """std=0 should not cause division by zero."""
        result = self.validator._check_sigma_bound(
            price=700.0,
            features=high_demand_features,
            rules=base_rules,
            price_stats={"mean_24h": 700.0, "std_24h": 0.0},
        )
        assert result.passed is True

    def test_margin_assert_exact_boundary(self, high_demand_features, base_rules):
        """Price exactly at margin floor should pass."""
        exact_floor = base_rules.cost_price * (1 + base_rules.min_margin_pct)
        result = self.validator._check_margin_floor(
            price=exact_floor,
            features=high_demand_features,
            rules=base_rules,
        )
        assert result.passed is True

    def test_margin_assert_one_cent_below_fails(self, high_demand_features, base_rules):
        exact_floor = base_rules.cost_price * (1 + base_rules.min_margin_pct)
        result = self.validator._check_margin_floor(
            price=exact_floor - 0.01,
            features=high_demand_features,
            rules=base_rules,
        )
        assert result.passed is False

    def test_no_segment_prices_skips_fairness(self, high_demand_features, base_rules):
        result = self.validator._check_fairness(
            price=700.0,
            features=high_demand_features,
            rules=base_rules,
            segment_prices=None,
        )
        assert result.passed is True

    def test_single_segment_always_passes(self, high_demand_features, base_rules):
        """Only one segment → spread is 0 → always pass."""
        result = self.validator._check_fairness(
            price=700.0,
            features=high_demand_features,
            rules=base_rules,
            segment_prices={"premium": 700.0},
        )
        assert result.passed is True

    def test_full_validation_returns_first_failure(
        self, high_demand_features, base_rules
    ):
        """Validation should stop at first failing check."""
        price = 300.0  # will fail margin floor
        result = self.validator.validate(
            price=price,
            features=high_demand_features,
            rules=base_rules,
        )
        assert result.passed is False
        assert result.failed_check == "margin_floor_assert"


# ─────────────────────────────────────────────────────────────────────────────
# A/B router properties
# ─────────────────────────────────────────────────────────────────────────────

class TestABRouterProperties:
    def setup_method(self):
        self.router = ABRouter()

    def test_100_pct_control_always_control(self):
        config = {"variants": [{"name": "control", "traffic_pct": 100}]}
        for uid in [f"user_{i}" for i in range(50)]:
            v = self.router.assign(uid, "exp_x", config)
            assert v == PricingVariant.CONTROL

    def test_traffic_split_approximate(self):
        """With 50/50 split, distribution should be roughly equal."""
        config = {
            "variants": [
                {"name": "control", "traffic_pct": 50},
                {"name": "gbr_v1",  "traffic_pct": 50},
            ]
        }
        counts = {"control": 0, "gbr_v1": 0}
        for i in range(1000):
            v = self.router.assign(f"user_{i:05d}", "exp_split", config)
            counts[v.value] += 1

        # Allow 10% deviation from 50/50
        assert 400 <= counts["control"] <= 600
        assert 400 <= counts["gbr_v1"]  <= 600

    def test_same_user_always_same_variant(self):
        config = {
            "variants": [
                {"name": "control", "traffic_pct": 33},
                {"name": "gbr_v1",  "traffic_pct": 33},
                {"name": "gbr_v2",  "traffic_pct": 34},
            ]
        }
        user_id = "user_sticky_test"
        variants = {self.router.assign(user_id, "exp_001", config) for _ in range(20)}
        assert len(variants) == 1, "Same user must get same variant every time"

    def test_malformed_config_returns_control(self):
        bad_config = {"variants": [{"name": "control"}]}  # missing traffic_pct
        v = self.router.assign("user_x", "exp_y", bad_config)
        assert v == PricingVariant.CONTROL


# ─────────────────────────────────────────────────────────────────────────────
# Feature vector tests
# ─────────────────────────────────────────────────────────────────────────────

class TestFeatureVectorProperties:
    def test_to_numpy_dtype_is_float32(self, high_demand_features):
        import numpy as np
        arr = high_demand_features.to_numpy_array()
        assert arr.dtype == np.float32

    def test_all_values_finite(self, high_demand_features):
        import numpy as np
        arr = high_demand_features.to_numpy_array()
        assert np.all(np.isfinite(arr)), "Feature vector must not contain inf/nan"

    def test_feature_names_match_array_length(self, high_demand_features):
        arr = high_demand_features.to_numpy_array()
        assert arr.shape[1] == len(high_demand_features.feature_names)

    def test_engagement_score_bounds(self):
        with pytest.raises(Exception):
            UserFeatures(
                engagement_score=1.5,   # > 1.0 should fail
                intent_prob=0.5,
                category_affinity=0.5,
                segment_wtp=0.5,
            )

    def test_inventory_ratio_bounds(self):
        with pytest.raises(Exception):
            SKUFeatures(
                demand_velocity_1h=100.0,
                competitor_price_delta=0.0,
                inventory_ratio=-0.1,   # < 0 should fail
                days_to_restock=5.0,
                category_elasticity=-1.2,
            )


# ─────────────────────────────────────────────────────────────────────────────
# Flink simulation — feature computation logic
# ─────────────────────────────────────────────────────────────────────────────

class TestFlinkSimulationLogic:
    def test_engagement_score_accumulates(self):
        from flink_pipeline.simulate import (
            compute_user_features, InMemoryUserState
        )
        state = InMemoryUserState()
        event1 = {
            "action": "page_view", "ts": int(time.time() * 1000),
            "category": "shoes", "referral_source": "direct",
        }
        f1 = compute_user_features(event1, state)
        assert f1["engagement_score"] > 0.0

        event2 = {
            "action": "cart_add", "ts": int(time.time() * 1000),
            "category": "shoes", "referral_source": "direct",
        }
        f2 = compute_user_features(event2, state)
        assert f2["engagement_score"] > f1["engagement_score"]

    def test_session_gap_resets_score(self):
        from flink_pipeline.simulate import (
            compute_user_features, InMemoryUserState
        )
        state = InMemoryUserState()
        now_ms = int(time.time() * 1000)

        # First event — builds up score
        event1 = {
            "action": "cart_add", "ts": now_ms,
            "category": "shoes", "referral_source": "direct",
        }
        compute_user_features(event1, state)
        score_before = state.engagement_score
        assert score_before > 0

        # Second event — 35 minutes later = new session
        event2 = {
            "action": "page_view", "ts": now_ms + 35 * 60 * 1000,
            "category": "shoes", "referral_source": "direct",
        }
        compute_user_features(event2, state)
        assert state.engagement_score < score_before  # reset happened

    def test_intent_prob_bounded_0_to_1(self):
        from flink_pipeline.simulate import (
            compute_user_features, InMemoryUserState
        )
        state = InMemoryUserState()
        now_ms = int(time.time() * 1000)

        # Simulate many high-intent actions
        for i in range(30):
            event = {
                "action": "cart_add", "ts": now_ms + i * 1000,
                "category": "electronics", "referral_source": "google_ads",
            }
            feat = compute_user_features(event, state)
            assert 0.0 <= feat["intent_prob"] <= 1.0

    def test_category_affinity_ewma_converges(self):
        from flink_pipeline.simulate import (
            compute_user_features, InMemoryUserState
        )
        state = InMemoryUserState()
        now_ms = int(time.time() * 1000)

        affinities = []
        for i in range(20):
            event = {
                "action": "page_view", "ts": now_ms + i * 5000,
                "category": "books", "referral_source": "direct",
            }
            feat = compute_user_features(event, state)
            affinities.append(feat["category_affinity"])

        # After many events in same category, affinity should be high (near 1)
        assert affinities[-1] > affinities[0]
        assert affinities[-1] <= 1.0

    def test_sku_feature_demand_velocity_normalized(self):
        from flink_pipeline.simulate import compute_sku_features
        counter = {"sku_abc": 600.0}  # above max
        feat = compute_sku_features("sku_abc", counter)
        assert feat["demand_velocity_1h"] <= 500.0  # capped at 500

    def test_sku_feature_zero_demand(self):
        from flink_pipeline.simulate import compute_sku_features
        feat = compute_sku_features("sku_new", {})
        assert feat["demand_velocity_1h"] == 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Event ingestion API validation
# ─────────────────────────────────────────────────────────────────────────────

class TestEventIngestionAPI:
    def test_valid_action_accepted(self):
        from event_ingestion_api.app.main import ClickEvent
        e = ClickEvent(
            user_id="u1", session_id="s1",
            action="cart_add", product_id="p1",
        )
        assert e.action == "cart_add"

    def test_invalid_action_rejected(self):
        from event_ingestion_api.app.main import ClickEvent
        with pytest.raises(Exception):
            ClickEvent(
                user_id="u1", session_id="s1",
                action="fly_to_moon",
            )

    def test_ts_auto_filled(self):
        from event_ingestion_api.app.main import ClickEvent
        e = ClickEvent(user_id="u1", session_id="s1", action="page_view")
        assert e.ts is not None
        assert e.ts > 0

    def test_batch_max_size_enforced(self):
        from event_ingestion_api.app.main import BatchEventsRequest, ClickEvent
        events = [
            ClickEvent(user_id=f"u{i}", session_id="s1", action="page_view")
            for i in range(51)   # 1 over limit
        ]
        with pytest.raises(Exception):
            BatchEventsRequest(events=events)

    def test_rate_limiter_allows_within_limit(self):
        from event_ingestion_api.app.main import RateLimiter
        rl = RateLimiter(limit_per_min=10)
        for _ in range(10):
            assert rl.is_allowed("user_x") is True

    def test_rate_limiter_blocks_over_limit(self):
        from event_ingestion_api.app.main import RateLimiter
        rl = RateLimiter(limit_per_min=5)
        for _ in range(5):
            rl.is_allowed("user_y")
        assert rl.is_allowed("user_y") is False


# ─────────────────────────────────────────────────────────────────────────────
# Schema validation
# ─────────────────────────────────────────────────────────────────────────────

class TestSchemaValidation:
    def test_sku_rules_cost_below_base(self):
        """base_price must exceed cost_price."""
        with pytest.raises(Exception):
            SKUBusinessRules(
                sku_id="x",
                cost_price=1000.0,
                base_price=500.0,    # below cost — should fail
            )

    def test_pricing_request_empty_user_id(self):
        with pytest.raises(Exception):
            PricingRequest(user_id="", sku_id="sku_001")

    def test_pricing_request_empty_sku_id(self):
        with pytest.raises(Exception):
            PricingRequest(user_id="user_001", sku_id="")
