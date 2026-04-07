"""
NestMatch — unit tests for the matching engine.

Run with:
    pytest tests/test_engine.py -v
"""

import pytest
from app.models import (
    SearchRequest, BudgetInput, PropertyInput, LocationInput, LifestyleInput
)
from app.engine import (
    passes_hard_filters,
    score_price_fit,
    score_commute,
    score_size,
    run_search,
)
from app.seed_data import SEED_PROPERTIES


# ── Fixtures ──────────────────────────────────────────────────────────────────

def make_request(**overrides) -> SearchRequest:
    """Standard buyer request — easy to override individual fields."""
    defaults = dict(
        budget=BudgetInput(min_price=900000, max_price=1400000),
        property=PropertyInput(
            property_type="apartment",
            min_bedrooms=2,
            min_internal_size_sqm=95,
            new_build=False,
        ),
        location=LocationInput(
            commute_destination="Sydney CBD",
            max_commute_mins=35,
            transport_access="high",
        ),
        lifestyle=LifestyleInput(
            school_priority=True,
            noise_preference="quiet",
            parking_required=True,
        ),
    )
    defaults.update(overrides)
    return SearchRequest(**defaults)


SAMPLE_PROP = {
    "id": "test-001",
    "title": "Test apartment",
    "suburb": "Chatswood",
    "price_min": 1100000,
    "price_max": 1200000,
    "bedrooms": 2,
    "internal_size_sqm": 105,
    "property_type": "apartment",
    "parking_spaces": 1,
    "is_new_build": False,
    "commute_cbd_mins": 31,
    "distance_to_station_m": 420,
    "transport_score": 0.88,
    "school_score": 0.82,
    "noise_score": 0.75,
    "lifestyle_family_score": 0.78,
}


# ── Hard filter tests ─────────────────────────────────────────────────────────

class TestHardFilters:
    def test_passes_valid_property(self):
        req = make_request()
        assert passes_hard_filters(SAMPLE_PROP, req) is True

    def test_rejects_over_budget(self):
        req = make_request(budget=BudgetInput(min_price=800000, max_price=1000000))
        assert passes_hard_filters(SAMPLE_PROP, req) is False

    def test_rejects_wrong_type(self):
        req = make_request(
            property=PropertyInput(
                property_type="house",
                min_bedrooms=2,
                min_internal_size_sqm=95,
                new_build=False,
            )
        )
        assert passes_hard_filters(SAMPLE_PROP, req) is False

    def test_rejects_insufficient_bedrooms(self):
        req = make_request(
            property=PropertyInput(
                property_type="apartment",
                min_bedrooms=3,
                min_internal_size_sqm=95,
                new_build=False,
            )
        )
        assert passes_hard_filters(SAMPLE_PROP, req) is False

    def test_rejects_too_small_beyond_tolerance(self):
        """A property 15 sqm under minimum should be rejected."""
        small_prop = {**SAMPLE_PROP, "internal_size_sqm": 75}
        req = make_request(
            property=PropertyInput(
                property_type="apartment",
                min_bedrooms=2,
                min_internal_size_sqm=95,
                new_build=False,
            )
        )
        assert passes_hard_filters(small_prop, req) is False

    def test_admits_property_within_size_tolerance(self):
        """A property 8 sqm under minimum should be admitted (tolerance = 10)."""
        borderline = {**SAMPLE_PROP, "internal_size_sqm": 87}
        req = make_request(
            property=PropertyInput(
                property_type="apartment",
                min_bedrooms=2,
                min_internal_size_sqm=95,
                new_build=False,
            )
        )
        assert passes_hard_filters(borderline, req) is True

    def test_rejects_no_parking_when_required(self):
        no_park = {**SAMPLE_PROP, "parking_spaces": 0}
        req = make_request()  # parking_required=True
        assert passes_hard_filters(no_park, req) is False

    def test_admits_no_parking_when_not_required(self):
        no_park = {**SAMPLE_PROP, "parking_spaces": 0}
        req = make_request(
            lifestyle=LifestyleInput(
                school_priority=True,
                noise_preference="quiet",
                parking_required=False,
            )
        )
        assert passes_hard_filters(no_park, req) is True

    def test_rejects_new_build_when_buyer_wants_established(self):
        new_prop = {**SAMPLE_PROP, "is_new_build": True}
        req = make_request()  # new_build=False
        assert passes_hard_filters(new_prop, req) is False

    def test_new_build_preference_none_admits_anything(self):
        new_prop = {**SAMPLE_PROP, "is_new_build": True}
        req = make_request(
            property=PropertyInput(
                property_type="apartment",
                min_bedrooms=2,
                min_internal_size_sqm=95,
                new_build=None,
            )
        )
        assert passes_hard_filters(new_prop, req) is True


# ── Feature score tests ───────────────────────────────────────────────────────

class TestFeatureScores:
    def test_perfect_price_fit(self):
        """Property priced at the midpoint of the budget should score high."""
        req = make_request(budget=BudgetInput(min_price=1000000, max_price=1300000))
        mid_prop = {**SAMPLE_PROP, "price_min": 1140000, "price_max": 1160000}
        score = score_price_fit(mid_prop, req)
        assert score >= 0.90

    def test_over_budget_scores_zero(self):
        req = make_request(budget=BudgetInput(min_price=800000, max_price=1000000))
        score = score_price_fit(SAMPLE_PROP, req)
        assert score == 0.0

    def test_commute_well_under_limit_scores_one(self):
        req = make_request(
            location=LocationInput(
                commute_destination="Sydney CBD",
                max_commute_mins=60,
                transport_access="high",
            )
        )
        prop = {**SAMPLE_PROP, "commute_cbd_mins": 20}
        assert score_commute(prop, req) == 1.0

    def test_commute_over_limit_penalised(self):
        req = make_request(
            location=LocationInput(
                commute_destination="Sydney CBD",
                max_commute_mins=35,
                transport_access="high",
            )
        )
        prop = {**SAMPLE_PROP, "commute_cbd_mins": 50}
        assert score_commute(prop, req) < 0.5

    def test_size_at_ideal_scores_one(self):
        req = make_request(
            property=PropertyInput(
                property_type="apartment",
                min_bedrooms=2,
                min_internal_size_sqm=95,
                new_build=False,
            )
        )
        # Ideal = 95 * 1.2 = 114 sqm
        prop = {**SAMPLE_PROP, "internal_size_sqm": 114}
        assert score_size(prop, req) == 1.0

    def test_size_well_below_minimum_scores_zero(self):
        req = make_request(
            property=PropertyInput(
                property_type="apartment",
                min_bedrooms=2,
                min_internal_size_sqm=95,
                new_build=False,
            )
        )
        prop = {**SAMPLE_PROP, "internal_size_sqm": 70}
        assert score_size(prop, req) == 0.0


# ── Integration tests (full pipeline) ────────────────────────────────────────

class TestFullPipeline:
    def test_returns_results_for_valid_request(self):
        req = make_request()
        results, total = run_search(SEED_PROPERTIES, req)
        assert total == len(SEED_PROPERTIES)
        assert len(results) > 0

    def test_results_are_sorted_descending(self):
        req = make_request()
        results, _ = run_search(SEED_PROPERTIES, req)
        scores = [r.match_score for r in results]
        assert scores == sorted(scores, reverse=True)

    def test_all_results_have_highlights(self):
        req = make_request()
        results, _ = run_search(SEED_PROPERTIES, req)
        for r in results:
            assert len(r.highlights) >= 1, f"{r.property_id} has no highlights"

    def test_penrith_house_filtered_when_type_is_apartment(self):
        """Penrith house (prop-008) must not appear when buyer wants apartment."""
        req = make_request()
        results, _ = run_search(SEED_PROPERTIES, req)
        ids = [r.property_id for r in results]
        assert "prop-008" not in ids

    def test_new_build_filtered_when_buyer_wants_established(self):
        """Parramatta new build (prop-004) must not appear when new_build=False."""
        req = make_request()
        results, _ = run_search(SEED_PROPERTIES, req)
        ids = [r.property_id for r in results]
        assert "prop-004" not in ids

    def test_no_results_for_impossible_criteria(self):
        """No Sydney apartment costs $200k — should return zero results."""
        req = make_request(budget=BudgetInput(min_price=100000, max_price=200000))
        results, _ = run_search(SEED_PROPERTIES, req)
        assert len(results) == 0

    def test_chatswood_scores_higher_than_epping_for_short_commute(self):
        """
        When commute limit is tight (35 min), Chatswood (31 min) should
        outscore Epping (42 min) all else being equal.
        """
        req = make_request(
            location=LocationInput(
                commute_destination="Sydney CBD",
                max_commute_mins=35,
                transport_access="high",
            )
        )
        results, _ = run_search(SEED_PROPERTIES, req)
        id_score = {r.property_id: r.match_score for r in results}

        # prop-001 = Chatswood, prop-002 = Epping
        if "prop-001" in id_score and "prop-002" in id_score:
            assert id_score["prop-001"] > id_score["prop-002"]

    def test_score_within_valid_range(self):
        req = make_request()
        results, _ = run_search(SEED_PROPERTIES, req)
        for r in results:
            assert 0 <= r.match_score <= 100
