"""
Tests for the cost estimation module.

Validates:
- Known models produce correct USD estimates
- Prefix matching works (e.g. "gemini-2.5-flash-001")
- Unknown models return 0.0 and increment the counter
- Zero tokens produce zero cost
- PRICING_TABLE has minimum expected entries
"""
import uuid

import pytest

from src.monitoring.cost import PRICING_TABLE, PricingEntry, estimate_cost


class TestEstimateCost:

    def test_gemini_flash_known_cost(self):
        # gemini-2.5-flash: $0.15/1M input, $0.60/1M output
        cost = estimate_cost('gemini-2.5-flash', input_tokens=1_000_000, output_tokens=1_000_000)
        assert abs(cost - 0.75) < 0.001  # 0.15 + 0.60

    def test_gemini_pro_known_cost(self):
        # gemini-2.5-pro: $1.25/1M input, $10.00/1M output
        cost = estimate_cost('gemini-2.5-pro', input_tokens=500_000, output_tokens=100_000)
        expected = (500_000 * 1.25 / 1_000_000) + (100_000 * 10.00 / 1_000_000)
        assert abs(cost - expected) < 0.001

    def test_gpt4o_known_cost(self):
        # gpt-4o: $2.50/1M input, $10.00/1M output
        cost = estimate_cost('gpt-4o', input_tokens=2000, output_tokens=500)
        expected = (2000 * 2.50 / 1_000_000) + (500 * 10.00 / 1_000_000)
        assert abs(cost - expected) < 0.0001

    def test_claude_sonnet_known_cost(self):
        # claude-sonnet-4: $3.00/1M input, $15.00/1M output
        cost = estimate_cost('claude-sonnet-4', input_tokens=10_000, output_tokens=5_000)
        expected = (10_000 * 3.00 / 1_000_000) + (5_000 * 15.00 / 1_000_000)
        assert abs(cost - expected) < 0.0001

    def test_zero_tokens_zero_cost(self):
        cost = estimate_cost('gemini-2.5-flash', input_tokens=0, output_tokens=0)
        assert cost == 0.0

    def test_prefix_matching(self):
        """Versioned model names like 'gemini-2.5-flash-001' should match."""
        cost = estimate_cost('gemini-2.5-flash-001', input_tokens=1000, output_tokens=500)
        expected = (1000 * 0.15 / 1_000_000) + (500 * 0.60 / 1_000_000)
        assert abs(cost - expected) < 0.0001

    def test_unknown_model_returns_zero(self):
        cost = estimate_cost('totally-unknown-model-xyz', input_tokens=10000, output_tokens=5000)
        assert cost == 0.0

    def test_unknown_model_increments_counter(self):
        from src.monitoring.cost import _get_unknown_model_counter
        counter = _get_unknown_model_counter()
        before = counter.labels(model='counter_test_model')._value.get()
        estimate_cost('counter_test_model', input_tokens=100, output_tokens=50)
        after = counter.labels(model='counter_test_model')._value.get()
        assert after == before + 1


class TestPricingTable:

    def test_minimum_entry_count(self):
        """Table should have at least 10 models."""
        assert len(PRICING_TABLE) >= 10

    def test_all_entries_are_pricing_entries(self):
        for key, entry in PRICING_TABLE.items():
            assert isinstance(entry, PricingEntry), f"{key} is not a PricingEntry"

    def test_all_prices_positive(self):
        for key, entry in PRICING_TABLE.items():
            assert entry.input_per_m > 0, f"{key} has non-positive input price"
            assert entry.output_per_m > 0, f"{key} has non-positive output price"

    def test_output_more_expensive_than_input(self):
        """For all known models, output tokens cost more than input tokens."""
        for key, entry in PRICING_TABLE.items():
            assert entry.output_per_m >= entry.input_per_m, (
                f"{key}: output ({entry.output_per_m}) cheaper than input ({entry.input_per_m})"
            )

    def test_gemini_flash_cheaper_than_pro(self):
        flash = PRICING_TABLE['gemini-2.5-flash']
        pro = PRICING_TABLE['gemini-2.5-pro']
        assert flash.input_per_m < pro.input_per_m
        assert flash.output_per_m < pro.output_per_m
