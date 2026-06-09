"""
LLM Cost Estimation Module

Static pricing table for models used by the Viperine pipeline.
Provides a single `estimate_cost()` function that converts token counts
to estimated USD spend.

Aligned with RapporteurRAG's cost.py pattern.

Usage:
    from monitoring.cost import estimate_cost

    cost_usd = estimate_cost('gemini-2.5-flash', input_tokens=1500, output_tokens=300)
"""

import logging
from dataclasses import dataclass
from typing import Optional

from prometheus_client import Counter

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PricingEntry:
    """Per-million token pricing for a model."""
    input_per_m: float   # USD per 1M input tokens
    output_per_m: float  # USD per 1M output tokens


# ── Pricing Table ─────────────────────────────────────────────────────────────
# Source: provider pricing pages as of 2026-06.
# Update when prices change; old sessions will use the rate at call time.
PRICING_TABLE: dict[str, PricingEntry] = {
    # Google Gemini
    "gemini-2.5-pro": PricingEntry(input_per_m=1.25, output_per_m=10.00),
    "gemini-2.5-flash": PricingEntry(input_per_m=0.15, output_per_m=0.60),
    "gemini-2.0-flash": PricingEntry(input_per_m=0.10, output_per_m=0.40),
    "gemini-1.5-pro": PricingEntry(input_per_m=1.25, output_per_m=5.00),
    "gemini-1.5-flash": PricingEntry(input_per_m=0.075, output_per_m=0.30),
    # OpenAI
    "gpt-4o": PricingEntry(input_per_m=2.50, output_per_m=10.00),
    "gpt-4o-mini": PricingEntry(input_per_m=0.15, output_per_m=0.60),
    "gpt-4.1": PricingEntry(input_per_m=2.00, output_per_m=8.00),
    "gpt-4.1-mini": PricingEntry(input_per_m=0.40, output_per_m=1.60),
    "gpt-4.1-nano": PricingEntry(input_per_m=0.10, output_per_m=0.40),
    # Anthropic
    "claude-sonnet-4": PricingEntry(input_per_m=3.00, output_per_m=15.00),
    "claude-3.5-sonnet": PricingEntry(input_per_m=3.00, output_per_m=15.00),
    "claude-3.5-haiku": PricingEntry(input_per_m=0.80, output_per_m=4.00),
}

# Counter for lookups that miss the pricing table (detects new/renamed models)
_unknown_model_counter: Optional[Counter] = None


def _get_unknown_model_counter() -> Counter:
    """Lazy-init to avoid import-time Prometheus registry side effects in tests."""
    global _unknown_model_counter
    if _unknown_model_counter is None:
        _unknown_model_counter = Counter(
            'kafka_consumer_cost_unknown_model_total',
            'Token usage for models not in the pricing table',
            ['model']
        )
    return _unknown_model_counter


def estimate_cost(
    model: str,
    input_tokens: int,
    output_tokens: int,
) -> float:
    """
    Estimate the USD cost of a single LLM call.

    Args:
        model: Model identifier (must match a key in PRICING_TABLE)
        input_tokens: Number of input/prompt tokens
        output_tokens: Number of output/completion tokens

    Returns:
        Estimated cost in USD.  Returns 0.0 for unknown models (and increments
        a Prometheus counter so we can alert on missing pricing).
    """
    entry = PRICING_TABLE.get(model)
    if entry is None:
        # Try prefix matching for versioned model names (e.g. "gemini-2.5-flash-001")
        for key in PRICING_TABLE:
            if model.startswith(key):
                entry = PRICING_TABLE[key]
                break

    if entry is None:
        logger.warning(f"Unknown model '{model}' — cannot estimate cost")
        _get_unknown_model_counter().labels(model=model).inc()
        return 0.0

    cost = (input_tokens * entry.input_per_m / 1_000_000) + (output_tokens * entry.output_per_m / 1_000_000)
    return cost
