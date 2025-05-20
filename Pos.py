"""
risk_limits_test.py
===================

Self-contained demo + sanity checks for the dynamic-multiplier risk-limit helper.

Run directly:

    python risk_limits_test.py
"""

from __future__ import annotations
from typing import Tuple, List

try:
    import pandas as pd
except ImportError:   # drop-in fallback; pandas is just for pretty printing
    pd = None


# ─────────────────────────── HARD LIMIT CONSTANTS ────────────────────────────
MAX_TRD_BUY   = 1_000     # max shares you may BUY in a single fill
MAX_TRD_SELL  = 1_000     # max shares you may SELL in a single fill
MAX_POS_LONG  = 2_000     # absolute long-side position cap
MAX_POS_SHORT = 2_000     # absolute short-side (negative) position cap


# ─────────────────────────── LIMIT HELPER FUNCTION ───────────────────────────
def allowed_trade_limits(
    position: float,
    multiplier: float,
    *,
    max_trd_buy:   float = MAX_TRD_BUY,
    max_trd_sell:  float = MAX_TRD_SELL,
    max_pos_long:  float = MAX_POS_LONG,
    max_pos_short: float = MAX_POS_SHORT,
) -> Tuple[float, float]:
    """
    Return (max_buy, max_sell) allowed **for the next single trade**, after
    applying a throttle *multiplier* in the range [0, 1].

    • multiplier == 0   →  flatten-only (you may only reduce exposure)
    • 0 < multiplier ≤ 1 → per-trade limits **and** position caps are scaled
                           by *multiplier* and enforced.

    Parameters can be overridden per symbol/test via keyword arguments.
    """
    if not 0.0 <= multiplier <= 1.0:
        raise ValueError("multiplier must be in [0, 1]")

    # ── flatten-only mode ────────────────────────────────────────────────────
    if multiplier == 0:
        if position > 0:
            return 0.0, min(position, max_trd_sell)          # only SELL allowed
        if position < 0:
            return min(abs(position), max_trd_buy), 0.0      # only BUY allowed
        return 0.0, 0.0                                      # flat → no trade

    # ── scaled limits (0 < multiplier ≤ 1) ───────────────────────────────────
    trd_buy   = multiplier * max_trd_buy
    trd_sell  = multiplier * max_trd_sell
    pos_long  = multiplier * max_pos_long
    pos_short = multiplier * max_pos_short

    room_to_long  = max(0.0, pos_long  - position)           # room to BUY
    room_to_short = max(0.0, position + pos_short)           # room to SELL

    return min(trd_buy, room_to_long), min(trd_sell, room_to_short)


# ───────────────────────────── TEST UTILITIES ────────────────────────────────
POSITIONS:   List[int]   = [-2_500, -2_000, -1_250, -500, 0, 500, 1_250, 2_000, 2_500]
MULTIPLIERS: List[float] = [0, 0.25, 0.50, 0.75, 1.00]


def _build_grid() -> "pd.DataFrame | List[dict]":
    """Create an exhaustive grid of (position × multiplier) results."""
    rows = []
    for pos in POSITIONS:
        for m in MULTIPLIERS:
            buy, sell = allowed_trade_limits(pos, m)
            rows.append(
                dict(
                    position=pos,
                    multiplier=m,
                    max_buy=buy,
                    max_sell=sell,
                    scaled_pos_long=m * MAX_POS_LONG,
                    scaled_pos_short=m * MAX_POS_SHORT,
                )
            )
    if pd is None:
        return rows
    return pd.DataFrame(rows)


def _sanity_assertions() -> None:
    """Hard-code a few spot checks to fail fast if logic regresses."""
    # 1) Multiplier 0 must never allow trades that extend exposure.
    assert allowed_trade_limits(-1500, 0) == (1000, 0)          # buy-back only
    assert allowed_trade_limits( 1500, 0) == (0, 1000)          # sell-down only
    assert allowed_trade_limits(   0, 0) == (0, 0)              # flat → nothing

    # 2) At multiplier 1, limits revert to hard caps.
    assert allowed_trade_limits(0, 1) == (MAX_TRD_BUY, MAX_TRD_SELL)

    # 3) Scaling example: long 1 250, multiplier 0.5
    #    Position caps shrink to ±1 000 → already over cap, so buy=0
    assert allowed_trade_limits(1250, 0.5)[0] == 0              # buy blocked
    assert allowed_trade_limits(1250, 0.5)[1] == 500            # sell allowed

    print("✓ sanity assertions passed")


# ──────────────────────────────── MAIN ENTRANCE ──────────────────────────────
def run_demo() -> None:
    """Print a grid of results and run sanity checks."""
    _sanity_assertions()

    grid = _build_grid()
    if pd is None:
        # pandas unavailable → crude console print
        from pprint import pprint
        pprint(grid)
    else:
        print("\nFull (position × multiplier) grid:")
        with pd.option_context("display.max_rows", None, "display.max_columns", None):
            print(grid.to_string(index=False))


if __name__ == "__main__":
    run_demo()
