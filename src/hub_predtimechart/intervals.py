"""
The prediction intervals predtimechart can plot, and the quantile levels that back them. Shared by
`generate_options.py` (which offers the intervals), `generate_data.py` (which extracts the quantile values), and
`hub_config_ptc.py` (which validates that a hub has the required ones).
"""

from collections.abc import Iterable


# quantile levels backing each interval, in the order the intervals are presented to the user. '0%' plots the median
# alone, so it has no bounding quantiles
INTERVAL_TO_QUANTILE_LEVELS: dict[str, tuple[float, ...]] = {
    '0%': (),
    '50%': (0.25, 0.75),
    '80%': (0.1, 0.9),
    '95%': (0.025, 0.975),
}

# the median, needed by every interval (including '0%')
MEDIAN_QUANTILE_LEVEL = 0.5

# intervals every predtimechart-compatible hub must support: `_validate_hub_ptc_compatibility()` requires their
# quantile levels of all hubs. the remaining intervals are optional - `ptc_options_for_hub()` offers them only when the
# hub's `tasks.json` requires the backing quantile levels
REQUIRED_INTERVALS: frozenset[str] = frozenset({'0%', '50%', '95%'})


def quantile_levels_for_intervals(intervals: Iterable[str]) -> set[float]:
    """
    Returns the set of quantile levels needed to plot `intervals` (`INTERVAL_TO_QUANTILE_LEVELS` keys), including the
    median. Raises KeyError for an unrecognized interval.
    """
    return {MEDIAN_QUANTILE_LEVEL} | {level for interval in intervals
                                      for level in INTERVAL_TO_QUANTILE_LEVELS[interval]}
