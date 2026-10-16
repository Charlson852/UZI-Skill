"""Parse valuation percentiles without confusing the lookback with the value."""
import math
import re


def parse_percentile(value):
    """Return a percentile on a 0..100 scale, or None for missing/invalid data."""
    if isinstance(value, bool) or value is None:
        return None
    text = str(value).strip()
    number = r"([+-]?\d+(?:\.\d+)?)"
    match = re.search(number + r"\s*%?\s*分位", text)
    if match is None:
        match = re.fullmatch(number + r"\s*%?", text)
    if match is None:
        return None
    result = float(match.group(1))
    return result if math.isfinite(result) and 0 <= result <= 100 else None
