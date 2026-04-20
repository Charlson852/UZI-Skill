"""v3.0.0 · pipeline.schema 测试."""
from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(SCRIPTS))


def test_quality_enum():
    from lib.pipeline import Quality
    assert Quality.FULL.value == "full"
    assert Quality.PARTIAL.value == "partial"
    assert Quality.MISSING.value == "missing"
    assert Quality.ERROR.value == "error"


def test_dim_result_empty():
    from lib.pipeline import DimResult, Quality
    r = DimResult.empty("0_basic")
    assert r.dim_key == "0_basic"
    assert r.quality == Quality.MISSING
    assert r.data == {}
    assert r.data_gaps == []


def test_dim_result_error():
    from lib.pipeline import DimResult, Quality
    r = DimResult.error_result("6_fund_holders", "SSL failed", source="akshare")
    assert r.quality == Quality.ERROR
    assert "SSL failed" in r.error


def test_dim_result_to_from_dict_roundtrip():
    from lib.pipeline import DimResult, Quality
    r = DimResult(
        dim_key="1_financials",
        data={"roe": 15.5},
        source="akshare",
        quality=Quality.PARTIAL,
        data_gaps=["net_margin"],
    )
    d = r.to_dict()
    assert d["quality"] == "partial"
    r2 = DimResult.from_dict(d)
    assert r2.dim_key == r.dim_key
    assert r2.quality == Quality.PARTIAL
    assert r2.data["roe"] == 15.5


def test_fetcher_spec_required_dim_key():
    from lib.pipeline.schema import FetcherSpec
    import pytest
    with pytest.raises(ValueError):
        FetcherSpec(dim_key="")
