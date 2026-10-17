"""Offline evidence contracts: unknown != zero, and report period != fetch year."""
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from lib.stock_features import extract_features, _avg, _min, _f, summary
from lib.investor_criteria import Rule
from lib.investor_evaluator import _safe_check, evaluate_all
from lib import quant_signal as quant


def features(**dimensions):
    return extract_features({"ticker": "UNKNOWN", "dimensions": {
        key: {"data": value} for key, value in dimensions.items()
    }}, {})


@pytest.mark.parametrize("value", [None, "—", "N/A", "NaN", "inf", float("nan"), float("inf")])
def test_unavailable_numbers_never_become_measurements(value):
    assert _f(value, None) is None
    f = features(**{"1_financials": {"gross_margin": value}})
    assert f["gross_margin"] is None


def test_empty_data_has_no_directional_votes():
    f = features()
    assert all(row["signal"] == "skip" for row in evaluate_all(f).values())
    for key in ("price", "roe_latest", "gross_margin", "is_safe", "no_violations",
                "rsi_oversold", "peer_avg_pe", "has_top_fund_holder", "industry_growth",
                "ai_chain_hit", "ai_chokepoint_score", "pe_x_pb", "fcf_latest_yi"):
        assert f[key] is None, key
    assert "gross_margin" in f["missing_features"]
    assert isinstance(summary(f), str)


@pytest.mark.parametrize("check", [lambda f: not f.get("x", False),
                                  lambda f: f.get("x", 0) < 10,
                                  lambda f: f.get("x", False) is False,
                                  lambda f: True])
@pytest.mark.parametrize("input_features", [{}, {"x": None}])
def test_rule_defaults_and_constants_cannot_invent_evidence(check, input_features):
    assert _safe_check(Rule("test", "test", 1, check), input_features) is None


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_direct_nonfinite_rule_inputs_abstain(value):
    assert _safe_check(Rule("test", "test", 1, lambda f: f["x"] > 0), {"x": value}) is None


def test_real_zero_false_and_losses_are_preserved():
    assert _avg([0, -5, 20, None]) == 5
    assert _min([0, 10, None]) == 0
    f = features(**{
        "0_basic": {"pe_ttm": 0, "pb": 2, "dividend_yield_ttm": 0},
        "10_valuation": {"pe": 25, "dividend_yield": 5},
        "1_financials": {"roe_history": [0, -5, 20], "gross_margin": 0,
                         "revenue_history": [100, 100], "net_profit_history": [10, -10, 0, 5]},
        "2_kline": {"indicators": {"kdj_k": 1, "kdj_d": 0, "obv_trend_up": False}},
        "15_events": {"event_timeline": []},
    })
    assert f["roe_5y_avg"] == 5
    assert f["roe_5y_min"] == -5
    assert f["gross_margin"] == f["pe"] == f["pe_x_pb"] == f["dividend_yield"] == 0
    assert f["revenue_growth_latest"] == 0
    assert f["consecutive_profit_years"] == 1
    assert f["kdj_golden_cross"] is True
    assert f["obv_trend_up"] is False
    assert f["has_positive_catalyst"] is False
    assert _safe_check(Rule("zero", "zero", 1, lambda f: f.get("gross_margin") == 0), f) is True
    assert _safe_check(Rule("false", "false", 1, lambda f: f.get("obv_trend_up")), f) is False


def test_partial_series_does_not_fill_missing_endpoint_with_zero():
    f = features(**{"1_financials": {"revenue_history": [100, None],
                                    "roe_history": [20, None], "operating_cash_flow_yi": "—"}})
    assert f["revenue_growth_latest"] is None
    assert f["revenue_latest_yi"] is None
    assert f["roe_latest"] is None
    assert f["roe_5y_above_15"] is None
    assert f["fcf_positive"] is None


def test_failed_dimension_cannot_reuse_stale_payload():
    f = extract_features({"ticker": "UNKNOWN", "dimensions": {
        "1_financials": {"status": "failed", "data": {"revenue_ttm": 100, "net_profit_ttm": 20}},
        "18_trap": {"status": "failed", "data": {"trap_level": "安全"}},
    }}, {})
    assert f["net_margin"] is None and f["net_profit_latest_yi"] is None
    assert f["is_safe"] is None


def test_missing_candle_and_partial_dupont_are_unknown():
    f = features(**{"2_kline": {"candles_60d": [{"close": 5, "high": None, "low": 0}]},
                    "1_financials": {"dupont": {"net_margin_pct": 0}}})
    assert f["pct_from_60d_high"] is None and f["pct_from_60d_low"] is None
    assert f["dupont_net_margin"] == 0 and f["dupont_asset_turnover"] is None


@pytest.mark.parametrize("dcf,expected", [("—", None), ("N/A", None), ("¥100.0亿", 100)])
def test_dcf_placeholder_is_not_zero_intrinsic_value(dcf, expected):
    f = features(**{"10_valuation": {"dcf": dcf}, "0_basic": {"market_cap_yi": 100}})
    assert f["dcf_intrinsic_yi"] == expected
    assert f["safety_margin"] == (None if expected is None else 0)


def test_missing_roe_does_not_imply_distress(monkeypatch):
    from lib.stock_style import detect_style, DISTRESSED
    monkeypatch.setattr(quant, "detect_quant_signal", lambda *a, **k: {"is_quant_factor_style": None})
    assert detect_style({"pb": 0.8, "market": "US"}) != DISTRESSED
    assert detect_style({"pb": 0.8, "roe_5y_min": 0, "market": "US"}) == DISTRESSED


def holdings(period="2026年2季度股票投资明细", weight=1.5):
    return [{"股票代码": f"{600000 + i:06d}", "占净值比例": weight - i / 100,
             "季度": period} for i in range(10)]


@pytest.fixture
def provider(monkeypatch, tmp_path):
    from lib import cache
    monkeypatch.setattr(cache, "CACHE_ROOT", tmp_path)
    monkeypatch.setattr(cache, "NO_CACHE", False)
    monkeypatch.setattr(quant, "_today", lambda: date(2026, 10, 17))
    monkeypatch.setattr(quant, "_fetch_all_holding_funds", lambda *a, **k: [])
    calls, payloads = [], {}

    def fetch(symbol, date):
        calls.append((symbol, date))
        result = payloads.get(date, [])
        if isinstance(result, Exception):
            raise result
        return pd.DataFrame(result)

    monkeypatch.setattr(quant, "ak", SimpleNamespace(fund_portfolio_hold_em=fetch))
    return payloads, calls, tmp_path


def test_latest_disclosed_quarter_sorted_not_head_or_current_quarter(provider):
    payloads, calls, root = provider
    payloads["2026"] = holdings("2026年1季度股票投资明细", 8) + list(reversed(holdings()))
    rows = quant._fetch_top_holdings("000001")
    assert len(rows) == 10
    assert rows[0]["占净值比例"] == 1.5
    assert {row["report_period"] for row in rows} == {"2026-06-30"}
    assert rows[0]["holdings_status"] == "available"
    assert rows[0]["publication_date_verified"] is False
    assert any("period_2026-06-30" in path.name for path in root.rglob("*.json"))
    assert quant._fetch_top_holdings("000001", 3) == rows[:3]
    assert calls == [("000001", "2026")]


def test_cross_year_fallback_uses_returned_period(provider, monkeypatch):
    payloads, calls, _ = provider
    monkeypatch.setattr(quant, "_today", lambda: date(2027, 1, 5))
    payloads["2026"] = holdings("2026年3季度股票投资明细")
    rows = quant._fetch_top_holdings("000001")
    assert rows[0]["report_period"] == "2026-09-30"
    assert calls == [("000001", "2027"), ("000001", "2026")]


@pytest.mark.parametrize("period,status", [("2026年1季度股票投资明细", "stale"),
                                          ("2026年4季度股票投资明细", None),
                                          ("bad period", None)])
def test_stale_future_and_unknown_periods(provider, period, status):
    payloads, _, _ = provider
    payloads["2026"] = holdings(period)
    rows = quant._fetch_top_holdings("000001")
    assert (rows[0]["holdings_status"] if rows else None) == status
    result = quant.detect_quant_signal("600000.SH", [{"fund_code": "000001"}])
    assert result["is_quant_factor_style"] is None
    assert result["verified_funds_total"] == 0


@pytest.mark.parametrize("weight", [None, 0, -1, float("nan"), float("inf"), "—"])
def test_invalid_weight_never_classifies_as_quant(provider, weight):
    payloads, _, _ = provider
    rows = holdings()
    rows[0]["占净值比例"] = weight
    payloads["2026"] = rows
    fetched = quant._fetch_top_holdings("000001")
    assert fetched[0]["holdings_status"] == "invalid_weights"
    assert quant._is_quant_like(rows)[0] is False


def test_errors_are_not_cached_for_a_day(provider):
    payloads, _, root = provider
    payloads["2026"] = RuntimeError("offline")
    assert quant._fetch_top_holdings("000001") == []
    assert list(root.rglob("*.json")) == []
    payloads["2026"] = holdings()
    assert quant._fetch_top_holdings("000001")[0]["holdings_status"] == "available"


def test_fund_codes_cannot_escape_cache_directory(provider):
    _, calls, root = provider
    assert quant._fetch_top_holdings("../../outside") == []
    assert not calls and not list(root.rglob("*.json"))


def test_duplicate_funds_do_not_supply_three_votes(provider):
    payloads, _, _ = provider
    payloads["2026"] = holdings()
    result = quant.detect_quant_signal("600000.SH", [{"fund_code": "000001"}] * 3)
    assert result["count"] == 1
    assert result["active_funds_total"] == 1
    assert result["is_quant_factor_style"] is False


def test_three_same_period_samples_and_partial_failure(provider, monkeypatch):
    payloads, _, _ = provider
    payloads["2026"] = holdings()
    result = quant.detect_quant_signal("600000.SH", [{"fund_code": f"{i:06d}"} for i in range(1, 4)])
    assert result["is_quant_factor_style"] is True
    assert result["counts_by_period"] == {"2026-06-30": 3}
    assert result["evidence_status"] == "available"
    original = quant._fetch_top_holdings
    monkeypatch.setattr(quant, "_fetch_top_holdings", lambda code, **kw: [] if code == "000003" else original(code, **kw))
    result = quant.detect_quant_signal("600000.SH", [{"fund_code": f"{i:06d}"} for i in range(1, 4)])
    assert result["is_quant_factor_style"] is None
    assert result["evidence_status"] == "partial"
    assert result["verified_funds_total"] == 2


def test_mixed_periods_do_not_combine_three_votes(provider, monkeypatch):
    def fetch(code, **kwargs):
        rows = holdings()
        for row in rows:
            row.update(holdings_status="available", report_period="2026-06-30" if code != "000003" else "2026-09-30")
        return rows
    monkeypatch.setattr(quant, "_fetch_top_holdings", fetch)
    result = quant.detect_quant_signal("600000.SH", [{"fund_code": f"{i:06d}"} for i in range(1, 4)])
    assert result["is_quant_factor_style"] is False
    assert max(result["counts_by_period"].values()) == 2


def test_report_displays_coverage_and_escapes_period():
    from lib.report.special_cards import render_fund_managers
    html = render_fund_managers([], {"evidence_status": "partial", "report_periods": ["<script>"],
                                    "verified_funds_total": 2, "active_funds_total": 5})
    assert "部分缺失" in html and "2/5" in html
    assert "<script>" not in html and "&lt;script&gt;" in html
    assert "未核验公告时间" in html


def test_synthesis_preserves_scan_evidence_and_missing_roe(monkeypatch):
    from lib.pipeline.score_fns import score_dimensions, generate_panel, generate_synthesis
    monkeypatch.delenv("UZI_SCHOOL", raising=False)
    sig = {"is_quant_factor_style": None, "evidence_status": "partial", "report_periods": ["2026-06-30"],
           "verified_funds_total": 1, "active_funds_total": 2}
    monkeypatch.setattr(quant, "detect_quant_signal", lambda *args, **kwargs: sig)
    raw = {"ticker": "UNKNOWN", "market": "U", "dimensions": {"0_basic": {"data": {"pb": 0.8}}}}
    dims = score_dimensions(raw)
    result = generate_synthesis(raw, dims, generate_panel(dims, raw))
    assert result["quant_signal"] == sig
    assert result["detected_style"] != "distressed"


def test_fund_lookup_fallback_probes_real_periods(monkeypatch):
    import fetch_fund_holders as holders
    monkeypatch.setattr(holders, "_recent_quarter_date", lambda: "20260331")
    calls = []

    def fail(**kwargs):
        raise RuntimeError("offline")

    def fallback(symbol, date):
        calls.append(date)
        return pd.DataFrame([] if date == "20260331" else [{"股票代码": "600000", "基金代码": "000001"}])

    monkeypatch.setattr(holders, "ak", SimpleNamespace(stock_fund_stock_holder=fail,
                                                     stock_report_fund_hold_detail=fallback))
    rows = holders.fetch_holding_funds("600000")
    assert calls == ["20260331", "20251231"]
    assert rows[0]["截止日期"] == "20251231"


def test_non_a_share_scan_does_not_query_domestic_funds(provider):
    _, calls, _ = provider
    assert quant.detect_quant_signal("AAPL")["evidence_status"] == "not_applicable"
    assert quant.detect_quant_signal("00700.HK")["evidence_status"] == "not_applicable"
    assert not calls


def test_new_disclosure_is_discovered_after_short_cache_expiry(provider, monkeypatch):
    from lib import cache
    payloads, calls, _ = provider
    clock = [100000.0]
    monkeypatch.setattr(cache.time, "time", lambda: clock[0])
    payloads["2026"] = holdings()
    assert quant._fetch_top_holdings("000001")[0]["report_period"] == "2026-06-30"
    payloads["2026"] += holdings("2026年3季度股票投资明细", 8)
    clock[0] += 3601
    assert quant._fetch_top_holdings("000001")[0]["report_period"] == "2026-09-30"
    assert len(calls) == 2


@pytest.mark.parametrize("placeholder", ["—", "-", "N/A", " "])
def test_unknown_industry_cannot_mean_not_in_ai_chain(placeholder):
    f = features(**{"0_basic": {"industry": placeholder, "market_cap_yi": 100}})
    assert f["ai_chain_hit"] is None
    assert f["ai_chokepoint_score"] is None


def test_negative_revenue_does_not_create_complex_cagr():
    f = features(**{"1_financials": {"revenue_history": [100, 80, 20, -10]}})
    assert f["revenue_growth_3y_cagr"] is None
    assert f["revenue_growth_latest"] == -150


def test_normalized_fund_codes_do_not_count_twice(provider):
    payloads, _, _ = provider
    payloads["2026"] = holdings()
    result = quant.detect_quant_signal("600000.SH", [
        {"fund_code": "000001"}, {"fund_code": " 000001"}, {"fund_code": "000001 "},
    ])
    assert result["active_funds_total"] == result["count"] == 1
    assert result["is_quant_factor_style"] is False


def test_percent_weight_keeps_its_value(provider):
    payloads, _, _ = provider
    payloads["2026"] = holdings()
    payloads["2026"][0]["占净值比例"] = "1.5%"
    result = quant.detect_quant_signal("600000.SH", [{"fund_code": "000001"}])
    assert result["quant_funds"][0]["weight_pct"] == 1.5


def test_six_digit_foreign_ticker_cannot_use_a_share_scan(provider):
    _, calls, _ = provider
    assert quant.detect_quant_signal("000700.HK")["evidence_status"] == "not_applicable"
    assert not calls
