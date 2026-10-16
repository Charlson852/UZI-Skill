"""Offline contracts spanning the October issue/PR integration."""
import importlib
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys

import pytest

SCRIPTS = Path(__file__).resolve().parents[1]
ROOT = SCRIPTS.parents[2]
sys.path.insert(0, str(SCRIPTS))

from lib.analysis_profile import get_profile
from lib.pipeline.score_fns import generate_panel, generate_synthesis, score_dimensions
from lib.stock_features import extract_features
from lib.valuation_fields import parse_percentile
from lib.quant_signal import _fetch_all_holding_funds as real_fetch_all_holding_funds


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setenv("UZI_NO_UPDATE_CHECK", "1")
    monkeypatch.delenv("UZI_SCHOOL", raising=False)
    monkeypatch.delenv("UZI_QUANT_MAX_FUNDS", raising=False)
    monkeypatch.setattr("lib.quant_signal._fetch_all_holding_funds", lambda *a, **k: [])


@pytest.mark.parametrize("value,expected", [
    ("5 年 95 分位", 95), ("5 年 20 分位", 20), ("历史 0 分位", 0),
    ("95.5%", 95.5), ("3年 75.5 分位", 75.5), (100, 100),
    ("—", None), (None, None), ("5 年", None), (True, None),
    ("101%", None), ("-1 分位", None), ("NaN", None),
])
def test_percentile_parser(value, expected):
    assert parse_percentile(value) == expected


@pytest.mark.parametrize("value,expected_score", [("5 年 95 分位", 2), ("5 年 20 分位", 9)])
def test_valuation_score_and_investor_features_share_percentile(value, expected_score):
    raw = {"ticker": "AAPL", "market": "U", "dimensions": {"10_valuation": {"data": {"pe_quantile": value}}}}
    assert score_dimensions(raw)["dimensions"]["10_valuation"]["score"] == expected_score
    assert extract_features(raw, raw["dimensions"])["pe_quantile_5y"] == parse_percentile(value)


def test_session_start_emits_valid_json():
    result = subprocess.run(["bash", str(ROOT / "hooks/session-start")],
                            capture_output=True, text=True, check=True,
                            env={**os.environ, "UZI_NO_UPDATE_CHECK": "1"}, timeout=10)
    context = json.loads(result.stdout)["additionalContext"]
    assert "\n" in context and '说"分析 XXX"' in context
    assert result.stderr == ""


@pytest.mark.parametrize("depth,count", [("lite", 20), ("medium", 80), ("deep", 100)])
def test_quant_scan_budget_applies_to_existing_list(monkeypatch, depth, count):
    import lib.quant_signal as quant
    monkeypatch.setenv("UZI_DEPTH", depth)
    calls = []
    monkeypatch.setattr(quant, "_fetch_top_holdings", lambda code, **k: calls.append(code) or [])
    quant.detect_quant_signal("600519.SH", [{"fund_code": str(i)} for i in range(100)])
    assert len(calls) == count


def test_quant_unlimited_reaches_fetch_and_keeps_all_rows(monkeypatch):
    import lib.quant_signal as quant
    import fetch_fund_holders
    monkeypatch.setattr(quant, "ak", object())
    monkeypatch.setattr(fetch_fund_holders, "fetch_holding_funds", lambda _: [{"fund_code": str(i)} for i in range(100)])
    # Restore the real helper hidden by the offline fixture, without making requests.
    assert len(real_fetch_all_holding_funds("600519", max_funds=0)) == 100
    captured = []
    monkeypatch.setattr(quant, "_fetch_all_holding_funds", lambda code, **kw: captured.append(kw["max_funds"]) or [])
    quant.detect_quant_signal("600519.SH", max_funds=0)
    assert captured == [0]
    monkeypatch.setenv("UZI_DEPTH", "lite")
    monkeypatch.setenv("UZI_QUANT_MAX_FUNDS", "-1")
    assert quant.quant_max_funds() == 20
    with pytest.raises(ValueError):
        quant.detect_quant_signal("600519.SH", max_funds=-1)


def test_pipeline_cache_persists_depth_and_resaving_does_not_invent_it(monkeypatch, tmp_path):
    import lib.cache as cache
    import run_real_test as rrt
    from lib.pipeline.run import _write_cache
    monkeypatch.setattr(rrt, "__file__", str(tmp_path / "run_real_test.py"))
    monkeypatch.setattr(cache, "CACHE_ROOT", tmp_path / ".cache")
    monkeypatch.setenv("UZI_DEPTH", "lite")
    _write_cache("600519.SH", {"ticker": "600519.SH", "dimensions": {}})
    assert cache.read_task_output("600519.SH", "raw_data")["_profile_depth"] == "lite"
    cache.write_task_output("AAPL", "raw_data", {"dimensions": {}})
    assert "_profile_depth" not in cache.read_task_output("AAPL", "raw_data")


def test_cached_depth_requires_collection_metadata(monkeypatch):
    monkeypatch.chdir(ROOT)
    entry = runpy.run_path(str(ROOT / "run.py"), run_name="review_fixture")
    raw = {"_profile_depth": "lite", "dimensions": {}}
    monkeypatch.setattr("lib.cache.read_task_output", lambda *a: raw)
    assert entry["_cached_profile_depth"]("600519.SH") == "lite"
    raw.pop("_profile_depth")
    raw["dimensions"] = {k: {} for k in get_profile("medium").fetchers_enabled if k != "3_macro"}
    assert entry["_cached_profile_depth"]("600519.SH") is None
    raw["dimensions"] = {k: {} for k in get_profile("lite").fetchers_enabled}
    assert entry["_cached_profile_depth"]("600519.SH") is None


@pytest.mark.parametrize("depth", ["lite", "medium", "deep"])
def test_collector_profile_scope_preserves_top_level_providers(monkeypatch, depth):
    collect = importlib.import_module("lib.pipeline.collect")
    from lib.pipeline.process_runner import ProcessOutcome
    monkeypatch.setenv("UZI_DEPTH", depth)
    seen = []

    def fake_jobs(jobs, **kwargs):
        seen.extend(job.key for job in jobs)
        return [ProcessOutcome(key=job.key, value=(job.key, {"data": {}}, {})) for job in jobs]

    monkeypatch.setattr(collect, "run_process_jobs", fake_jobs)
    collect.collect("600519.SH")
    assert set(seen) == set(get_profile(depth).fetchers_enabled) | {"6_fund_holders", "similar_stocks"}


@pytest.mark.parametrize("mode", ["error", "empty", "valid"])
def test_similar_stock_failure_and_legacy_cache_render(mode):
    from lib.pipeline.collect import _apply_outcome
    from lib.pipeline.process_runner import ProcessOutcome
    from lib.report.special_cards import render_friendly_layer
    out = {}
    rows = [{"name": "Example", "ticker": "AAPL"}]
    outcome = (ProcessOutcome(key="similar_stocks", error="TimeoutError", timed_out=True)
               if mode == "error" else ProcessOutcome(key="similar_stocks", value=(
                   "similar_stocks", {"data": {}}, {"similar_stocks": rows} if mode == "valid" else {})))
    _apply_outcome(out, outcome)
    assert out["similar_stocks"] == (rows if mode == "valid" else [])
    for value in (out["similar_stocks"], {"data": {}}, [None, *rows], "bad"):
        render_friendly_layer({"friendly": {"similar_stocks": value}}, {})


def test_quant_school_abstention_never_becomes_a_directional_consensus(monkeypatch):
    from lib.report.institutional import _render_style_chip
    from lib.report.special_cards import render_panel_insights
    from lib.self_review import check_consensus_formula_sanity, check_panel_hollow_verdicts
    monkeypatch.setenv("UZI_SCHOOL", "G")
    raw = {"ticker": "AAPL", "market": "U", "dimensions": {}}
    dims = score_dimensions(raw)
    panel = generate_panel(dims, raw)
    assert panel["long_active"] == 0
    assert panel["consensus_valid"] is False and panel["panel_consensus"] is None
    synthesis = generate_synthesis(raw, dims, panel)
    assert synthesis["panel_consensus"] is None
    assert synthesis["overall_score"] == synthesis["fundamental_score"]
    assert "证据不足" in synthesis["verdict_label"]
    assert all(zone["price"] == "—" for zone in synthesis["buy_zones"].values())
    assert "不可用" in _render_style_chip(synthesis)
    assert "不可用" in render_panel_insights(synthesis, panel)
    assert check_consensus_formula_sanity({"panel": panel}) == []
    assert all(i.severity == "warning" for i in check_panel_hollow_verdicts({"panel": panel}))


def test_generated_panel_exports_all_seats_without_mixing_short_book():
    from render_panel_web import render_page
    raw = {"ticker": "AAPL", "market": "U", "dimensions": {"0_basic": {"data": {"name": "Apple"}}}}
    panel = generate_panel(score_dimensions(raw), raw)
    page = render_page([{"ticker": "AAPL", "panel": panel, "raw": raw, "snapshot": {}}])
    assert page.count('<article class="reviewer') == len(panel["investors"])
    assert len(panel["investors"]) > sum(panel["signal_distribution"].values())
    assert "Apple" in page


def test_multi_market_unknown_issuer_and_malformed_url_fail_closed():
    from render_panel_web import render_page, safe_url
    unknown = {"ticker": "AAPL", "panel": {"investors": []}, "raw": {}, "snapshot": {}}
    with pytest.raises(ValueError, match="same issuer"):
        render_page([unknown, {**unknown, "ticker": "MSFT"}])
    assert safe_url("https://[invalid") is None
