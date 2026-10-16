"""Regression tests for unimplemented model gates and review override propagation."""
import sys
from copy import deepcopy
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lib.investor_evaluator import evaluate
from lib.panel_evidence import MODEL_REQUIREMENTS, merge_investor_overrides
from lib.pipeline.score_fns import generate_panel, generate_synthesis, score_dimensions, _summarize_panel
from lib.report.panel_cards import render_chat_message


@pytest.fixture(autouse=True)
def unlocked(monkeypatch):
    monkeypatch.delenv("UZI_SCHOOL", raising=False)


@pytest.fixture
def favorable():
    return {"market": "US", "ticker": "TEST", "name": "Test", "industry": "科技",
            "stage_num": 2, "ma_bull_aligned": True, "volatility_1y": 30, "ytd_return": 10,
            "upside_to_target": 20, "roe_latest": 15, "roe": 15,
            "pe_quantile_5y": 30, "revenue_growth_3y_cagr": 20,
            "pe_ttm": 15, "pb": 2, "debt_ratio": 40,
            "price_above_ma200": True, "roe_5y_min": 10}


@pytest.mark.parametrize("iid", MODEL_REQUIREMENTS)
def test_favorable_thresholds_cannot_substitute_for_model_validation(iid, favorable):
    favorable["model_verified"] = True  # No trusted validation producer exists.
    result = evaluate(iid, favorable)
    assert result["signal"] == "skip"
    assert result["abstention_category"] == "missing_model"
    assert result["required_evidence"] == MODEL_REQUIREMENTS[iid]
    assert result["confidence"] == 0 and not result["pass_rules"]


def test_market_scope_precedes_model_gate(favorable):
    favorable["market"] = "A"
    assert evaluate("thorp", favorable)["abstention_category"] == "market_not_applicable"


def test_school_lock_precedes_model_gate(monkeypatch, favorable):
    monkeypatch.setenv("UZI_SCHOOL", "A")
    assert evaluate("simons", favorable)["abstention_category"] == "school_locked"


def test_unrelated_judge_still_uses_rules(favorable):
    result = evaluate("darvas", favorable)
    assert result["signal"] == "bullish" and result["score"] == 100


def active(iid="buffett", group="A"):
    return {"investor_id": iid, "name": iid, "group": group, "signal": "bullish",
            "score": 100, "confidence": 90, "verdict": "强烈买入", "headline": "old",
            "comment": "old", "pass": [{"name": "old", "msg": "old", "weight": 1}],
            "fail": [], "weight_pass": 1, "weight_total": 1}


@pytest.mark.parametrize("score_field", [{}, {"score": None}, {"score": 100}])
def test_skip_override_clears_old_score_and_keeps_evidence(score_field):
    investor = active()
    override = {"signal": "skip", "abstention_category": "missing_data",
                "skip_reason": "缺少可核验现金流", "required_evidence": ["现金流原表"],
                "headline": "缺现金流", **score_field}
    merge_investor_overrides([investor], {"buffett": override})
    assert investor["score"] is None and investor["confidence"] == 0
    assert investor["abstention_category"] == "missing_data"
    assert investor["skip_reason"] == override["skip_reason"]
    assert investor["required_evidence"] == ["现金流原表"]
    assert investor["pass"] == [] and investor["weight_total"] == 0
    html = render_chat_message(investor)
    assert "数据缺失" in html and "缺少可核验现金流" in html
    assert "None分" not in html and "100分" not in html


def test_active_override_clears_old_abstention_metadata():
    investor = active()
    merge_investor_overrides([investor], {"buffett": {"signal": "skip", "abstention_category": "missing_data"}})
    merge_investor_overrides([investor], {"buffett": {"signal": "neutral", "score": 55, "confidence": 70}})
    assert investor["score"] == 55 and investor["confidence"] == 70
    assert investor["verdict"] == "关注"
    assert all(key not in investor for key in ("skip_reason", "abstention_category", "required_evidence"))


@pytest.mark.parametrize("score", [None, True, float("nan"), float("inf"), -1, 101])
def test_invalid_active_score_rejected_atomically(score):
    investor = active()
    before = deepcopy(investor)
    with pytest.raises(ValueError):
        merge_investor_overrides([investor], {"buffett": {"signal": "neutral", "score": score}})
    assert investor == before


def test_model_gate_applies_to_old_cache_and_cannot_be_overridden():
    investor = active("simons", "G")
    merge_investor_overrides([investor], None)
    assert investor["signal"] == "skip" and investor["score"] is None
    merge_investor_overrides([investor], {"simons": {"signal": "bullish", "score": 100}})
    assert investor["signal"] == "skip" and investor["abstention_category"] == "missing_model"


def test_summary_recomputed_for_skip_instead_of_using_old_vote():
    investors = [active(), active("darvas", "D")]
    merge_investor_overrides(investors, {"buffett": {"signal": "skip", "abstention_category": "missing_data"}})
    panel = _summarize_panel(investors, "TEST")
    assert panel["long_active"] == 1
    assert panel["signal_distribution"] == {"bullish": 1, "neutral": 0, "bearish": 0, "skip": 1}
    assert panel["abstention_distribution"] == {"missing_data": 1}
    assert panel["school_scores"]["A"]["n_active"] == 0
    assert panel["school_scores"]["D"]["score_mean"] == 100
    assert panel["consensus_formula"]["active"] == 1


def test_full_scoring_and_synthesis_propagate_skip_override():
    raw = {"ticker": "AAPL", "market": "US", "dimensions": {}}
    dims = score_dimensions(raw)
    panel = generate_panel(dims, raw)
    for investor in panel["investors"]:
        if investor["investor_id"] in MODEL_REQUIREMENTS:
            assert investor["signal"] == "skip" and investor["score"] is None
    # Turn a known active vote into an evidence-based abstention.
    target = next(i for i in panel["investors"] if i["signal"] != "skip")
    old_active = panel["long_active"]
    generate_synthesis(raw, dims, panel, {"per_investor_override": {target["investor_id"]: {
        "signal": "skip", "score": None, "abstention_category": "missing_data",
        "skip_reason": "缺证据", "required_evidence": ["原始报告"]}}})
    assert target["score"] is None and target["skip_reason"] == "缺证据"
    assert panel["long_active"] == old_active - 1
    assert panel["abstention_distribution"]["missing_data"] >= 1


def test_short_book_and_vote_totals_recomputed_separately():
    short = active("burry", "C")
    short.update(mandate="short", signal="bearish", score=10)
    investors = [active(), short]
    merge_investor_overrides(investors, {"burry": {"signal": "skip", "abstention_category": "missing_data"}})
    panel = _summarize_panel(investors, "TEST")
    assert panel["long_active"] == 1 and panel["signal_distribution"]["skip"] == 0
    assert panel["short_consensus"]["active"] == 0
    assert panel["short_consensus"]["skip"] == 1
    assert panel["short_consensus"]["top_short_candidates"] == []


def test_skip_override_removes_old_directional_comment():
    investor = active()
    investor.update(comment="看多，强烈买入", reasoning="旧的看多理由")
    merge_investor_overrides([investor], {"buffett": {"signal": "skip", "skip_reason": "尚缺原表"}})
    assert "强烈买入" not in render_chat_message(investor)
    assert investor["reasoning"] == investor["comment"] == "尚缺原表"


def test_stage2_renderer_reads_persisted_reviewed_panel(monkeypatch, tmp_path):
    from types import SimpleNamespace
    import run_real_test as rrt
    import lib.cache as cache
    import lib.agent_review as review
    import assemble_report

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("UZI_NO_AUTO_OPEN", "1")
    monkeypatch.setenv("UZI_DEPTH", "medium")
    monkeypatch.setattr(cache, "CACHE_ROOT", tmp_path / ".cache")
    monkeypatch.setattr(rrt, "_resolve_cached_target", lambda *a, **kw: SimpleNamespace(full="AAPL"))
    raw = {"ticker": "AAPL", "market": "US", "dimensions": {}}
    dims = score_dimensions(raw)
    panel = _summarize_panel([active(), active("simons", "G")], "AAPL")
    for key, value in [("raw_data", raw), ("dimensions", dims), ("panel", panel)]:
        cache.write_task_output("AAPL", key, value)
    analysis = {"agent_reviewed": True, "per_investor_override": {"buffett": {
        "signal": "skip", "score": None, "abstention_category": "missing_data",
        "skip_reason": "缺少现金流原表", "required_evidence": ["现金流原表"]}}}
    monkeypatch.setattr(review, "load_fresh_agent_analysis", lambda *a: (analysis, "fixture"))
    report = tmp_path / "report.html"
    report.write_text("<!doctype html><p>" + "fixture " * 2000 + "</p>")
    rendered = []
    def assemble(_):
        persisted = cache.read_task_output("AAPL", "panel")
        assert persisted["agent_reviewed"] is True
        assert persisted["signal_distribution"]["bullish"] == 0
        assert persisted["signal_distribution"]["skip"] == 2
        assert persisted["abstention_distribution"] == {"missing_data": 1, "missing_model": 1}
        for investor in persisted["investors"]:
            assert investor["score"] is None
            rendered.append(render_chat_message(investor))
        return report
    monkeypatch.setattr(assemble_report, "assemble", assemble)
    monkeypatch.setitem(sys.modules, "inline_assets", SimpleNamespace(main=lambda _: report))
    for name in ("render_share_card", "render_war_report"):
        monkeypatch.setitem(sys.modules, name, SimpleNamespace(main=lambda _: None))
    assert rrt.stage2("AAPL") == str(report)
    assert any("缺少现金流原表" in html for html in rendered)
    assert any("模型未验证" in html for html in rendered)
