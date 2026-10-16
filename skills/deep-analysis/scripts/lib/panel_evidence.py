"""Evidence gates for panel methods that have no implemented validation pipeline.

These entries deliberately have no boolean bypass. Remove a gate only when a
method-specific model and reproducible validation artifacts are implemented.
"""
from math import isfinite
from lib.panel_status import STATUS_LABELS

MODEL_REQUIREMENTS = {
    "gann": ["预定义高低点锚点、角度尺度与周期模型", "样本外验证", "交易成本与稳定性检验"],
    "simons": ["预定义统计交易信号", "样本外回测", "交易成本与稳定性检验"],
    "thorp": ["校准收益概率与损失边界", "期望值与凯利仓位计算", "样本外验证"],
    "shaw": ["同口径横截面样本与因子标准化", "组合样本外回测", "交易成本与稳定性检验"],
    "asness": ["价值、质量、动量因子标准化", "组合样本外回测", "交易成本与稳定性检验"],
}

def model_gap(investor_id):
    required = MODEL_REQUIREMENTS.get(investor_id)
    if required is None:
        return None
    return {
        "abstention_category": "missing_model",
        "skip_reason": "对应模型与验证流程尚未实现：" + "、".join(required),
        "required_evidence": list(required),
    }


def normalize_abstention(investor):
    """A skipped vote has no directional score or stale rule evidence."""
    if investor.get("signal") != "skip":
        return
    investor.update(score=None, confidence=0, **{"pass": [], "fail": []},
                    weight_pass=0, weight_total=0)
    investor["verdict"] = STATUS_LABELS.get(investor.get("abstention_category"), "跳过（原因未分类）")


def enforce_model_gate(investor):
    gap = model_gap(investor.get("investor_id"))
    # A prior market/school exclusion remains the primary reason for abstaining.
    if gap and investor.get("abstention_category") not in ("market_not_applicable", "school_locked"):
        investor.update(gap, signal="skip", verdict="模型未验证")
        investor["headline"] = gap["skip_reason"]
        investor["reasoning"] = gap["skip_reason"]
        investor["comment"] = gap["skip_reason"]
    normalize_abstention(investor)


def merge_investor_overrides(investors, overrides):
    """Merge review metadata, validate active scores, then enforce model gates."""
    by_id = {i.get("investor_id"): i for i in investors}
    if isinstance(overrides, dict):
        for iid, override in overrides.items():
            investor = by_id.get(iid)
            if investor is None or not isinstance(override, dict):
                continue
            previous_signal = investor.get("signal")
            updated = dict(investor)
            for field in ("signal", "score", "confidence", "headline", "reasoning", "comment",
                          "verdict", "abstention_category", "skip_reason", "required_evidence"):
                if field in override:
                    updated[field] = override[field]
            if updated.get("signal") not in ("bullish", "bearish", "neutral", "skip"):
                raise ValueError(f"Invalid override signal for {iid}")
            if updated["signal"] != "skip":
                if previous_signal == "skip":
                    # A new active vote must supply its own score, not inherit a sentinel.
                    updated["score"] = override.get("score")
                score = updated.get("score")
                if iid not in MODEL_REQUIREMENTS and (isinstance(score, bool) or
                    not isinstance(score, (int, float)) or not isfinite(score) or not 0 <= score <= 100):
                    raise ValueError(f"Active override requires a finite score in [0, 100]: {iid}")
                if "verdict" not in override and iid not in MODEL_REQUIREMENTS:
                    signal = updated["signal"]
                    if updated.get("mandate") == "short":
                        updated["verdict"] = "做空候选" if signal == "bearish" else "无明确做空逻辑"
                    elif signal == "bullish":
                        updated["verdict"] = "强烈买入" if score >= 80 else "买入"
                    elif signal == "bearish":
                        updated["verdict"] = "回避" if score <= 20 else "观望"
                    else:
                        updated["verdict"] = "关注" if score >= 50 else "观望"
                for field in ("abstention_category", "skip_reason", "required_evidence"):
                    updated.pop(field, None)
            elif previous_signal != "skip":
                # Never inherit an active vote's category/reason for a new abstention.
                updated["abstention_category"] = override.get("abstention_category") or "unspecified"
                updated["skip_reason"] = override.get("skip_reason") or override.get("headline") or "原因未分类"
                updated["headline"] = override.get("headline") or updated["skip_reason"]
                updated["reasoning"] = override.get("reasoning") or updated["skip_reason"]
                updated["comment"] = override.get("comment") or updated["reasoning"]
            if not updated.get("comment") and updated.get("headline"):
                updated["comment"] = updated["headline"]
            investor.clear()
            investor.update(updated)
    # Covers old cached panels as well as newly generated/overridden panels.
    for investor in investors:
        enforce_model_gate(investor)
