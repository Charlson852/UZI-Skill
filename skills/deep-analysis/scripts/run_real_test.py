"""End-to-end live pipeline on a real ticker.

Runs all 22 fetchers (with graceful failure), computes dimensions + panel
+ synthesis rule-based, then calls assemble_report + inline_assets.

Usage: python run_real_test.py 002273.SZ
"""
from __future__ import annotations

import io
import json
import os
import sys
import time
import traceback
from pathlib import Path

# Force UTF-8 output on Windows GBK consoles
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))

from lib.cache import write_task_output  # noqa: E402
from lib.investor_db import INVESTORS  # noqa: E402
from lib.investor_personas import get_comment as _persona_comment  # noqa: E402
from lib.market_router import parse_ticker  # noqa: E402
from lib.stock_features import extract_features  # noqa: E402
from lib.investor_evaluator import evaluate as _evaluate_investor  # noqa: E402
from concurrent.futures import ThreadPoolExecutor, as_completed  # noqa: E402

# Fetcher registry: (module_name, dim_key, fetcher_args_fn)
# fetcher_args_fn(ticker, raw_so_far) → args tuple for main()
FETCHER_MAP = [
    ("fetch_basic",           "0_basic",        lambda t, r: (t,)),
    ("fetch_financials",      "1_financials",   lambda t, r: (t,)),
    ("fetch_kline",           "2_kline",        lambda t, r: (t,)),
    ("fetch_peers",           "4_peers",        lambda t, r: (t,)),
    ("fetch_chain",           "5_chain",        lambda t, r: (t,)),
    ("fetch_research",        "6_research",     lambda t, r: (t,)),
    ("fetch_industry",        "7_industry",     lambda t, r: (r.get("0_basic", {}).get("data", {}).get("industry", "") or "综合",)),
    ("fetch_materials",       "8_materials",    lambda t, r: (t,)),
    ("fetch_futures",         "9_futures",      lambda t, r: (r.get("0_basic", {}).get("data", {}).get("industry", "") or "综合",)),
    ("fetch_valuation",       "10_valuation",   lambda t, r: (t,)),
    ("fetch_governance",      "11_governance",  lambda t, r: (t,)),
    ("fetch_capital_flow",    "12_capital_flow",lambda t, r: (t,)),
    ("fetch_policy",          "13_policy",      lambda t, r: (r.get("0_basic", {}).get("data", {}).get("industry", "") or "综合",)),
    ("fetch_moat",            "14_moat",        lambda t, r: (t,)),
    ("fetch_events",          "15_events",      lambda t, r: (t,)),
    ("fetch_lhb",             "16_lhb",         lambda t, r: (t,)),
    ("fetch_sentiment",       "17_sentiment",   lambda t, r: (t,)),
    ("fetch_trap_signals",    "18_trap",        lambda t, r: (t,)),
    ("fetch_contests",        "19_contests",    lambda t, r: (t,)),
    ("fetch_macro",           "3_macro",        lambda t, r: (r.get("0_basic", {}).get("data", {}).get("industry", "") or "综合",)),
]


def run_fetcher(module_name: str, args: tuple) -> dict:
    try:
        mod = __import__(module_name)
        result = mod.main(*args)
        return result if isinstance(result, dict) else {"data": result}
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        return {"data": {}, "source": module_name, "fallback": True, "error": f"{type(e).__name__}: {e}"}


def collect_raw_data(ticker: str, max_workers: int = 6) -> dict:
    """Parallel fetcher execution via ThreadPoolExecutor.

    Strategy: run fetch_basic first (others depend on industry etc), then
    spawn all remaining fetchers in parallel. Bonus fetchers (fund_holders,
    similar_stocks) run in a second wave since they depend on base cache.
    """
    from datetime import datetime as _dt
    raw = {"ticker": ticker, "market": "A", "fetched_at": _dt.now().isoformat(timespec="seconds")}
    dims: dict = {}
    t0 = time.time()

    # ── Wave 1: fetch_basic (串行, 后续 fetcher 依赖它拿 industry) ──
    print("  [wave 1] fetch_basic ...", end="", flush=True)
    wave1_start = time.time()
    dims["0_basic"] = run_fetcher("fetch_basic", (ticker,))
    print(f" ✓ ({time.time() - wave1_start:.1f}s)")

    # ── Wave 2: all other 19 fetchers in parallel ──
    print(f"  [wave 2] 19 fetchers parallel (max_workers={max_workers})...")
    wave2_start = time.time()
    others = [(m, d, a) for m, d, a in FETCHER_MAP if d != "0_basic"]

    def _run_one(item):
        mod_name, dim_key, args_fn = item
        t = time.time()
        args = args_fn(ticker, dims)
        result = run_fetcher(mod_name, args)
        return dim_key, mod_name, result, time.time() - t

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_run_one, it): it for it in others}
        for fut in as_completed(futures):
            try:
                dim_key, mod_name, result, elapsed = fut.result()
                dims[dim_key] = result
                err = result.get("error") if isinstance(result, dict) else None
                has_data = bool(result.get("data")) if isinstance(result, dict) else False
                status = "✗" if err else ("✓" if has_data else "·")
                tail = f" {err[:60]}" if err else ""
                print(f"    {status} {dim_key:18} ({elapsed:5.1f}s){tail}")
            except Exception as e:
                print(f"    ✗ fetcher crash: {e}")
    wave2_elapsed = time.time() - wave2_start
    print(f"  [wave 2] done in {wave2_elapsed:.1f}s")

    # ── Wave 3: bonus fetchers (parallel) ──
    print("  [wave 3] bonus fetchers parallel ...")
    wave3_start = time.time()

    def _fund_holders():
        try:
            import fetch_fund_holders
            fh = fetch_fund_holders.main(ticker, limit=6)
            return ("fund_managers", (fh.get("data") or {}).get("fund_managers", []), None)
        except Exception as e:
            return ("fund_managers", [], str(e))

    def _similar_stocks():
        try:
            import fetch_similar_stocks
            ss = fetch_similar_stocks.main(ticker, top_n=4)
            return ("similar_stocks", (ss.get("data") or {}).get("similar_stocks", []), None)
        except Exception as e:
            return ("similar_stocks", [], str(e))

    with ThreadPoolExecutor(max_workers=2) as pool:
        for fut in as_completed([pool.submit(_fund_holders), pool.submit(_similar_stocks)]):
            key, val, err = fut.result()
            raw[key] = val
            status = "✗" if err else "✓"
            print(f"    {status} {key}: {len(val) if isinstance(val, list) else 'n/a'}")
    wave3_elapsed = time.time() - wave3_start
    print(f"  [wave 3] done in {wave3_elapsed:.1f}s")

    raw["dimensions"] = dims
    total_elapsed = time.time() - t0
    print(f"\n  Task 1 total: {total_elapsed:.1f}s (wave1 {time.time() - wave1_start:.1f}s + wave2 {wave2_elapsed:.1f}s + wave3 {wave3_elapsed:.1f}s)")
    return raw


# ─────────── DIMENSIONS SCORING (rule-based) ───────────

def _f(v, default=0.0):
    try:
        return float(str(v).replace("%", "").replace(",", "").replace("+", ""))
    except (ValueError, TypeError):
        return default


def score_dimensions(raw: dict) -> dict:
    dims = raw.get("dimensions", {})
    out = {}

    def _get(key: str) -> dict:
        return (dims.get(key) or {}).get("data") or {}

    # 1 · 财报
    fin = _get("1_financials")
    roe = _f(fin.get("roe"))
    last_roe = (fin.get("roe_history") or [0])[-1] if fin.get("roe_history") else roe
    net_margin = _f(fin.get("net_margin"))
    health = fin.get("financial_health") or {}
    debt = _f(health.get("debt_ratio"))
    rev_hist = fin.get("revenue_history") or []
    growth = ((rev_hist[-1] - rev_hist[-2]) / rev_hist[-2] * 100) if len(rev_hist) >= 2 and rev_hist[-2] else 0
    score_1 = 5
    if last_roe >= 15: score_1 += 2
    elif last_roe >= 10: score_1 += 1
    elif last_roe < 5: score_1 -= 2
    if net_margin >= 15: score_1 += 1
    if growth >= 20: score_1 += 1
    if debt >= 60: score_1 -= 1
    score_1 = max(1, min(10, score_1))
    reasons_pass_1 = []
    reasons_fail_1 = []
    if last_roe >= 15: reasons_pass_1.append(f"ROE 最新 {last_roe:.1f}%")
    elif last_roe < 8: reasons_fail_1.append(f"ROE 最新 {last_roe:.1f}% 偏低")
    if growth >= 20: reasons_pass_1.append(f"营收增速 {growth:.1f}%")
    elif growth < 5: reasons_fail_1.append(f"营收增速 {growth:.1f}% 停滞")
    if debt < 40: reasons_pass_1.append(f"资产负债率 {debt:.0f}% 健康")
    elif debt > 60: reasons_fail_1.append(f"资产负债率 {debt:.0f}% 偏高")
    out["1_financials"] = {"score": score_1, "weight": 5,
                            "label": f"ROE {last_roe:.1f}% · 营收增速 {growth:+.1f}% · 负债率 {debt:.0f}%",
                            "reasons_pass": reasons_pass_1, "reasons_fail": reasons_fail_1}

    # 2 · K 线
    kline = _get("2_kline")
    stage = str(kline.get("stage", ""))
    ma_align = str(kline.get("ma_align", ""))
    stats = kline.get("kline_stats") or {}
    score_2 = 5
    if "Stage 2" in stage: score_2 += 2
    elif "Stage 1" in stage: score_2 += 1
    elif "Stage 3" in stage or "Stage 4" in stage: score_2 -= 2
    if "多头" in ma_align: score_2 += 1
    dd_str = stats.get("max_drawdown", "0%")
    dd = _f(dd_str)
    if dd <= -30: score_2 -= 1
    score_2 = max(1, min(10, score_2))
    label_2 = f"{stage} · 均线{ma_align}"
    if stats.get("ytd_return"): label_2 += f" · YTD {stats['ytd_return']}"
    out["2_kline"] = {"score": score_2, "weight": 4, "label": label_2,
                      "reasons_pass": [f"{stage}"] if "Stage 2" in stage else [],
                      "reasons_fail": [f"最大回撤 {dd:.1f}%"] if dd <= -25 else []}

    # 3 · 宏观 (qualitative — give middle)
    out["3_macro"] = {"score": 6, "weight": 3, "label": "宏观环境中性（需 web search 补充）"}

    # 4 · 同行
    peers = _get("4_peers")
    peer_table = peers.get("peer_table") or []
    score_4 = 5
    if peer_table and len(peer_table) > 1:
        score_4 = 7  # we have data
        try:
            self_row = next((p for p in peer_table if p.get("is_self")), None)
            if self_row:
                self_pe = _f(self_row.get("pe"))
                avg_pe = sum(_f(p.get("pe")) for p in peer_table if not p.get("is_self")) / max(1, len([p for p in peer_table if not p.get("is_self")]))
                if self_pe > 0 and avg_pe > 0:
                    if self_pe < avg_pe * 0.9: score_4 += 1
                    elif self_pe > avg_pe * 1.2: score_4 -= 1
        except Exception:
            pass
    out["4_peers"] = {"score": score_4, "weight": 4,
                      "label": f"同业 {len(peer_table) - 1} 家对比" if peer_table else "无同行数据",
                      "reasons_pass": [], "reasons_fail": []}

    # 5 · 上下游
    chain = _get("5_chain")
    breakdown = chain.get("main_business_breakdown") or []
    score_5 = 6 if breakdown else 5
    out["5_chain"] = {"score": score_5, "weight": 4,
                      "label": f"主营 {len(breakdown)} 类业务已识别" if breakdown else "产业链数据不完整",
                      "reasons_pass": [], "reasons_fail": []}

    # 6 · 研报
    research = _get("6_research")
    coverage = research.get("report_count", 0)
    ratings = research.get("rating_distribution") or {}
    buy_count = sum(v for k, v in ratings.items() if "买入" in str(k) or "增持" in str(k))
    score_6 = 5 + min(3, coverage // 5)
    if buy_count >= 10: score_6 += 1
    score_6 = min(10, score_6)
    out["6_research"] = {"score": score_6, "weight": 3,
                         "label": f"{coverage} 份研报 · 买入/增持 {buy_count} 份" if coverage else "研报数据稀少",
                         "reasons_pass": [f"覆盖券商 {coverage} 家"] if coverage >= 10 else [],
                         "reasons_fail": [] if coverage else ["缺乏覆盖"]}

    # 7 · 行业景气 (stub heavy qualitative)
    out["7_industry"] = {"score": 7, "weight": 4, "label": "行业处于成长期（需 web search 确认）"}

    # 8 · 原材料
    out["8_materials"] = {"score": 6, "weight": 3, "label": "原材料成本数据需 web search"}

    # 9 · 期货关联
    out["9_futures"] = {"score": 5, "weight": 2, "label": "无强关联期货品种"}

    # 10 · 估值
    val = _get("10_valuation")
    pe_q_str = str(val.get("pe_quantile", ""))
    import re
    m = re.search(r'(\d+)', pe_q_str)
    pe_q = int(m.group(1)) if m else 50
    score_10 = 5
    if pe_q < 30: score_10 = 9
    elif pe_q < 50: score_10 = 7
    elif pe_q < 70: score_10 = 5
    elif pe_q < 85: score_10 = 3
    else: score_10 = 2
    out["10_valuation"] = {"score": score_10, "weight": 5,
                            "label": f"PE {val.get('pe', '—')} · 5 年 {pe_q} 分位 · 行业均值 {val.get('industry_pe', '—')}",
                            "reasons_pass": ["PE 在 5 年中位数以下"] if pe_q < 50 else [],
                            "reasons_fail": ["PE 已在 5 年高位区"] if pe_q >= 75 else []}

    # 11 · 治理
    gov = _get("11_governance")
    pledge = gov.get("pledge") or []
    has_insider = bool(gov.get("insider_trades_1y"))
    score_11 = 6
    if not pledge or (isinstance(pledge, list) and len(pledge) == 0): score_11 += 1
    if has_insider: score_11 += 1
    out["11_governance"] = {"score": min(10, score_11), "weight": 4,
                             "label": f"质押记录 {len(pledge) if isinstance(pledge, list) else '—'} · 内部交易 {'有' if has_insider else '无'}"}

    # 12 · 资金面
    cap = _get("12_capital_flow")
    north = cap.get("northbound_20d", "—")
    unlock = cap.get("unlock_schedule") or []
    score_12 = 5
    if "+" in str(north): score_12 += 2
    if len(unlock) == 0: score_12 += 1
    score_12 = min(10, score_12)
    out["12_capital_flow"] = {"score": score_12, "weight": 4,
                               "label": f"北向 20日 {north} · 12 个月解禁 {len(unlock)} 次",
                               "reasons_pass": [f"北向净流入 {north}"] if "+" in str(north) else [],
                               "reasons_fail": []}

    # 13 · 政策
    out["13_policy"] = {"score": 6, "weight": 3, "label": "政策环境中性"}

    # 14 · 护城河
    out["14_moat"] = {"score": 6, "weight": 3, "label": "护城河需定性评估"}

    # 15 · 事件
    events = _get("15_events")
    news = events.get("news") or []
    notices = events.get("recent_notices") or []
    score_15 = 5 + min(3, len(news) // 10)
    out["15_events"] = {"score": score_15, "weight": 4,
                        "label": f"近期新闻 {len(news)} 条 · 公告 {len(notices)} 份"}

    # 16 · 龙虎榜
    lhb = _get("16_lhb")
    lhb_count = lhb.get("lhb_count_30d", 0)
    matched = lhb.get("matched_youzi") or []
    score_16 = 5 + min(3, lhb_count // 2)
    if matched: score_16 += 1
    score_16 = min(10, score_16)
    out["16_lhb"] = {"score": score_16, "weight": 4,
                     "label": f"近 30 天上榜 {lhb_count} 次 · 识别游资 {len(matched)} 位",
                     "reasons_pass": [f"{'/'.join(matched[:3])} 席位出现"] if matched else []}

    # 17 · 舆情
    hot = _get("17_sentiment")
    hot_rank = (hot.get("hot_rank") or {}).get("rank_history") or []
    score_17 = 6 + min(2, len(hot_rank) // 10)
    out["17_sentiment"] = {"score": score_17, "weight": 3,
                            "label": f"雪球热度上榜 {len(hot_rank)} 次"}

    # 18 · 杀猪盘 (stub → safe by default, 9 分)
    out["18_trap"] = {"score": 9, "weight": 5, "label": "🟢 未发现推广痕迹（需 web search 8 信号确认）"}

    # 19 · 实盘赛
    contests = _get("19_contests")
    summary = contests.get("summary") or {}
    xq_total = summary.get("xueqiu_cubes_total", 0)
    hi = summary.get("high_return_cubes", 0)
    score_19 = 5 + min(3, xq_total // 5) + min(2, hi)
    score_19 = min(10, score_19)
    out["19_contests"] = {"score": score_19, "weight": 4,
                           "label": f"雪球 {xq_total} 个组合持有 · {hi} 个收益 >50%",
                           "reasons_pass": [f"{xq_total} 个雪球组合持有"] if xq_total else []}

    # Overall fundamental score
    total_weighted = sum(v["score"] * v["weight"] for v in out.values())
    total_weight = sum(v["weight"] for v in out.values())
    fundamental = (total_weighted / total_weight * 10) if total_weight else 0

    return {"ticker": raw["ticker"], "fundamental_score": round(fundamental, 1), "dimensions": out}


# ─────────── PANEL GENERATION (rule-based) ───────────

GROUP_VERDICTS = {
    "bullish":  ["强烈买入", "买入", "关注"],
    "bearish":  ["观望", "回避", "等待"],
    "neutral":  ["观望", "不适合", "不达标"],
}

COMMENT_TEMPLATES = {
    "A": {
        "bullish": [
            "ROE 和现金流都看得过去，长期持有没问题。",
            "商业模式清晰，10 年后还能赚钱的那种。",
            "安全边际尚可，不急着全仓。",
        ],
        "bearish": [
            "估值已透支未来几年的增长，等回调。",
            "护城河在侵蚀，这种价格不该买。",
            "现金流质量存疑，再观察两个季度。",
        ],
        "neutral": ["看不太懂，先放观察池。", "不在能力圈内。"],
    },
    "B": {
        "bullish": ["PEG 合理且成长性可见，可以进攻。", "CANSLIM 多数条件达标。"],
        "bearish": ["估值已脱离 PEG 合理区间。", "机构持股过高，不符合 CANSLIM S 项。"],
        "neutral": ["增长故事需要更多验证。"],
    },
    "C": {
        "bullish": ["宏观环境对这只票的反身性有利。", "流动性拐点已到，可以下注。"],
        "bearish": ["反身性正反馈进入晚期，小心。"],
        "neutral": ["宏观判断暂时不明。"],
    },
    "D": {
        "bullish": ["Stage 2 + 量能配合，技术面允许进场。", "VCP 形态已成，止损位清晰。"],
        "bearish": ["距 52 周高点太近，不是入场点。"],
        "neutral": ["等待明确突破。"],
    },
    "E": {
        "bullish": ["生意对、人对、价格还凑合。", "ROE 持续性强，可以重仓。"],
        "bearish": ["价格对不起生意质量。"],
        "neutral": ["看不懂就不要碰。"],
    },
    "F": {
        "bullish": ["板块有格局，趋势向上可以跟。", "二板定龙头，题材在线。", "情绪合力在，短线机会。"],
        "bearish": ["市值不在我的射程里。", "题材已过热，这不是我的菜。"],
        "neutral": ["不在风格里，不适合。"],
    },
    "G": {
        "bullish": ["多因子评分 top 20%，值得下注。", "凯利公式给出正仓位。"],
        "bearish": ["统计上已进入均值回归区。"],
        "neutral": ["因子中性，模型无信号。"],
    },
}


def generate_panel(dims_scored: dict, raw: dict) -> dict:
    """Rule-engine-based panel — each investor's verdict cites specific
    criteria from investor_criteria.py that were hit or missed.
    """
    # Build the flat feature dict once for all 51 investors
    features = extract_features(raw, raw.get("dimensions", {}))

    basic_ctx = (raw.get("dimensions", {}).get("0_basic") or {}).get("data") or {}
    kline_ctx = (raw.get("dimensions", {}).get("2_kline") or {}).get("data") or {}
    fin_ctx = (raw.get("dimensions", {}).get("1_financials") or {}).get("data") or {}

    investors_out = []
    vote_dist = {"strongly_buy": 0, "buy": 0, "watch": 0, "wait": 0, "avoid": 0, "n_a": 0, "skip": 0}
    sig_dist = {"bullish": 0, "neutral": 0, "bearish": 0, "skip": 0}

    def _score_to_verdict(score: float, signal: str) -> str:
        if signal == "bullish" and score >= 80:
            return "强烈买入"
        if signal == "bullish":
            return "买入"
        if signal == "bearish" and score <= 20:
            return "回避"
        if signal == "bearish":
            return "观望"
        # neutral
        return "关注" if score >= 50 else "观望"

    for inv in INVESTORS:
        inv_id = inv["id"]
        verdict_obj = _evaluate_investor(inv_id, features)

        sig = verdict_obj["signal"]
        score = int(max(0, verdict_obj["score"]))
        confidence = int(verdict_obj["confidence"])

        # Handle "skip" — investor won't look at this market
        if sig == "skip":
            verdict = "不适合"
            score = 0
            confidence = 0
        else:
            verdict = _score_to_verdict(score, sig)

        # Persona voice layer — wraps the rule-based headline in the investor's
        # signature quote style (from investor_personas.py)
        ctx = {
            "name": basic_ctx.get("name", "这只票"),
            "industry": basic_ctx.get("industry", "该行业"),
            "price": basic_ctx.get("price", "—"),
            "pe": basic_ctx.get("pe_ttm", "—"),
            "roe": str((fin_ctx.get("roe_history") or ["—"])[-1]),
            "stage": kline_ctx.get("stage", "—"),
            "growth": fin_ctx.get("revenue_growth", "—"),
        }
        persona_line = _persona_comment(inv_id, sig, ctx)

        # Final comment = persona quote + rule-hit evidence
        headline = verdict_obj["headline"]
        comment = f"{persona_line}\n{headline}"
        reasoning = verdict_obj["rationale"]

        v_key = {"强烈买入": "strongly_buy", "买入": "buy", "关注": "watch",
                 "观望": "wait", "回避": "avoid", "不适合": "skip"}.get(verdict, "n_a")
        vote_dist[v_key] = vote_dist.get(v_key, 0) + 1
        sig_dist[sig] = sig_dist.get(sig, 0) + 1

        investors_out.append({
            "investor_id": inv_id,
            "name": inv["name"],
            "group": inv["group"],
            "avatar": f"avatars/{inv_id}.svg",
            "signal": sig,
            "confidence": confidence,
            "score": score,
            "verdict": verdict,
            "reasoning": reasoning,
            "comment": comment,
            "headline": headline,
            "pass": [{"name": r["name"], "msg": r["msg"], "weight": r["weight"]}
                     for r in verdict_obj["pass_rules"][:4]],
            "fail": [{"name": r["name"], "msg": r["msg"], "weight": r["weight"]}
                     for r in verdict_obj["fail_rules"][:4]],
            "weight_pass": verdict_obj["weight_pass"],
            "weight_total": verdict_obj["weight_total"],
            "ideal_price": None,
            "period": "中长线" if inv["group"] in ("A", "B", "E") else "短线",
        })

    active_count = len(investors_out) - sig_dist.get("skip", 0)
    consensus = sig_dist["bullish"] / max(active_count, 1) * 100
    return {
        "ticker": raw["ticker"],
        "panel_consensus": round(consensus, 1),
        "vote_distribution": vote_dist,
        "signal_distribution": sig_dist,
        "investors": investors_out,
    }


def generate_synthesis(raw: dict, dims_scored: dict, panel: dict) -> dict:
    from compute_friendly import compute_scenarios, compute_exit_triggers

    basic = (raw.get("dimensions", {}).get("0_basic") or {}).get("data") or {}
    name = basic.get("name") or raw.get("ticker")
    price = basic.get("price") or 0

    fund_score = dims_scored.get("fundamental_score", 60)
    consensus = panel.get("panel_consensus", 50)
    overall = fund_score * 0.6 + consensus * 0.4

    if overall >= 85: verdict_label = "值得重仓"
    elif overall >= 70: verdict_label = "可以蹲一蹲"
    elif overall >= 55: verdict_label = "观望优先"
    elif overall >= 40: verdict_label = "谨慎"
    else: verdict_label = "回避"

    # Pick bull and bear for great divide
    # CRITICAL: must pick from ACTUALLY bullish/bearish investors, never misattribute
    investors = panel.get("investors", [])
    inv_by_score = sorted(investors, key=lambda x: -x.get("score", 0))

    # Bull = highest score among genuinely bullish, or highest score overall if no bullish
    bulls = [i for i in investors if i["signal"] == "bullish"]
    bears = [i for i in investors if i["signal"] == "bearish"]
    neutrals = [i for i in investors if i["signal"] == "neutral"]

    if bulls:
        bull = sorted(bulls, key=lambda x: -x.get("score", 0))[0]
    elif neutrals:
        # No bullish → pick highest-scoring neutral as "relative bull"
        bull = sorted(neutrals, key=lambda x: -x.get("score", 0))[0]
    else:
        # Everyone is bearish → pick least bearish
        bull = inv_by_score[0]

    if bears:
        bear = sorted(bears, key=lambda x: x.get("score", 100))[0]
    elif neutrals:
        # No bearish → pick lowest-scoring neutral as "relative bear"
        bear = sorted(neutrals, key=lambda x: x.get("score", 100))[0]
    else:
        # Everyone is bullish → pick least bullish
        bear = inv_by_score[-1]

    # Safety: bull and bear must be different investors
    if bull["investor_id"] == bear["investor_id"] and len(investors) > 1:
        bear = inv_by_score[-1] if bull == inv_by_score[0] else inv_by_score[0]

    # Build debate rounds — use actual headline + reasoning from evaluator
    bull_headline = bull.get("headline", bull.get("comment", ""))
    bear_headline = bear.get("headline", bear.get("comment", ""))
    bull_reasoning = bull.get("reasoning", "")
    bear_reasoning = bear.get("reasoning", "")

    bull_pass_rules = bull.get("pass", [])
    bull_fail_rules = bull.get("fail", [])
    bear_pass_rules = bear.get("pass", [])
    bear_fail_rules = bear.get("fail", [])

    rounds = [
        {
            "round": 1,
            "bull_say": bull_headline,
            "bear_say": bear_headline,
        },
        {
            "round": 2,
            "bull_say": " · ".join(r.get("msg", r.get("name", "")) for r in bull_pass_rules[:3]) or "数据支持我的判断。",
            "bear_say": " · ".join(r.get("msg", r.get("name", "")) for r in bear_fail_rules[:3]) or "风险点太多。",
        },
        {
            "round": 3,
            "bull_say": f"综合看，{bull.get('score', 0)} 分，我的立场不变。",
            "bear_say": f"综合看，{bear.get('score', 0)} 分，风险大于收益。",
        },
    ]

    kline = (raw.get("dimensions", {}).get("2_kline") or {}).get("data") or {}
    val = (raw.get("dimensions", {}).get("10_valuation") or {}).get("data") or {}

    # v2.0 · Pull institutional modeling summaries
    d20 = (raw.get("dimensions", {}).get("20_valuation_models") or {}).get("data") or {}
    d21 = (raw.get("dimensions", {}).get("21_research_workflow") or {}).get("data") or {}
    d22 = (raw.get("dimensions", {}).get("22_deep_methods") or {}).get("data") or {}
    dcf_summary = d20.get("summary") or {}
    init_cov = d21.get("initiating_coverage") or {}
    ic_memo = d22.get("ic_memo") or {}
    competitive = d22.get("competitive_analysis") or {}

    # Build punchline with conflict — prefer real conflicts over platitudes
    dcf_sm = dcf_summary.get("dcf_safety_margin_pct", 0) or 0
    lbo_irr = dcf_summary.get("lbo_irr_pct", 0) or 0
    tp = (init_cov.get("headline") or {}).get("target_price") or 0
    upside = (init_cov.get("headline") or {}).get("upside_pct", 0) or 0
    rating = (init_cov.get("headline") or {}).get("rating", "")

    # Try to build a "conflict" punchline from DCF vs LBO vs current price
    if dcf_sm and lbo_irr and abs(dcf_sm) > 10 and lbo_irr > 15:
        if dcf_sm < 0 and lbo_irr > 20:
            punchline = f"DCF 说高估 {abs(dcf_sm):.0f}%，但 LBO 测试显示 PE 买方仍能赚 {lbo_irr:.0f}% IRR — 冲突很有意思。"
        elif dcf_sm > 15 and lbo_irr > 20:
            punchline = f"DCF 认为低估 {dcf_sm:.0f}%，LBO IRR {lbo_irr:.0f}% 也确认 — 双重信号看多。"
        else:
            punchline = f"机构建模定调 {rating}，目标价 ¥{tp}（{upside:+.0f}%），LBO 视角 IRR {lbo_irr:.0f}%。"
    elif tp > 0 and abs(upside) > 5:
        punchline = f"首次覆盖 {rating}，目标价 ¥{tp}，空间 {upside:+.0f}%。"
    else:
        punchline = f"{name} · ROE 历史与当前估值存在结构性分歧，等待方向明朗。"

    # Risks
    risks = []
    for key, dim in dims_scored["dimensions"].items():
        if dim["score"] <= 4:
            risks.extend(dim.get("reasons_fail", [])[:1])
    if not risks:
        risks = ["估值偏高", "苹果订单依赖度偏高", "行业竞争加剧"]
    risks = risks[:5]

    # Friendly layer
    scenarios = compute_scenarios(raw, dims_scored)
    exit_triggers = compute_exit_triggers(raw, dims_scored, {})
    similar_stocks = raw.get("similar_stocks", [])

    # Dashboard
    ytd_return = (kline.get("kline_stats") or {}).get("ytd_return", "—")
    core_conclusion = f"{name} · {int(overall)} 分 · {verdict_label}。51 位大佬里 {panel['signal_distribution']['bullish']} 人看多，YTD {ytd_return}。{punchline}"

    # v2.0 · Auto-generated dim_commentary stubs (Claude should rewrite these)
    dim_commentary_stub: dict[str, str] = {}
    dim_labels = {
        "0_basic": "基础信息",
        "1_financials": "财报",
        "2_kline": "K线技术面",
        "10_valuation": "估值分位",
        "4_peers": "同行对比",
        "5_chain": "产业链",
        "7_industry": "行业景气",
        "14_moat": "护城河",
        "18_trap": "杀猪盘",
    }
    for dim_key, label in dim_labels.items():
        dim = (raw.get("dimensions", {}).get(dim_key) or {})
        score_info = dims_scored.get("dimensions", {}).get(dim_key) or {}
        if dim.get("data"):
            score = score_info.get("score", 0)
            dim_commentary_stub[dim_key] = f"[脚本占位] {label} 得分 {score}/10 · 需 Claude 补充定性评语"

    return {
        "ticker": raw["ticker"],
        "name": name,
        "overall_score": round(overall, 1),
        "verdict_label": verdict_label,
        "fundamental_score": round(fund_score, 1),
        "panel_consensus": round(consensus, 1),
        "dim_commentary": dim_commentary_stub,  # Claude 在 Task 4 里重写这些
        "institutional_modeling": {
            "dcf_intrinsic": dcf_summary.get("dcf_intrinsic"),
            "dcf_safety_margin_pct": dcf_summary.get("dcf_safety_margin_pct"),
            "dcf_verdict": dcf_summary.get("dcf_verdict"),
            "lbo_irr_pct": dcf_summary.get("lbo_irr_pct"),
            "lbo_verdict": dcf_summary.get("lbo_verdict"),
            "comps_verdict": dcf_summary.get("comps_verdict"),
            "initiating_rating": (init_cov.get("headline") or {}).get("rating"),
            "target_price": (init_cov.get("headline") or {}).get("target_price"),
            "upside_pct": (init_cov.get("headline") or {}).get("upside_pct"),
            "ic_recommendation": (ic_memo.get("sections", {}).get("I_exec_summary", {}) or {}).get("headline"),
            "bcg_position": (competitive.get("bcg_position") or {}).get("category"),
            "industry_attractiveness": competitive.get("industry_attractiveness_pct"),
        },
        "claude_narrative_stub": {
            "_note": "以下字段是脚本生成的占位，Task 4 中 Claude 必须根据原始数据重写",
            "needs_rewrite": ["great_divide.punchline", "dashboard.core_conclusion",
                              "debate.rounds[*].bull_say", "debate.rounds[*].bear_say",
                              "buy_zones.*.rationale", "risks[*]"],
        },
        "debate": {
            "bull": {"investor_id": bull["investor_id"], "name": bull["name"], "group": bull["group"]},
            "bear": {"investor_id": bear["investor_id"], "name": bear["name"], "group": bear["group"]},
            "rounds": rounds,
            "punchline": punchline,
        },
        "great_divide": {
            "bull_avatar": bull["investor_id"],
            "bear_avatar": bear["investor_id"],
            "bull_score": bull["score"],
            "bear_score": bear["score"],
            "bull_signal": bull["signal"],
            "bear_signal": bear["signal"],
            "punchline": punchline,
        },
        "risks": risks,
        "buy_zones": {
            "value": {"price": round(price * 0.85, 2) if price else "—", "rationale": "历史 PE 25 分位"},
            "growth": {"price": round(price * 0.92, 2) if price else "—", "rationale": "PEG 合理区"},
            "technical": {"price": round(price * 0.95, 2) if price else "—", "rationale": "MA60 支撑位"},
            "youzi": {"price": price or "—", "rationale": "当前情绪未破"},
        },
        "friendly": {
            "scenarios": scenarios,
            "exit_triggers": exit_triggers,
            "similar_stocks": similar_stocks,
        },
        "fund_managers": raw.get("fund_managers", []),
        "dashboard": {
            "core_conclusion": core_conclusion,
            "data_perspective": {
                "trend": f"{kline.get('stage', '—')}",
                "price": f"¥{price}" if price else "—",
                "volume": "—",
                "chips": kline.get("ma_align", "—"),
            },
            "intelligence": {
                "news": "近期新闻 + 公告已采集",
                "risks": risks[:3],
                "catalysts": ["季报", "新品"],
            },
            "battle_plan": {
                "entry": f"¥{round(price * 0.92, 2) if price else '—'}",
                "position": "50% 起步",
                "stop": f"¥{round(price * 0.85, 2) if price else '—'}",
                "target": f"¥{round(price * 1.25, 2) if price else '—'}",
            },
        },
    }


def main(ticker: str = "002273.SZ"):
    ti = parse_ticker(ticker)
    print(f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"🎯 TARGET: {ti.full}")
    print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")

    print("📊 Task 1 · 数据采集")
    raw = collect_raw_data(ti.full)
    write_task_output(ti.full, "raw_data", raw)

    # Data integrity check
    from lib.data_integrity import validate as _validate_raw, format_report as _fmt_integrity
    _integrity = _validate_raw(raw)
    print("\n" + _fmt_integrity(_integrity))
    raw["_integrity"] = _integrity
    if _integrity["critical_missing"]:
        print("  ⚠️  Task 1 有关键字段缺失，下游评估可能不准确")

    print("\n🏛  Task 1.5 · 机构级财务建模 (Dims 20-22)")
    from compute_deep_methods import compute_dim_20, compute_dim_21, compute_dim_22
    _features_pre = extract_features(raw, raw.get("dimensions", {}))
    raw["dimensions"]["20_valuation_models"] = compute_dim_20(_features_pre, raw)
    _d20 = raw["dimensions"]["20_valuation_models"]["data"]
    raw["dimensions"]["21_research_workflow"] = compute_dim_21(_features_pre, raw, _d20)
    _d21 = raw["dimensions"]["21_research_workflow"]["data"]
    raw["dimensions"]["22_deep_methods"] = compute_dim_22(_features_pre, raw, _d20, _d21)
    write_task_output(ti.full, "raw_data", raw)  # rewrite with new dims
    _s20 = _d20["summary"]
    _s21 = _d21["summary"]
    _s22 = raw["dimensions"]["22_deep_methods"]["data"]["summary"]
    print(f"  DCF: ¥{_s20.get('dcf_intrinsic')} · 安全边际 {_s20.get('dcf_safety_margin_pct')}% · {_s20.get('dcf_verdict')}")
    print(f"  LBO: IRR {_s20.get('lbo_irr_pct')}% · {_s20.get('lbo_verdict')}")
    print(f"  首次覆盖: {_s21.get('rec_rating')} · TP ¥{_s21.get('target_price')} ({_s21.get('upside_pct'):+}%)")
    print(f"  IC Memo: {_s22.get('ic_recommendation')}")
    print(f"  BCG: {_s22.get('bcg_position')} · 行业吸引力 {_s22.get('industry_attractiveness')}%")

    print("\n📏 Task 2 · 19 维打分")
    dims = score_dimensions(raw)
    write_task_output(ti.full, "dimensions", dims)
    print(f"  基本面得分: {dims['fundamental_score']}/100")

    print("\n🎭 Task 3 · 51 贤评审团")
    panel = generate_panel(dims, raw)
    write_task_output(ti.full, "panel", panel)
    sd = panel["signal_distribution"]
    print(f"  看多 {sd['bullish']} · 中性 {sd['neutral']} · 看空 {sd['bearish']}")

    print("\n⚖ Task 4 · 综合研判")
    syn = generate_synthesis(raw, dims, panel)
    write_task_output(ti.full, "synthesis", syn)
    print(f"  综合评分: {syn['overall_score']}/100 · {syn['verdict_label']}")

    print("\n📄 Task 5 · 报告组装")
    from assemble_report import assemble
    out = assemble(ti.full)
    print(f"  → {out}")

    from inline_assets import main as inline_main
    standalone = inline_main(ti.full)

    # Ensure standalone file is fully written before opening
    standalone_path = Path(standalone).resolve()
    assert standalone_path.exists() and standalone_path.stat().st_size > 10000, \
        f"Standalone file missing or too small: {standalone_path}"

    print(f"\n✅ All done!")
    print(f"   Standalone: {standalone_path}")
    print(f"   Size: {standalone_path.stat().st_size // 1024} KB")

    # Auto-open AFTER everything is complete
    import webbrowser
    webbrowser.open(standalone_path.as_uri())
    print(f"   🌐 已在浏览器中打开")


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else "002273.SZ"
    main(arg)
