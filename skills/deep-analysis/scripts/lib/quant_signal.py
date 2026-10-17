"""量化基金 / 量化重仓信号检测 · v2.7

不维护具体基金白名单（基金名字未必含"量化"）。改用结构性特征：
**第一大持仓占净值 < 2% → 疑似量化基金**
理由：量化基金持 100-300 只股票分散持仓；主动价值/集中型基金第一大常 8-10%。

判定流程：
1. 对每只持有目标股票的基金（来自 `raw["fund_managers"]`）
2. 抓它的前 10 大持仓（发现缓存 1h，报告期快照 24h）
3. 若 top-1 < 2% → 疑似量化
4. 若该量化基金的 top-10 含目标股票 → 计入 quant_holders
5. 同一报告期 count >= QUANT_FACTOR_MIN_COUNT (3) → quant_factor style 触发

使用：
    from lib.quant_signal import detect_quant_signal
    sig = detect_quant_signal("600120.SH", raw["fund_managers"])
    if sig["is_quant_factor_style"]:
        # mark stock as quant_factor type
"""
from __future__ import annotations

import os
import math
import re
import calendar
from datetime import date, datetime
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor

from .cache import cached, TTL_QUARTERLY, TTL_HOURLY

try:
    import akshare as ak  # type: ignore
except ImportError:
    ak = None


QUANT_TOP1_THRESHOLD = 2.0       # 第一大持仓占净值 < 2% → 疑似量化
QUANT_FACTOR_MIN_COUNT = 3       # 至少 3 家量化基金 top-10 持有 → 触发 style
HOLDINGS_MAX_AGE_DAYS = 180  # Research freshness budget, not a filing deadline.

# 扫描预算 · 判定只需凑够 QUANT_FACTOR_MIN_COUNT 家即可定性，
# 没必要对全部持仓基金逐只抓 top-10（贵州茅台实测 695 家 → 695 次 akshare 调用）。
_QUANT_MAX_FUNDS_BY_DEPTH = {"lite": 20, "medium": 80, "deep": 0}   # 0 = 不限制
_QUANT_MAX_FUNDS_FALLBACK = 80


def quant_max_funds() -> int:
    """本次量化扫描参与的基金数上限 · 0 表示不限制.

    优先级：``UZI_QUANT_MAX_FUNDS`` env > 按当前 depth 档位默认
    （lite 20 / medium 80 / deep 不限）。档位不可读时回落 80。
    """
    raw = os.environ.get("UZI_QUANT_MAX_FUNDS")
    if raw is not None:
        try:
            value = int(raw)
            if value >= 0:
                return value
        except ValueError:
            pass
    try:
        from .analysis_profile import get_profile
        depth = get_profile().depth
    except Exception:
        depth = "medium"
    return _QUANT_MAX_FUNDS_BY_DEPTH.get(depth, _QUANT_MAX_FUNDS_FALLBACK)


# ─── 私募量化备查名单 ──
# 私募不公开持仓，本模块无法直接抓取；agent 走 web search / LHB / 大股东交叉验证
KNOWN_PRIVATE_QUANTS: tuple[str, ...] = (
    "幻方", "九坤", "灵均", "鸣石", "因诺", "明汯", "玄信", "衍复", "宽德", "念空",
    "锐天", "九章", "信弘", "汐泰", "黑翼", "白鹭", "诚奇", "盛冠达", "千象",
)


def _today() -> date:
    return datetime.now(ZoneInfo("Asia/Shanghai")).date()


def _report_period(value) -> date | None:
    match = re.search(r"(\d{4})年([1-4])季度", str(value))
    if not match:
        return None
    year, quarter = map(int, match.groups())
    month = quarter * 3
    try:
        return date(year, month, calendar.monthrange(year, month)[1])
    except ValueError:
        return None


def _weight(row: dict) -> float | None:
    try:
        value = float(str(row.get("占净值比例", "")).strip().rstrip("%"))
        return value if math.isfinite(value) and 0 < value <= 100 else None
    except (ValueError, TypeError):
        return None


def _fetch_top_holdings(fund_code: str, top_n: int = 10) -> list[dict]:
    """Select the latest returned quarter, never mix quarters or cache errors.

    The provider exposes report periods, not publication timestamps. This is a
    current snapshot only, not point-in-time evidence for a historical backtest.
    """
    fund_code = str(fund_code).strip()
    if not re.fullmatch(r"[0-9]{6}", fund_code) or ak is None or top_n <= 0:
        return []
    today = _today()
    for year in (today.year, today.year - 1):
        def fetch_year():
            df = ak.fund_portfolio_hold_em(symbol=fund_code, date=str(year))
            if df is None or df.empty:
                raise ValueError("No disclosed holdings")
            return df.to_dict("records")
        try:
            rows = cached(f"_quant/{fund_code}", f"holdings_v2_year_{year}_{today.isoformat()}",
                          fetch_year, ttl=TTL_HOURLY)
            periods = [(row, _report_period(row.get("季度"))) for row in rows]
            periods = [(row, period) for row, period in periods
                       if period is not None and period.year == year and period <= today]
            if not periods:
                continue
            latest = max(period for _, period in periods)
            selected = [dict(row) for row, period in periods if period == latest]
            weights = [_weight(row) for row in selected]
            status = "available"
            if any(value is None for value in weights):
                status = "invalid_weights"
            elif (today - latest).days > HOLDINGS_MAX_AGE_DAYS:
                status = "stale"
            selected.sort(key=lambda row: -(_weight(row) or 0))
            codes = [str(row.get("股票代码", "")).strip() for row in selected]
            if not all(codes) or len(codes) != len(set(codes)):
                status = "invalid_holdings"
            if len(selected) < 10 and status == "available":
                status = "incomplete"
            for row in selected:
                row.update(report_period=latest.isoformat(), holdings_status=status,
                           observed_at=today.isoformat(), publication_date_verified=False)
            # Separate immutable-period snapshots from the short-lived discovery cache.
            cached(f"_quant/{fund_code}", f"holdings_v2_period_{latest.isoformat()}_{today.isoformat()}",
                   lambda: selected, ttl=TTL_QUARTERLY)
            return selected[:top_n]
        except Exception:
            continue
    return []


def _is_quant_like(top_holdings: list[dict]) -> tuple[bool, float]:
    """结构性判定：top-1 占净值 < QUANT_TOP1_THRESHOLD → True
    返回 (is_quant_like, top1_pct)"""
    if not top_holdings:
        return False, 0.0
    weights = [_weight(row) for row in top_holdings]
    if any(value is None for value in weights):
        return False, 0.0
    top1_pct = max(weights)
    return top1_pct < QUANT_TOP1_THRESHOLD, top1_pct


def _fetch_all_holding_funds(ticker_code: str, max_funds: int = 80) -> list[dict]:
    """直接调 fetch_fund_holders.fetch_holding_funds 拿全部持有本股的基金（不受
    run_real_test 默认 limit=6 限制）。

    raw["fund_managers"] 默认只有 6 个（按 5Y 收益率 top 6）；做量化信号判定需要
    更大样本（至少 50-80）才靠谱。
    """
    if ak is None:
        return []
    try:
        # Avoid circular import — fetch_fund_holders is in scripts/, lib/ is its peer
        import sys
        from pathlib import Path
        scripts_dir = Path(__file__).resolve().parent.parent
        if str(scripts_dir) not in sys.path:
            sys.path.insert(0, str(scripts_dir))
        from fetch_fund_holders import fetch_holding_funds
        all_funds = fetch_holding_funds(ticker_code)
        # Normalize: each entry must have at least 基金代码 + 基金名称
        out = []
        for h in (all_funds[:max_funds] if max_funds else all_funds):
            code = str(h.get("基金代码") or h.get("fund_code") or "").strip()
            name = str(h.get("基金名称") or h.get("fund_name") or "").strip()
            if code:
                out.append({"fund_code": code, "fund_name": name})
        return out
    except Exception:
        return []


def detect_quant_signal(stock_code: str, fund_managers: list[dict] | None = None,
                        max_funds: int | None = None) -> dict:
    """对所有持有本股的基金，识别量化属性 + 是否 top-10 持有。

    Args:
        stock_code: full ticker like "600120.SH" or "600120"
        fund_managers: list from raw["fund_managers"]; if empty/None, will auto-fetch
                       up to max_funds via fetch_holding_funds (不受 wave3 limit=6 制约)
        max_funds: 参与扫描的基金数上限 · None → quant_max_funds()（按 depth 档位取默认值）。
                   0 → 不限制。既约束自动扩容的样本，也约束调用方传入的列表。

    Returns:
        {
            "count": 5,                       # quant funds with target in top-10
            "quant_funds": [                  # sorted by weight_pct desc
                {"name": "国金量化多因子A", "fund_code": "006195",
                 "rank": 4, "weight_pct": 2.3, "top1_pct": 1.8,
                 "manager": "马芳"},
                ...
            ],
            "active_funds_total": 50,         # how many funds checked
            "quant_funds_total": 12,          # how many were detected as quant-like
            "is_quant_factor_style": True,    # count >= MIN_COUNT
        }
    """
    ticker_parts = (stock_code or "").strip().upper().split(".")
    code5 = ticker_parts[0]
    cap = quant_max_funds() if max_funds is None else max_funds
    if not isinstance(cap, int) or isinstance(cap, bool) or cap < 0:
        raise ValueError("max_funds must be a non-negative integer")

    empty = {
        "count": 0, "quant_funds": [], "active_funds_total": 0, "quant_funds_total": 0,
        "is_quant_factor_style": None, "evidence_status": "unavailable",
        "verified_funds_total": 0, "unavailable_funds_total": 0,
        "report_periods": [], "counts_by_period": {}, "unavailable_reasons": {},
        "publication_date_verified": False, "scope": "scanned_funds_only",
    }
    if (not re.fullmatch(r"[0-9]{6}", code5) or len(ticker_parts) > 2
            or len(ticker_parts) == 2 and ticker_parts[1] not in {"SH", "SZ", "BJ"}):
        return {**empty, "evidence_status": "not_applicable"}

    # 样本太小时（如 wave3 默认 6 个）自动拉更大样本
    if not fund_managers or len(fund_managers) < 20:
        bigger = _fetch_all_holding_funds(
            code5, max_funds=cap)
        if bigger:
            fund_managers = bigger

    if not fund_managers:
        return empty

    # A repeated share-class/fund code must not count as multiple votes.
    fund_managers = list({str(m.get("fund_code")).strip(): {
        **m, "fund_code": str(m.get("fund_code")).strip()
    } for m in fund_managers if isinstance(m, dict) and str(m.get("fund_code") or "").strip()}.values())
    if not fund_managers:
        return empty

    # 此前 cap 只作用于"自动扩容"分支：调用方传入的完整列表（贵州茅台实测 695 家，
    # fetch_holding_funds v2.4 起 limit=None 不封顶）会被逐只扫完 → 695 次 akshare
    # 调用，lite 档冷跑因此耗时 17 分钟。此处对已有列表统一施加预算。
    # 列表在 fetch_fund_holders 中已按持仓比例降序，取前 cap 家即最大的 cap 家。
    if cap and len(fund_managers) > cap:
        fund_managers = fund_managers[:cap]

    def _check_one(m: dict) -> dict | None:
        fund_code = m.get("fund_code")
        if not fund_code:
            return None
        top10 = _fetch_top_holdings(str(fund_code), top_n=10)
        if not top10 or any(row.get("holdings_status") != "available" for row in top10):
            return {"_unavailable": top10[0].get("holdings_status", "unavailable") if top10 else "unavailable",
                    "report_period": top10[0].get("report_period") if top10 else None}
        period = top10[0]["report_period"]
        is_q, top1_pct = _is_quant_like(top10)
        if not is_q:
            return {"_not_quant": True, "report_period": period}
        # ✓ 量化基金 — 检查目标股票是否在 top-10
        for rank, h in enumerate(top10, start=1):
            holding_code = str(h.get("股票代码", "")).strip()
            if holding_code == code5:
                weight_pct = _weight(h)
                return {
                    "name": str(m.get("fund_name", "") or ""),
                    "fund_code": str(fund_code),
                    "rank": rank,
                    "weight_pct": weight_pct,
                    "top1_pct": top1_pct,
                    "manager": str(m.get("name", "") or ""),
                    "report_period": period,
                }
        # 量化但不持目标股票
        return {"_quant_no_hold": True, "report_period": period}

    quant_holders: list[dict] = []
    quant_total = 0
    unavailable = {}
    periods = set()

    # workers 默认 1 — 防 mini_racer V8 isolate crash（Py3.13）；
    # akshare.fund_portfolio_hold_em 内部用 JS 解码，多线程不安全。
    import os as _os
    try:
        _w = min(8, max(1, int(_os.environ.get("UZI_QUANT_WORKERS", "1"))))
    except ValueError:
        _w = 1
    with ThreadPoolExecutor(max_workers=_w) as pool:
        for r in pool.map(_check_one, fund_managers):
            if r is None:
                continue
            if r.get("report_period"):
                periods.add(r["report_period"])
            if r.get("_unavailable"):
                reason = r["_unavailable"]
                unavailable[reason] = unavailable.get(reason, 0) + 1
                continue
            if r.get("_not_quant"):
                continue
            quant_total += 1
            if r.get("_quant_no_hold"):
                continue
            quant_holders.append(r)

    quant_holders.sort(key=lambda x: -x.get("weight_pct", 0))
    by_period = {period: sum(h["report_period"] == period for h in quant_holders) for period in periods}
    matched = max(by_period.values(), default=0) >= QUANT_FACTOR_MIN_COUNT
    unknown_count = sum(unavailable.values())

    return {
        "count": len(quant_holders),
        "quant_funds": quant_holders[:10],
        "active_funds_total": len(fund_managers),
        "quant_funds_total": quant_total,
        "is_quant_factor_style": True if matched else None if unknown_count else False,
        "evidence_status": "partial" if unknown_count and unknown_count < len(fund_managers)
                           else "unavailable" if unknown_count else "available",
        "verified_funds_total": len(fund_managers) - unknown_count,
        "unavailable_funds_total": unknown_count,
        "unavailable_reasons": unavailable,
        "report_periods": sorted(periods),
        "counts_by_period": by_period,
        "publication_date_verified": False,
        "scope": "scanned_funds_only",
    }


if __name__ == "__main__":
    import json
    import sys

    code = sys.argv[1] if len(sys.argv) > 1 else "600120.SH"

    # Read existing raw_data cache for testing
    from pathlib import Path
    cache = Path(".cache") / code / "raw_data.json"
    if not cache.exists():
        print(f"No cached raw_data for {code}; run stage1 first.")
        sys.exit(1)

    raw = json.loads(cache.read_text(encoding="utf-8"))
    fund_managers = raw.get("fund_managers", [])
    print(f"Testing {code} · {len(fund_managers)} fund_managers from cache")

    sig = detect_quant_signal(code, fund_managers)
    print(json.dumps(sig, ensure_ascii=False, indent=2, default=str))
