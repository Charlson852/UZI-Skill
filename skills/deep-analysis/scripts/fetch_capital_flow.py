"""Dimension 12 · 资金面 (北向 / 融资融券 / 股东户数 / 主力 / 限售解禁 / 大宗交易).

codex-develop:
- 避免误用全市场超重接口拖垮 A 股 stage1
- 机构持仓历史改走单票接口
- 解禁信息改走个股 queue 接口而非整年全市场明细
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta

import akshare as ak  # type: ignore
import requests
from lib import data_sources as ds
from lib.market_router import parse_ticker


def _safe(fn, default):
    try:
        return fn()
    except Exception as e:
        return {"error": str(e)} if isinstance(default, dict) else default


def _last_n_quarters(n: int = 8) -> list[tuple[str, str]]:
    """Return recent quarters in Sina format plus a compact label.

    Example: [("20241", "24Q1"), ("20242", "24Q2"), ...]
    """
    now = datetime.now()
    cur_q = ((now.month - 1) // 3) + 1
    year = now.year
    out: list[tuple[str, str]] = []
    for i in range(n):
        q = cur_q - i
        y = year
        while q <= 0:
            q += 4
            y -= 1
        out.append((f"{y}{q}", f"{str(y)[2:]}Q{q}"))
    out.reverse()
    return out


def _recent_trade_dates(days: int = 7) -> list[str]:
    today = datetime.now().date()
    out: list[str] = []
    cursor = today
    while len(out) < days:
        if cursor.weekday() < 5:
            out.append(cursor.strftime("%Y%m%d"))
        cursor -= timedelta(days=1)
    out.reverse()
    return out


def _fetch_margin_recent(ti) -> list[dict]:
    """Use lightweight market summaries instead of heavy full-market detail tables."""
    try:
        if ti.full.endswith("SH"):
            dates = _recent_trade_dates(5)
            start_date, end_date = dates[0], dates[-1]
            df = ak.stock_margin_sse(start_date=start_date, end_date=end_date)
            return [] if df is None or df.empty else df.tail(5).to_dict("records")

        rows: list[dict] = []
        for date in reversed(_recent_trade_dates(8)):
            try:
                df = ak.stock_margin_szse(date=date)
                if df is None or df.empty:
                    continue
                rec = df.iloc[0].to_dict()
                rec["信用交易日期"] = date
                rows.append(rec)
                if len(rows) >= 5:
                    break
            except Exception:
                continue
        rows.reverse()
        return rows
    except Exception:
        return []


def _fetch_holder_count_history(code: str, limit: int = 8) -> list[dict]:
    """Fetch shareholder-count history for a single stock via Eastmoney filtered query."""
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "END_DATE,HOLD_NOTICE_DATE",
        "sortTypes": "-1,-1",
        "pageSize": str(limit),
        "pageNumber": "1",
        "reportName": "RPT_HOLDERNUM_DET",
        "columns": (
            "SECURITY_CODE,SECURITY_NAME_ABBR,END_DATE,HOLD_NOTICE_DATE,HOLDER_NUM,PRE_HOLDER_NUM,"
            "HOLDER_NUM_CHANGE,HOLDER_NUM_RATIO,AVG_HOLD_NUM,AVG_MARKET_CAP,TOTAL_MARKET_CAP"
        ),
        "filter": f'(SECURITY_CODE="{code}")',
        "source": "WEB",
        "client": "WEB",
    }
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    data_json = r.json()
    result = (((data_json or {}).get("result") or {}).get("data")) or []
    return result[:limit]


def main(ticker: str) -> dict:
    ti = parse_ticker(ticker)
    if ti.market == "H":
        # v2.5 · HK 港股通南北向标记 + 历史每日市值（净值变动 proxy）
        # akshare 港股通南北向 spot (stock_hsgt_sh_hk_spot_em) 走 push2 已 blocked，
        # 这里用 stock_hk_security_profile_em 拿"是否沪/深港通标的"标记 + eniu 历史市值。
        from lib.hk_data_sources import fetch_hk_basic_combined
        try:
            enriched = fetch_hk_basic_combined(ti.code.zfill(5))
        except Exception:
            enriched = {}
        is_sh = enriched.get("is_south_bound_sh", False)
        is_sz = enriched.get("is_south_bound_sz", False)
        # eniu 市值历史（近 30 个数据点作为南北向资金流的 proxy）
        mv_hist: list = []
        try:
            import akshare as _ak  # type: ignore
            df = _ak.stock_hk_indicator_eniu(symbol=f"hk{ti.code.zfill(5)}", indicator="市值")
            if df is not None and not df.empty:
                mv_hist = df.tail(30).to_dict("records")
        except Exception:
            pass
        return {
            "ticker": ti.full,
            "data": {
                "is_south_bound_sh": is_sh,
                "is_south_bound_sz": is_sz,
                "south_bound_eligibility": "沪+深" if (is_sh and is_sz) else ("沪" if is_sh else ("深" if is_sz else "—")),
                "north_bound": "—",
                "margin_balance": "—",
                "main_flow_recent": [],
                "mv_history_30d": mv_hist[-30:],
                "_note": (
                    "HK 南向具体持股变动需走 AASTOCKS Playwright 或 hkexnews holdings page；"
                    "本字段提供港股通资格 + eniu 市值历史作 proxy。"
                ),
            },
            "source": "akshare:stock_hk_security_profile_em + stock_hk_indicator_eniu",
            "fallback": False,
        }
    if ti.market != "A":
        return {"ticker": ti.full, "data": {"_note": "capital_flow only A-share / HK for now"}, "source": "skip", "fallback": False}

    north = ds.fetch_northbound(ti)

    margin = _safe(lambda: _fetch_margin_recent(ti), [])

    holders = _safe(lambda: _fetch_holder_count_history(ti.code, limit=8), [])

    main_flow = _safe(
        lambda: ak.stock_individual_fund_flow(stock=ti.code, market=ti.full[-2:].lower()).tail(20).to_dict("records"),
        [],
    )

    # 大宗交易：保守降级。历史全市场扫描过重，先不让它拖垮 stage1。
    block_trades = []

    # 限售股解禁：使用个股 queue 接口，避免整年全市场明细扫描。
    unlock_future = _safe(
        lambda: ak.stock_restricted_release_queue_em(symbol=ti.code).head(20).to_dict("records"),
        [],
    )
    unlock = unlock_future[:10] if unlock_future else []

    # Normalize unlock_schedule for viz
    def _month_label(d):
        s = str(d)[:7].replace("-", "")
        if len(s) == 6:
            return f"{s[2:4]}-{s[4:6]}"
        return s[-5:] if s else "—"

    unlock_schedule = []
    for row in (unlock_future or [])[:12]:
        date = row.get("解禁日期") or row.get("解禁时间") or ""
        amount_str = row.get("解禁市值") or row.get("市值(亿元)") or row.get("解禁股份数量") or 0
        try:
            amount = float(str(amount_str).replace(",", ""))
            # 如果原始单位是元而非亿，做换算
            if amount > 1e6:
                amount = amount / 1e8
            unlock_schedule.append({"date": _month_label(date), "amount": round(amount, 2)})
        except (ValueError, TypeError):
            pass

    # 机构持仓 8 季度历史：改走单票接口，避免误用/扫描全市场大表。
    inst_history: dict = {"quarters": [], "fund": [], "qfii": [], "shehui": []}
    try:
        quarters = _last_n_quarters(8)
        inst_history["quarters"] = [label for _, label in quarters]

        for quarter_code, _quarter_label in quarters:
            fund_pct = qfii_pct = shehui_pct = 0.0
            try:
                df_inst = ak.stock_institute_hold_detail(stock=ti.code, quarter=quarter_code)
                if df_inst is not None and not df_inst.empty:
                    type_col = "持股机构类型" if "持股机构类型" in df_inst.columns else None
                    pct_col = "占流通股比例" if "占流通股比例" in df_inst.columns else None
                    if type_col and pct_col:
                        for _, row in df_inst.iterrows():
                            inst_type = str(row.get(type_col, "")).strip()
                            try:
                                pct = float(row.get(pct_col, 0) or 0)
                            except (TypeError, ValueError):
                                pct = 0.0
                            if "基金" in inst_type:
                                fund_pct += pct
                            elif "QFII" in inst_type:
                                qfii_pct += pct
                            elif "社保" in inst_type:
                                shehui_pct += pct
            except Exception:
                pass
            inst_history["fund"].append(round(fund_pct, 2))
            inst_history["qfii"].append(round(qfii_pct, 2))
            inst_history["shehui"].append(round(shehui_pct, 2))
    except Exception:
        pass

    # Build summary strings for viz
    def _north_sum_20d(hist):
        if not isinstance(hist, dict):
            return "—"
        flows = hist.get("flow_history", [])
        if not flows:
            return "—"
        try:
            total = sum(float(r.get("净买额") or r.get("净买入额") or 0) for r in flows[-20:])
            return f"{total / 1e8:+.1f}亿"
        except Exception:
            return "—"

    def _main_sum_20d(flow_list):
        if not flow_list:
            return "—"
        try:
            total = sum(float(r.get("主力净流入", 0) or 0) for r in flow_list[-20:])
            return f"{total / 1e4:+.1f}万" if abs(total) < 1e8 else f"{total / 1e8:+.1f}亿"
        except Exception:
            return "—"

    def _holders_trend(h):
        if not h or len(h) < 2:
            return "—"
        last = h[0]  # gdhs 接口通常最新在前
        prev = h[-1]
        try:
            l = float(str(last.get("股东户数", 0)).replace(",", ""))
            p = float(str(prev.get("股东户数", 0)).replace(",", ""))
            trend = "3 季连降" if l < p * 0.95 else "3 季连升" if l > p * 1.05 else "基本持平"
            return trend
        except Exception:
            return "—"

    return {
        "ticker": ti.full,
        "data": {
            "northbound": north,
            "northbound_20d": _north_sum_20d(north),
            "margin_recent": margin,
            "margin_trend": f"近 5 日 {len(margin)} 条记录" if margin else "—",
            "holder_count_history": holders,
            "holders_trend": _holders_trend(holders),
            "main_fund_flow_20d": main_flow,
            "main_20d": _main_sum_20d(main_flow),
            "main_5d": "—",
            "block_trades_recent": block_trades,
            "unlock_recent": unlock,
            "unlock_schedule": unlock_schedule,
            "institutional_history": inst_history,
        },
        "source": "akshare+eastmoney:multi (north + margin_summary + holdernum_filtered + fund_flow + restricted_release_queue + stock_institute_hold_detail)",
        "fallback": False,
    }


if __name__ == "__main__":
    print(json.dumps(main(sys.argv[1] if len(sys.argv) > 1 else "002273.SZ"), ensure_ascii=False, indent=2, default=str))
