"""HK data source enhancements · v2.5.

Wraps akshare HK functions that were previously unused, plus a static-HTML
HKEXNews announcement scraper. Used by:

- `_fetch_basic_hk` in data_sources.py
- `fetch_peers.py` HK branch
- `fetch_capital_flow.py` HK branch
- `fetch_events.py` HK branch

Why a separate module:
1. akshare has 50+ HK functions but data_sources.py only used 2 (spot + hist).
   The unused ones (xq basic info, EM company profile, valuation/growth/scale
   comparison) cover most of the HK gap without scraping.
2. A few HK functions still hit blocked push2 endpoints (ggt_components,
   main_board_spot, hsgt_sh_hk_spot). We avoid those and use what works.
3. AASTOCKS itself is JS-rendered — we register it as Tier-2 Playwright in
   `data_source_registry.py` for agent use, but don't reverse-engineer its AJAX.

Each function returns either a populated dict/list or an empty container — never
raises. Errors go into `_<func>_err` keys. Caller decides if degradation is OK.
"""
from __future__ import annotations

import re
from typing import Any

from .cache import cached, TTL_DAILY, TTL_QUARTERLY, TTL_HOURLY

try:
    import akshare as ak
except ImportError:
    ak = None

try:
    import requests
except ImportError:
    requests = None


_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


def _safe_get(df, col, default=None):
    """First-row column getter that handles missing columns / empty df."""
    if df is None or df.empty or col not in df.columns:
        return default
    val = df.iloc[0][col]
    return val if val is not None and str(val) != "nan" else default


def fetch_hk_basic(code5: str) -> dict:
    """Combined basic info via XueQiu (akshare) + EM company profile + EM security profile.

    code5: 5-digit padded code, e.g. "00700"

    Returns a flat dict with as many fields as can be retrieved. Never raises.
    """
    out: dict[str, Any] = {"code5": code5}
    if ak is None:
        out["_err"] = "akshare not installed"
        return out

    # Source 1: XueQiu basic info (industry, intro, chairman, web)
    try:
        df = ak.stock_individual_basic_info_hk_xq(symbol=code5)
        if df is not None and not df.empty:
            info = dict(zip(df["item"], df["value"]))
            out.update({
                "name": info.get("comcnname") or info.get("comenname"),
                "full_name": info.get("comcnname"),
                "name_en": info.get("comenname"),
                "main_business": info.get("mbu"),
                "intro": info.get("comintr"),
                "chairman": info.get("chairman"),
                "office_address": info.get("hofclctmbu") or info.get("rgiofc"),
                "country": info.get("nation_name"),
                "website": info.get("web_site"),
                "telephone": info.get("tel"),
                "email": info.get("email"),
                "listed_date": info.get("lsdateipo"),
            })
    except Exception as e:
        out["_xq_err"] = f"{type(e).__name__}: {str(e)[:120]}"

    # Source 2: EM company profile (industry, founding date, employees, auditor)
    try:
        df = ak.stock_hk_company_profile_em(symbol=code5)
        if df is not None and not df.empty:
            row = df.iloc[0]
            out.update({
                "industry": str(row.get("所属行业") or "") or out.get("industry"),
                "incorporation_country": str(row.get("注册地") or ""),
                "incorporation_date": str(row.get("公司成立日期") or ""),
                "employees": str(row.get("员工人数") or ""),
                "auditor": str(row.get("核数师") or ""),
                "year_end": str(row.get("年结日") or ""),
            })
            # Override description with the longer EM version if present
            if row.get("公司介绍"):
                out["intro"] = str(row["公司介绍"])
    except Exception as e:
        out["_em_profile_err"] = f"{type(e).__name__}: {str(e)[:120]}"

    # Source 3: EM security profile (listing date, ISIN, southbound eligibility)
    try:
        df = ak.stock_hk_security_profile_em(symbol=code5)
        if df is not None and not df.empty:
            row = df.iloc[0]
            out.update({
                "isin": str(row.get("ISIN（国际证券识别编码）") or ""),
                "lot_size": str(row.get("每手股数") or ""),
                "issue_price": str(row.get("发行价") or ""),
                "is_south_bound_sh": str(row.get("是否沪港通标的") or "") == "是",
                "is_south_bound_sz": str(row.get("是否深港通标的") or "") == "是",
            })
    except Exception as e:
        out["_em_security_err"] = f"{type(e).__name__}: {str(e)[:120]}"

    return out


def fetch_hk_valuation_ranks(code5: str) -> dict:
    """Get the stock's valuation/growth/scale rank within all HK listed stocks.

    These rank fields are useful for the report's "Position in HK Universe"
    visualization — even without explicit peer list, knowing "PE ranks 37/N"
    is informative.
    """
    out: dict[str, Any] = {"code5": code5}
    if ak is None:
        return out

    # Valuation comparison (PE/PB/PS/PCF + ranks)
    try:
        df = ak.stock_hk_valuation_comparison_em(symbol=code5)
        if df is not None and not df.empty:
            row = df.iloc[0]
            out["valuation"] = {
                "pe_ttm": float(row.get("市盈率-TTM") or 0) or None,
                "pe_ttm_rank": int(row.get("市盈率-TTM排名") or 0) or None,
                "pb_mrq": float(row.get("市净率-MRQ") or 0) or None,
                "pb_mrq_rank": int(row.get("市净率-MRQ排名") or 0) or None,
                "ps_ttm": float(row.get("市销率-TTM") or 0) or None,
                "ps_ttm_rank": int(row.get("市销率-TTM排名") or 0) or None,
                "pcf_ttm": float(row.get("市现率-TTM") or 0) or None,
                "pcf_ttm_rank": int(row.get("市现率-TTM排名") or 0) or None,
            }
    except Exception as e:
        out["_val_err"] = f"{type(e).__name__}: {str(e)[:120]}"

    # Growth comparison (revenue / EPS growth + ranks)
    try:
        df = ak.stock_hk_growth_comparison_em(symbol=code5)
        if df is not None and not df.empty:
            row = df.iloc[0]
            out["growth"] = {
                "eps_yoy": float(row.get("基本每股收益同比增长率") or 0) or None,
                "eps_yoy_rank": int(row.get("基本每股收益同比增长率排名") or 0) or None,
                "revenue_yoy": float(row.get("营业收入同比增长率") or 0) or None,
                "revenue_yoy_rank": int(row.get("营业收入同比增长率排名") or 0) or None,
            }
    except Exception as e:
        out["_growth_err"] = f"{type(e).__name__}: {str(e)[:120]}"

    # Scale comparison (mcap, revenue, profit + ranks)
    try:
        df = ak.stock_hk_scale_comparison_em(symbol=code5)
        if df is not None and not df.empty:
            row = df.iloc[0]
            mcap_raw = float(row.get("总市值") or 0) or None
            rev_raw = float(row.get("营业总收入") or 0) or None
            profit_raw = float(row.get("净利润") or 0) or None
            out["scale"] = {
                "market_cap": mcap_raw,
                "market_cap_rank": int(row.get("总市值排名") or 0) or None,
                "revenue": rev_raw,
                "revenue_rank": int(row.get("营业总收入排名") or 0) or None,
                "net_profit": profit_raw,
                "net_profit_rank": int(row.get("净利润排名") or 0) or None,
            }
    except Exception as e:
        out["_scale_err"] = f"{type(e).__name__}: {str(e)[:120]}"

    return out


def _resolve_hkex_stock_id(code5: str) -> str | None:
    """Resolve exact security code using HKEX's public security directory.

    The `i` field is the title-search stockId; it is NOT the stock code.
    Never guess an ID when the directory cannot establish identity.
    """
    for listing in ("activestock", "inactivestock"):
        r = requests.get(
            f"https://www1.hkexnews.hk/ncms/script/eds/{listing}_sehk_c.json",
            timeout=15, headers={"User-Agent": _UA},
        )
        r.raise_for_status()
        matches = {str(row["i"]) for row in r.json()
                   if str(row.get("c", "")).zfill(5) == code5 and row.get("i") is not None}
        if len(matches) == 1:
            return matches.pop()
        if matches:
            return None
    return None


def _parse_hkex_announcements(html: str, code5: str, limit: int = 30) -> list[dict]:
    """Accept only announcement rows with an explicit matching security code."""
    from bs4 import BeautifulSoup
    from datetime import datetime
    from urllib.parse import urljoin, urlparse

    soup = BeautifulSoup(html, "html.parser")
    items, seen = [], set()
    for row in soup.select("tr"):
        code_cell = row.select_one("td.stock-short-code")
        name_cell = row.select_one("td.stock-short-name")
        date_cell = row.select_one("td.release-time")
        if code_cell is None or name_cell is None or date_cell is None:
            continue
        for cell in (code_cell, name_cell, date_cell):
            for label in cell.select(".mobile-list-heading"):
                label.decompose()
        codes = re.findall(r"(?<!\d)\d{5}(?!\d)", code_cell.get_text(" ", strip=True))
        if code5 not in codes:
            continue
        try:
            date = datetime.strptime(date_cell.get_text(" ", strip=True), "%d/%m/%Y %H:%M")
        except ValueError:
            continue
        for link in row.select(".doc-link a[href]"):
            url = urljoin("https://www1.hkexnews.hk", link["href"])
            parsed = urlparse(url)
            if (parsed.hostname != "www1.hkexnews.hk" or
                    not re.fullmatch(r"/listedco/listconews/sehk/\d{4}/\d{4}/[^/]+\.(?:pdf|htm|html)", parsed.path, re.I)):
                continue
            title = link.get_text(" ", strip=True)
            if not title or url in seen:
                continue
            seen.add(url)
            items.append({"date": date.isoformat(timespec="minutes"), "title": title,
                          "url": url, "source": "hkexnews", "stock_code": code5,
                          "issuer_name": name_cell.get_text(" ", strip=True),
                          "issuer_verified": True})
    return sorted(items, key=lambda item: item["date"], reverse=True)[:max(0, limit)]


def fetch_hk_announcements(code5: str, limit: int = 30) -> list[dict]:
    """Query by resolved HKEX ID and verify each returned row's security code."""
    if requests is None:
        return []
    if not re.fullmatch(r"\d{1,5}", str(code5)):
        return []
    code5 = str(code5).zfill(5)
    try:
        stock_id = _resolve_hkex_stock_id(code5)
        if stock_id is None:
            return []
        r = requests.get(
            "https://www1.hkexnews.hk/search/titlesearch.xhtml",
            params={"lang": "zh", "category": "0", "market": "SEHK", "stockId": stock_id},
            timeout=15, headers={"User-Agent": _UA},
        )
        r.raise_for_status()
        return _parse_hkex_announcements(r.text, code5, limit)
    except Exception:
        return []


def fetch_hk_basic_combined(code5: str) -> dict:
    """One-call helper · merges fetch_hk_basic + fetch_hk_valuation_ranks.

    Use from `_fetch_basic_hk` to enrich the akshare spot data with industry,
    PE/PB, market cap, ranks. All cached to TTL_QUARTERLY (24h) since these
    fields rarely change intra-day.
    """
    def _fetch():
        basic = fetch_hk_basic(code5)
        ranks = fetch_hk_valuation_ranks(code5)
        # Project a few useful top-level fields for compatibility with A-share schema
        val = ranks.get("valuation", {})
        scale = ranks.get("scale", {})
        if val.get("pe_ttm"):
            basic["pe_ttm"] = val["pe_ttm"]
        if val.get("pb_mrq"):
            basic["pb"] = val["pb_mrq"]
        if scale.get("market_cap"):
            basic["market_cap_raw"] = scale["market_cap"]
            basic["market_cap"] = f"{round(scale['market_cap'] / 1e8, 1)}亿"
        basic["_ranks"] = ranks
        return basic

    return cached(f"HK_{code5}", "hk_basic_combined", _fetch, ttl=TTL_QUARTERLY)


def fetch_hk_announcements_cached(code5: str, limit: int = 30) -> list[dict]:
    return cached(f"HK_{code5}", f"hk_anns_issuer_v2_{limit}", lambda: fetch_hk_announcements(code5, limit), ttl=TTL_HOURLY)


if __name__ == "__main__":
    import json
    import sys
    code = sys.argv[1] if len(sys.argv) > 1 else "00700"
    print(f"=== fetch_hk_basic_combined({code}) ===")
    print(json.dumps(fetch_hk_basic_combined(code), ensure_ascii=False, indent=2, default=str)[:2000])
    print(f"\n=== fetch_hk_announcements({code}) ===")
    anns = fetch_hk_announcements_cached(code, limit=10)
    for a in anns[:5]:
        print(f"  · {a['title'][:80]}")
    print(f"  total: {len(anns)} announcements")
