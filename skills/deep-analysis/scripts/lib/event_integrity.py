"""Identity contract for HK events, including legacy/resumed snapshots."""
from __future__ import annotations

EVENT_IDENTITY_VERSION = 2


def hk_event_data(code: str, notices: list[dict], news: list[dict]) -> dict:
    rows = sorted(notices + news, key=lambda row: row.get('date') or '', reverse=True)
    return {
        '_issuer_identity_version': EVENT_IDENTITY_VERSION,
        'event_timeline': [f"{row.get('date') or '—'} · {row.get('title', '')}" for row in rows[:30]],
        'recent_notices': notices, 'recent_news': news,
        'disclosures_count': len(notices), 'news_count': len(news),
        # These aggregates must be re-extracted from verified evidence by the analyst.
        'catalyst': [], 'catalysts': [], 'warnings': [],
        '_note': '仅包含公司归属已核验的公告及新闻；缺失公告不代表没有风险。',
    }


def sanitize_hk_events(raw: dict) -> bool:
    """Remove unverifiable events before scoring/feature extraction. Mutates raw.

    Old title-only timelines cannot establish issuer identity, even if fetched
    under a correctly named cache directory. Discard derived aggregates too.
    """
    from lib.market_router import parse_ticker
    try:
        ti = parse_ticker(raw.get('full') or raw.get('ticker') or '')
    except Exception:
        return False
    if ti.market != 'H':
        return False
    dim = (raw.get('dimensions') or {}).get('15_events')
    if not isinstance(dim, dict) or not isinstance(dim.get('data'), dict):
        return False
    data = dim['data']
    code = ti.code.zfill(5)
    def verified(row):
        return (isinstance(row, dict) and row.get('issuer_verified') is True
                and str(row.get('stock_code', '')).zfill(5) == code)
    notices = data.get('recent_notices') or []
    news = data.get('recent_news') or []
    current = data.get('_issuer_identity_version') == EVENT_IDENTITY_VERSION
    expected_timeline = hk_event_data(code, notices, news)['event_timeline'] if all(isinstance(row, dict) for row in notices + news) else []
    if (current and all(verified(row) for row in notices + news)
            and data.get('event_timeline') == expected_timeline):
        return False
    # Versionless data has no trustworthy derived timeline/catalysts. Retain
    # only rows that independently carry explicit verification metadata.
    kept_notices = [row for row in notices if verified(row)]
    kept_news = [row for row in news if verified(row)]
    dim['data'] = hk_event_data(code, kept_notices, kept_news)
    dim['data']['_identity_rejected_count'] = len(notices) + len(news) - len(kept_notices) - len(kept_news)
    dim['fallback'] = not (kept_notices or kept_news)
    dim.setdefault('_pipeline', {}).update({'quality': 'partial' if kept_notices or kept_news else 'missing'})
    return True
