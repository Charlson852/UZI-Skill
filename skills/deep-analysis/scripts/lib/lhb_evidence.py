"""Verify absence against the complete market-wide billboard response."""
from datetime import datetime, timedelta
import requests


def fetch_board_universe(code, days=30):
    end = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    url = 'https://datacenter-web.eastmoney.com/api/data/v1/get'
    params = {'reportName': 'RPT_DAILYBILLBOARD_DETAILSNEW', 'columns': 'ALL',
              'filter': f"(TRADE_DATE>='{start}')(TRADE_DATE<='{end}')",
              'pageSize': 500, 'pageNumber': 1, 'source': 'WEB', 'client': 'WEB',
              'sortColumns': 'TRADE_DATE', 'sortTypes': '-1'}
    result = {'verified': False, 'status': 'error', 'source': url,
              'window': [start, end], 'records': [], 'errors': []}
    try:
        rows = []
        page = 1
        expected_count = None
        while True:
            params['pageNumber'] = page
            response = requests.get(url, params=params, timeout=20)
            response.raise_for_status()
            payload = response.json()
            data = payload.get('result') or {}
            batch = data.get('data')
            pages = data.get('pages')
            if payload.get('success') is not True or not isinstance(batch, list) or not isinstance(pages, int) or not 1 <= pages <= 10:
                raise ValueError('invalid/empty universe response')
            if page == 1:
                expected_pages, expected_count = pages, data.get('count')
            elif pages != expected_pages or data.get('count') != expected_count:
                raise ValueError('universe changed during pagination')
            if any('SECURITY_CODE' not in row or 'TRADE_DATE' not in row for row in batch):
                raise ValueError('universe schema missing stock/date')
            rows.extend(batch)
            if page >= pages:
                break
            page += 1
        if expected_count is not None and len(rows) != expected_count:
            raise ValueError('incomplete universe pagination')
        matching = [row for row in rows if str(row['SECURITY_CODE']).zfill(6) == str(code).zfill(6)]
        result.update(records=matching, universe_record_count=len(rows), pages=pages,
                      status='board_records' if matching else 'no_records', verified=True,
                      fetched_at=datetime.now().isoformat())
    except (requests.RequestException, ValueError, TypeError) as exc:
        result['errors'].append(f'{type(exc).__name__}: {exc}')
    return result
