import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from datetime import datetime
from unittest.mock import patch
import pandas as pd
from lib import data_sources as ds
from lib.market_router import parse_ticker
from lib.stock_features import extract_features
from lib.investor_evaluator import evaluate
from lib.report.panel_cards import render_chat_message


def test_lhb_error_is_not_verified_zero():
    with patch.object(ds.ak, 'stock_lhb_stock_detail_date_em', side_effect=RuntimeError('offline')):
        result = ds._fetch_lhb_evidence_impl(parse_ticker('688981.SH'), 30)
    assert result['status'] == 'error' and result['verified'] is False
    assert result['errors']


def test_valid_empty_date_schema_confirms_absence():
    with patch.object(ds.ak, 'stock_lhb_stock_detail_date_em', return_value=pd.DataFrame({'交易日': []})):
        result = ds._fetch_lhb_evidence_impl(parse_ticker('688981.SH'), 30)
    assert result['status'] == 'no_records' and result['verified'] is True


def test_partial_seats_do_not_confirm_absence():
    day = datetime.now().strftime('%Y-%m-%d')
    with patch.object(ds.ak, 'stock_lhb_stock_detail_date_em', return_value=pd.DataFrame({'交易日': [day]})), patch.object(ds.ak, 'stock_lhb_stock_detail_em', return_value=None):
        result = ds._fetch_lhb_evidence_impl(parse_ticker('688981.SH'), 30)
    assert result['verified'] is False and result['failed_dates']


def test_old_unknown_lhb_cannot_override_scope():
    raw = {'ticker': '688981.SH', 'dimensions': {'16_lhb': {'data': {'lhb_count_30d': 0, 'matched_youzi': ['赵老哥'], 'inst_vs_youzi': {'institutional_net': 0}}}}}
    f = extract_features(raw, {})
    assert f['lhb_30d_count'] is None and f['inst_net_buy_lhb'] is None
    assert f['matched_youzi'] == []


def test_market_abstention_has_explicit_category(monkeypatch):
    monkeypatch.delenv('UZI_SCHOOL', raising=False)
    result = evaluate('zhao_lg', {'market': 'H', 'ticker': '00981.HK'})
    assert result['signal'] == 'skip'
    assert result['abstention_category'] == 'market_not_applicable'


def test_skipped_judge_renders_category_without_zero_score():
    html = render_chat_message({'investor_id': 'zhao_lg', 'signal': 'skip', 'score': 0, 'abstention_category': 'market_not_applicable'})
    assert '市场不适用' in html and '>0分<' not in html


def test_unverified_dimension_has_no_weight():
    from lib.pipeline.score_fns import score_dimensions
    raw = {'ticker': '688981.SH', 'dimensions': {'16_lhb': {'data': {'lhb_count_30d': None, 'status': 'error'}}}}
    dim = score_dimensions(raw)['dimensions']['16_lhb']
    assert dim['weight'] == 0 and '未核验' in dim['label']


def test_trend_failure_is_not_capacity_mismatch():
    from lib.investor_evaluator import _is_youzi_out_of_range
    out, reason = _is_youzi_out_of_range('zhang_mz', {'market_cap_yi': 9000, 'trend': 'down'})
    assert out is False


def test_universe_absence_requires_all_pages(monkeypatch):
    from lib.lhb_evidence import fetch_board_universe
    import lib.lhb_evidence as module
    class Response:
        def raise_for_status(self): pass
        def json(self): return {'success': True, 'result': {'pages': 2, 'count': 2, 'data': [{'SECURITY_CODE': '000001', 'TRADE_DATE': '2026-09-30'}]}}
    calls = []
    monkeypatch.setattr(module.requests, 'get', lambda *args, **kwargs: calls.append(kwargs['params']['pageNumber']) or Response())
    r = fetch_board_universe('688981')
    assert r['verified'] and r['status'] == 'no_records' and calls == [1, 2]


def test_universe_incomplete_cannot_claim_zero(monkeypatch):
    from lib.lhb_evidence import fetch_board_universe
    import lib.lhb_evidence as module
    class Response:
        def raise_for_status(self): pass
        def json(self): return {'success': True, 'result': {'pages': 1, 'count': 2, 'data': [{'SECURITY_CODE': '000001', 'TRADE_DATE': '2026-09-30'}]}}
    monkeypatch.setattr(module.requests, 'get', lambda *args, **kwargs: Response())
    assert fetch_board_universe('688981')['verified'] is False


def test_board_count_is_dates_not_seat_rows(monkeypatch):
    import fetch_lhb
    evidence = {'verified': True, 'status': 'ok', 'board_dates': ['20260930'],
                'records': [{'营业部名称': '机构专用', '买入金额': 1} for _ in range(10)]}
    monkeypatch.setattr(fetch_lhb.ds, 'fetch_lhb_evidence', lambda *a, **k: evidence)
    monkeypatch.setattr(fetch_lhb.ds, 'fetch_basic', lambda *a: {})
    data = fetch_lhb.main('688981.SH')['data']
    assert data['lhb_count_30d'] == 1 and data['seat_record_count'] == 10


def test_failed_query_main_keeps_unknown_net_amounts(monkeypatch):
    import fetch_lhb
    import lib.lhb_evidence as fallback
    evidence = {'verified': False, 'status': 'error', 'board_dates': [], 'records': []}
    monkeypatch.setattr(fetch_lhb.ds, 'fetch_lhb_evidence', lambda *a, **k: evidence)
    monkeypatch.setattr(fetch_lhb.ds, 'fetch_basic', lambda *a: {})
    monkeypatch.setattr(fallback, 'fetch_board_universe', lambda *a: {'verified': False})
    data = fetch_lhb.main('688981.SH')['data']
    assert data['lhb_count_30d'] is None
    assert data['inst_vs_youzi']['institutional_net'] is None
