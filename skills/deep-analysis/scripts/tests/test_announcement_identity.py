"""Regression: 00700 is NOT HKEX stockId 700 (00362)."""
from unittest.mock import Mock, patch
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.hk_data_sources import _parse_hkex_announcements, fetch_hk_announcements
from lib.event_integrity import hk_event_data, sanitize_hk_events
from lib.stock_features import extract_features


def row(code, title, path='2026100201234_c.pdf', date='02/10/2026 19:02'):
    return f'''<tr><td class="release-time"><span class="mobile-list-heading">發放時間:</span>{date}</td>
    <td class="stock-short-code"><span class="mobile-list-heading">股份代號:</span>{code}</td>
    <td class="stock-short-name"><span class="mobile-list-heading">股份簡稱:</span>公司</td>
    <td><div class="doc-link"><a href="/listedco/listconews/sehk/2026/1002/{path}">{title}</a></div></td></tr>'''


def test_mixed_issuers_filtered_and_multi_currency_supported():
    html = ('<table>' + row('00362', '清盤及停牌')
            + row('00700<br/>80700', '回購 &amp; 月報表', '2026100204567_c.pdf') + '</table>')
    items = _parse_hkex_announcements(html, '00700')
    assert len(items) == 1
    assert items[0]['title'] == '回購 & 月報表'
    assert items[0]['stock_code'] == '00700'
    assert items[0]['date'] == '2026-10-02T19:02'
    assert items[0]['issuer_verified'] is True
    assert _parse_hkex_announcements(html, '00007') == []


def test_unscoped_links_bad_dates_and_placeholder_documents_rejected():
    html = '<a href="/listedco/listconews/sehk/2026/1002/2026100201234_c.pdf">清盤</a>'
    html += row('00700', '公告', date='not a date')
    html += row('00700', '公告', path='../../../dod/tc.html')
    assert _parse_hkex_announcements(html, '00700') == []


def test_id_resolved_from_official_directory_not_ticker():
    directory = Mock(); directory.json.return_value = [{'c': '00362', 'i': 700}, {'c': '00700', 'i': 7609}]
    page = Mock(); page.text = '<table>' + row('00700', '回購') + '</table>'
    with patch('lib.hk_data_sources.requests.get', side_effect=[directory, page]) as get:
        assert len(fetch_hk_announcements('700')) == 1
        assert get.call_args.kwargs['params']['stockId'] == '7609'


def test_wrong_response_despite_correct_id_fails_closed():
    directory = Mock(); directory.json.return_value = [{'c': '00700', 'i': 7609}]
    page = Mock(); page.text = '<table>' + row('00362', '停牌清盤') + '</table>'
    with patch('lib.hk_data_sources.requests.get', side_effect=[directory, page]):
        assert fetch_hk_announcements('00700') == []


def test_missing_mapping_never_queries_guessed_stock_id():
    directory = Mock(); directory.json.return_value = []
    with patch('lib.hk_data_sources.requests.get', return_value=directory) as get:
        assert fetch_hk_announcements('00700') == []
        assert all('titlesearch' not in call.args[0] for call in get.call_args_list)


def test_legacy_contamination_removed_before_features_and_scoring():
    raw = {'ticker': '00700.HK', 'dimensions': {'15_events': {'data': {
        'event_timeline': ['其他公司诉讼风险'], 'recent_news': [{'title': '清盤停牌'}],
        'warnings': ['清盤'], 'catalyst': [{'event': '其他公司合作'}],
    }}}}
    features = extract_features(raw, {})
    assert features['recent_events_count'] == 0
    assert features['has_negative_catalyst'] is False
    assert raw['dimensions']['15_events']['data']['warnings'] == []
    assert sanitize_hk_events(raw) is False


def test_verified_snapshot_is_stable_and_explicit_mismatch_removed():
    notice = {'stock_code': '00700', 'issuer_verified': True, 'title': '回購', 'date': '2026-10-02'}
    raw = {'ticker': '00700.HK', 'dimensions': {'15_events': {'data': hk_event_data('00700', [notice], [])}}}
    assert sanitize_hk_events(raw) is False
    raw['dimensions']['15_events']['data']['recent_news'] = [dict(notice, stock_code='00362', title='清盤')]
    assert sanitize_hk_events(raw) is True
    assert raw['dimensions']['15_events']['data']['recent_news'] == []
    assert raw['dimensions']['15_events']['data']['recent_notices'] == [notice]


def test_cninfo_requires_matching_code():
    from fetch_events import _cninfo_direct_api
    response = Mock(status_code=200)
    response.json.return_value = {'announcements': [
        {'secCode': '600519', 'announcementTitle': '其他公司停牌'},
        {'secCode': '688981', 'announcementTitle': '中芯国际业绩'},
        {'announcementTitle': '未知归属清盘'},
    ]}
    with patch('requests.post', return_value=response):
        rows = _cninfo_direct_api('688981')
    assert [r['title'] for r in rows] == ['中芯国际业绩']


def test_web_query_is_not_proof_of_company_identity():
    from fetch_events import _web_search_events
    with patch('fetch_events.search_trusted', return_value=[
        {'title': '其他公司清盘', 'url': 'https://example.com/wrong'},
        {'title': '腾讯控股回购公告', 'url': 'https://example.com/right'},
    ]), patch('fetch_events.web_search', return_value=[]):
        rows = _web_search_events('腾讯控股')
    assert len(rows) == 1
    assert rows[0]['title'] == '腾讯控股回购公告'


def test_derived_timeline_cannot_bypass_verified_rows():
    raw = {'ticker': '00700.HK', 'dimensions': {'15_events': {'data': hk_event_data('00700', [], [])}}}
    raw['dimensions']['15_events']['data']['event_timeline'] = ['其他公司诉讼风险']
    assert sanitize_hk_events(raw) is True
    assert extract_features(raw, {})['has_negative_catalyst'] is False


def test_old_agent_votes_invalidated_after_evidence_changes(tmp_path):
    import json
    from lib.agent_review import analysis_input_hash, load_fresh_agent_analysis
    raw = {'ticker': '00700.HK', 'dimensions': {'15_events': {'data': {'event_timeline': ['其他公司清盤']}}}}
    (tmp_path / 'agent_analysis.json').write_text(json.dumps({'agent_reviewed': True, 'analysis_input_hash': analysis_input_hash(raw)}))
    sanitize_hk_events(raw)
    assert load_fresh_agent_analysis(tmp_path, raw)[0] is None


def test_stage2_blocks_legacy_scores_and_panel(monkeypatch):
    import pytest
    import run_real_test as rrt
    from types import SimpleNamespace
    monkeypatch.setattr(rrt, '_resolve_cached_target', lambda *a, **k: SimpleNamespace(full='00700.HK'))
    raw = {'ticker': '00700.HK', 'dimensions': {'15_events': {'data': {'event_timeline': ['清盤']}}}}
    monkeypatch.setattr('lib.cache.read_task_output', lambda ticker, task: raw if task == 'raw_data' else {'old': True})
    with pytest.raises(RuntimeError, match='旧评分与投票不可复用'):
        rrt.stage2('00700.HK')
