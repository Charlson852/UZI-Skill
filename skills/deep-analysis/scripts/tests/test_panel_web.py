from pathlib import Path
import json
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pytest
from render_panel_web import render_page, load_panel, safe_url


def market(ticker='688981.SH', name='中芯国际'):
    return {'ticker': ticker, 'panel': {'name': name, 'investors': [
        {'name': '测试席位', 'signal': 'skip', 'group': 'F', 'score': None,
         'abstention_category': 'market_not_applicable', 'reasoning': '<script>alert(1)</script>',
         'sources': ['bad', 'good']}], 'signal_distribution': {'skip': 1}},
        'raw': {}, 'snapshot': {'sources': {'bad': 'javascript:alert(1)', 'good': 'https://example.com/report'}}}


def test_html_contains_all_markets_and_safe_sources():
    page = render_page([market(), market('00981.HK')])
    assert page.count('<article class="reviewer') == 2
    assert '全部市场' in page and '00981.HK' in page
    assert '<script>alert(1)</script>' not in page
    assert 'javascript:alert(1)' not in page
    assert 'https://example.com/report' in page
    assert '市场不适用' in page and 'None' not in page


def test_inconsistent_counts_are_rejected():
    data = market(); data['panel']['signal_distribution'] = {'skip': 2}
    with pytest.raises(ValueError, match='votes disagree'):
        render_page([data])


def test_other_issuer_is_not_combined():
    with pytest.raises(ValueError, match='same issuer'):
        render_page([market(), market('00700.HK', '腾讯')])


def test_cache_directory_traversal_is_rejected(tmp_path):
    with pytest.raises(ValueError, match='single cache'):
        load_panel(tmp_path, '../secret')


def test_stale_review_is_not_published(tmp_path):
    folder = tmp_path / '688981.SH'; folder.mkdir()
    (folder / 'panel.json').write_text(json.dumps({'agent_reviewed': True}))
    (folder / 'raw_data.json').write_text('{}')
    with pytest.raises(ValueError, match='missing analyst review'):
        load_panel(tmp_path, '688981.SH')


def test_credential_url_is_not_allowed():
    assert safe_url('https://username:password@example.com/') is None
    assert safe_url('file:///private/report.html') is None
