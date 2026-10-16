"""Export one issuer's market panels as a portable, searchable web page."""
from __future__ import annotations
import argparse
from collections import Counter
from datetime import datetime, timezone
from html import escape
import json
from pathlib import Path
import secrets
from urllib.parse import urlsplit

from lib.agent_review import load_fresh_agent_analysis
from lib.panel_status import STATUS_LABELS, status_label

SIGNALS = {'bullish': '看多', 'neutral': '中性', 'bearish': '看空', 'skip': '弃权'}
GROUPS = {'A': '价值', 'B': '成长', 'C': '宏观', 'D': '技术', 'E': '中国价投', 'F': '游资', 'G': '量化', 'H': '科技', 'I': '卡位'}


def e(value):
    return escape(str(value if value is not None else '—'), quote=True)


def safe_url(value):
    if not isinstance(value, str):
        return None
    try:
        parts = urlsplit(value)
    except ValueError:
        return None
    return value if parts.scheme in ('http', 'https') and parts.netloc and not parts.username and not parts.password else None


def load_panel(cache_root, ticker):
    if not ticker or Path(ticker).name != ticker or ticker in ('.', '..'):
        raise ValueError('ticker must be a single cache directory name')
    folder = (cache_root / ticker).resolve()
    if not folder.is_relative_to(cache_root.resolve()):
        raise ValueError('ticker resolves outside cache root')
    panel = json.loads((folder / 'panel.json').read_text(encoding='utf-8'))
    raw = json.loads((folder / 'raw_data.json').read_text(encoding='utf-8'))
    if panel.get('agent_reviewed'):
        analysis, reason = load_fresh_agent_analysis(folder, raw)
        if analysis is None:
            raise ValueError(f'{ticker}: stale or missing analyst review: {reason}')
    snapshot_path = folder / 'verified_panel_snapshot.json'
    snapshot = json.loads(snapshot_path.read_text(encoding='utf-8')) if snapshot_path.exists() else {}
    return {'ticker': ticker, 'panel': panel, 'raw': raw, 'snapshot': snapshot}


def render_page(markets, title='全部市场评委投票'):
    if not markets:
        raise ValueError('at least one market panel is required')
    names = {m['panel'].get('name') or
             ((m.get('raw', {}).get('dimensions', {}).get('0_basic') or {}).get('data') or {}).get('name')
             for m in markets}
    if len(markets) > 1 and (None in names or '' in names):
        raise ValueError('all market panels need an identity to verify the same issuer')
    names.discard(None)
    names.discard('')
    if len(names) > 1:
        raise ValueError('all market panels must belong to the same issuer')
    total = sum(len(m['panel']['investors']) for m in markets)
    issuer = next(iter(names), '股票研究')
    options = ''.join(f'<option value="{e(m["ticker"])}">{e(m["ticker"])}</option>' for m in markets)
    summaries, cards = [], []
    for market in markets:
        ticker, panel = market['ticker'], market['panel']
        investors = panel['investors']
        votes = Counter(m.get('signal') for m in investors)
        actual = {k: votes[k] for k in SIGNALS}
        long_votes = Counter(m.get('signal') for m in investors if m.get('mandate') != 'short')
        declared = panel.get('signal_distribution')
        if declared is not None and any(declared.get(k, 0) != long_votes[k] for k in SIGNALS):
            raise ValueError(f'{ticker}: declared votes disagree with investor rows')
        price = panel.get('price_basis') or market['snapshot'].get('markets', {}).get(ticker) or {}
        review = '分析师已复核' if panel.get('agent_reviewed') else '规则基线 · 尚未由分析师复核'
        vote_html = ''.join(f'<div class="metric {key}"><strong>{count}</strong><span>{SIGNALS[key]}</span></div>' for key, count in actual.items())
        categories = Counter(m.get('abstention_category') or 'unspecified' for m in investors if m.get('signal') == 'skip')
        labels = ' · '.join(f'{STATUS_LABELS.get(k, "原因未分类")} {v}' for k, v in categories.items())
        price_text = ' · '.join(f'{k}: {price[k]}' for k in ('price_date', 'date', 'price', 'currency') if k in price)
        summaries.append(f'<section class="summary" data-market="{e(ticker)}"><div class="summary-title"><h2>{e(ticker)}</h2><span>{e(review)}</span></div><p>{e(price_text)}</p><div class="metrics">{vote_html}</div><p class="categories">弃权原因：{e(labels)}</p></section>')
        sources = market['snapshot'].get('sources') or {}
        for inv in investors:
            sig = inv.get('signal', 'neutral')
            group = inv.get('group', '')
            status = status_label(inv) if sig == 'skip' else SIGNALS.get(sig, sig)
            score = '—' if sig == 'skip' else inv.get('score', '—')
            source_links = []
            for reference in inv.get('sources') or []:
                url = safe_url(sources.get(reference) or reference)
                if url:
                    source_links.append(f'<a target="_blank" rel="noopener noreferrer" href="{e(url)}">{e(reference)}</a>')
            requirements = inv.get('required_evidence') or []
            gaps = '<p class="gap"><b>仍需证据：</b>' + e('；'.join(requirements)) + '</p>' if requirements else ''
            cards.append(f'''<article class="reviewer {e(sig)}" data-market="{e(ticker)}" data-school="{e(group)}" data-signal="{e(sig)}">
<div class="card-top"><div><span class="market">{e(ticker)}</span><h3>{e(inv.get('name'))}</h3><span class="school">{e(GROUPS.get(group, group))}</span></div><div class="rating"><span>{e(status)}</span><strong>{e(score)}</strong></div></div>
<h4>{e(inv.get('headline'))}</h4><p class="reason">{e(inv.get('reasoning') or inv.get('rationale'))}</p>{gaps}
<details><summary>查看模拟评论与来源</summary><p>{e(inv.get('comment'))}</p><p class="sources">{' · '.join(source_links) or '本页未提供可点击外部来源；请结合证据缺口阅读。'}</p><p class="confidence">置信度：{e(inv.get('confidence'))} · {e(inv.get('period'))}</p></details></article>''')
    return '''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex,nofollow"><title>'''+e(issuer)+' · '+e(title)+'''</title><style>
:root{--ink:#14213a;--muted:#657187;--line:#dce4ed;--bg:#f3f6fa}*{box-sizing:border-box;overflow-wrap:anywhere}body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.65 system-ui,-apple-system,"PingFang SC",sans-serif}header{background:#13223b;color:#fff;padding:38px max(24px,calc((100vw - 1160px)/2));border-bottom:4px solid #41b9ac}header .eyebrow{font-size:12px;letter-spacing:0;color:#8bd5cc}h1{font-size:32px;line-height:1.25;margin:12px 0}header p{max-width:850px;color:#c1cddd}main{max-width:1208px;margin:auto;padding:24px}.summaries{display:grid;grid-template-columns:repeat(auto-fit,minmax(290px,1fr));gap:18px}.summary,.reviewer{background:#fff;border:1px solid var(--line);border-radius:8px;padding:24px}.summary-title{display:flex;align-items:center;justify-content:space-between;gap:10px}.summary-title h2{margin:0;font-size:22px}.summary-title span,.summary p{font-size:12px;color:var(--muted)}.metrics{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}.metric{display:flex;flex-direction:column}.metric strong{font-size:30px}.metric span{color:var(--muted);font-size:13px}.bullish .rating,.metric.bullish strong{color:#14836d}.bearish .rating,.metric.bearish strong{color:#bd4b54}.skip .rating,.metric.skip strong{color:#778195}.filters{display:flex;flex-wrap:wrap;gap:10px;position:sticky;top:0;z-index:1;background:rgba(243,246,250,.97);padding:20px 0}label{font-size:12px;color:var(--muted)}select,input{display:block;padding:10px 12px;border:1px solid #bfcddd;border-radius:8px;background:#fff;color:var(--ink);font:inherit}input{min-width:230px}.count{margin:auto 0 auto auto;font-size:13px;color:var(--muted)}.reviewers{display:grid;grid-template-columns:repeat(auto-fit,minmax(310px,1fr));gap:16px}.card-top{display:flex;justify-content:space-between;gap:12px}.market{font-size:11px;color:var(--muted)}h3{font-size:22px;line-height:1.3;margin:4px 0}.school{font-size:12px;color:var(--muted)}.rating{display:flex;flex-direction:column;align-items:flex-end;text-align:right;font-size:12px}.rating strong{font-size:25px}h4{font-size:15px;line-height:1.6;margin:18px 0 8px}.reason{white-space:pre-line;color:#33445c}.gap{padding:10px 12px;background:#fff7e5;border-radius:8px;font-size:13px}details{border-top:1px solid var(--line);padding-top:12px;margin-top:16px;font-size:13px;color:var(--muted)}summary{cursor:pointer;color:#245a8f}a{color:#245a8f;overflow-wrap:anywhere}footer{padding:28px 0;color:var(--muted);font-size:12px}.empty{text-align:center;padding:40px}[hidden]{display:none!important}@media(max-width:500px){main{padding:14px}.summary,.reviewer{padding:18px}.filters{position:static}.count{width:100%;margin:0}.reviewers{grid-template-columns:1fr}input{width:100%;min-width:0}.search-label{flex:1 0 100%;min-width:0}h1{font-size:26px}}
</style></head><body><header><div class="eyebrow">股票研究助手 · 评委观察</div><h1>'''+e(issuer)+' · '+e(title)+'''</h1><p>各上市市场分别评估。评委观点均为方法论模拟，不是投资者本人意见。弃权不计入有效票，也不等于看空；补齐证据不保证所有席位都适用。</p></header><main><div class="summaries">'''+''.join(summaries)+'''</div><div class="filters"><label>市场<select id="market"><option value="">全部市场</option>'''+options+'''</select></label><label>流派<select id="school"><option value="">全部流派</option>'''+''.join(f'<option value="{g}">{e(label)}</option>' for g,label in GROUPS.items())+'''</select></label><label>判断<select id="signal"><option value="">全部判断</option>'''+''.join(f'<option value="{s}">{e(label)}</option>' for s,label in SIGNALS.items())+'''</select></label><label class="search-label">搜索评委或理由<input id="search" type="search" placeholder="姓名、证据、交易条件…"></label><output id="count" class="count" aria-live="polite">'''+str(total)+''' / '''+str(total)+''' 席</output></div><div class="reviewers">'''+''.join(cards)+'''</div><p id="empty" class="empty" hidden>没有符合筛选条件的席位。</p><footer>这是评委投票网页，并非完整估值报告。行情与财务口径以各席来源为准，新增证据日期可能不同于原行情基准。页面生成：'''+e(datetime.now(timezone.utc).isoformat(timespec='seconds'))+'''</footer></main><script>
const controls=['market','school','signal','search'].map(id=>document.getElementById(id));
const cards=[...document.querySelectorAll('.reviewer')];
function filter(){const [market,school,signal,search]=controls.map(x=>x.value.trim().toLowerCase());let count=0;for(const card of cards){const yes=(!market||card.dataset.market.toLowerCase()===market)&&(!school||card.dataset.school.toLowerCase()===school)&&(!signal||card.dataset.signal===signal)&&(!search||card.textContent.toLowerCase().includes(search));card.hidden=!yes;if(yes)count++;}for(const box of document.querySelectorAll('.summary'))box.hidden=!!market&&box.dataset.market.toLowerCase()!==market;document.getElementById('count').textContent=count+' / '+cards.length+' 席';document.getElementById('empty').hidden=count>0;}
for(const control of controls)control.addEventListener('input',filter);
</script></body></html>'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tickers', required=True, help='Comma-separated cache tickers for one issuer')
    parser.add_argument('--cache-root', type=Path, default=Path(__file__).parent / '.cache')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--publish-dir', type=Path)
    parser.add_argument('--base-url')
    args = parser.parse_args()
    if bool(args.publish_dir) != bool(args.base_url):
        parser.error('--publish-dir and --base-url must be provided together')
    if args.base_url and not safe_url(args.base_url):
        parser.error('--base-url must be an http(s) URL without credentials')
    tickers = list(dict.fromkeys(x.strip() for x in args.tickers.split(',') if x.strip()))
    markets = [load_panel(args.cache_root, t) for t in tickers]
    content = render_page(markets)
    output = args.output or Path(__file__).parent / 'reports' / ('panel-' + '-'.join(tickers)) / 'index.html'
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(content, encoding='utf-8')
    result = {'path': str(output.resolve()), 'tickers': tickers}
    if args.publish_dir:
        token = secrets.token_hex(16)
        destination = args.publish_dir / token
        destination.mkdir(parents=True)
        destination.chmod(0o755)
        target = destination / 'index.html'
        target.write_text(content, encoding='utf-8')
        target.chmod(0o644)
        result['url'] = args.base_url.rstrip('/') + '/' + token + '/index.html'
        manifest = output.with_suffix('.published.json')
        manifest.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
