"""Explicit abstention reasons, separate from directional signals."""
from collections import Counter

STATUS_LABELS = {
    'market_not_applicable': '市场不适用', 'method_not_applicable': '方法不适用',
    'strategy_out_of_scope': '策略射程外', 'conditions_not_met': '交易条件未满足',
    'missing_model': '模型未验证', 'missing_data': '数据缺失', 'unverified_data': '证据未核验',
    'school_locked': '流派锁定', 'unspecified': '原因未分类',
}


def summarize_abstentions(investors):
    return dict(Counter(m.get('abstention_category') or 'unspecified'
                        for m in investors if m.get('signal') == 'skip'))


def status_label(inv):
    category = inv.get('abstention_category')
    return STATUS_LABELS.get(category, '跳过（原因未分类）')
