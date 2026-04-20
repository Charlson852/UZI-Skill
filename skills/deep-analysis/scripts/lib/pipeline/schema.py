"""Pipeline 核心 schema · DimResult 是 fetcher → synthesis → renderer 的标准容器.

设计目标（从 v2.15.1 的 bug 教训来的）：
1. 每个 fetcher 返 DimResult · 不再是裸 dict · render 端不用猜字段位置
2. 空值 / 缺失 / 错误 三态显式（Quality enum）· 根绝"0 vs None vs '—'"风险
3. fetcher 声明自己的 required / optional 字段 · validator 自动检测 data_gaps
4. source 字段永远有值 · 追溯 token 用 · 方便回溯"这个数据哪来的"
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any


class Quality(str, Enum):
    """数据质量 · fetcher 返回时必须显式标注.

    - full: 所有 required fields 都有实测值
    - partial: required 齐但 optional 有缺失 / optional 齐 required 部分缺
    - missing: required 全空（fetcher 跑成功但没数据）
    - error: fetcher 抛异常 / 网络失败
    """
    FULL = "full"
    PARTIAL = "partial"
    MISSING = "missing"
    ERROR = "error"


@dataclass
class DimResult:
    """单个维度的抓取结果 · 所有 fetcher 统一返这个."""

    dim_key: str                          # 如 "0_basic" / "6_fund_holders"
    data: dict[str, Any] = field(default_factory=dict)
    source: str = "unknown"               # 如 "akshare:stock_individual_info_em"
    quality: Quality = Quality.MISSING
    error: str | None = None              # Quality.ERROR 时的 error message
    data_gaps: list[str] = field(default_factory=list)  # 缺失字段名列表

    # 元信息（可选）
    cached: bool = False                  # 是否走了 cache
    latency_ms: int | None = None         # 抓取耗时

    # v2.15.1 教训：有些 dim 需要额外 top-level 字段（如 fund_managers 放 raw 顶层）
    # 这些"溢出"字段放这里 · render 端按 dim 配置去拿
    top_level_fields: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """转 dict · 兼容老 raw_data.json 格式."""
        d = asdict(self)
        # Quality enum 转 str
        d["quality"] = self.quality.value if isinstance(self.quality, Quality) else str(self.quality)
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "DimResult":
        """从 dict 恢复 · 处理 Quality enum · 兼容老数据."""
        q = d.get("quality", "missing")
        if isinstance(q, str):
            try:
                q = Quality(q)
            except ValueError:
                q = Quality.MISSING
        return cls(
            dim_key=d.get("dim_key", ""),
            data=d.get("data") or {},
            source=d.get("source", "unknown"),
            quality=q,
            error=d.get("error"),
            data_gaps=d.get("data_gaps") or [],
            cached=d.get("cached", False),
            latency_ms=d.get("latency_ms"),
            top_level_fields=d.get("top_level_fields") or {},
        )

    # ─── 便捷构造 ─────────────────────────────────────
    @classmethod
    def empty(cls, dim_key: str, source: str = "unknown") -> "DimResult":
        """空结果（fetcher 跑了但没数据）."""
        return cls(dim_key=dim_key, source=source, quality=Quality.MISSING)

    @classmethod
    def error_result(cls, dim_key: str, error: str, source: str = "unknown") -> "DimResult":
        """错误结果（fetcher 抛异常 / 网络失败）."""
        return cls(dim_key=dim_key, source=source, quality=Quality.ERROR, error=error[:200])


@dataclass
class FetcherSpec:
    """Fetcher 元数据声明 · 用于 validator + collector."""

    dim_key: str
    required_fields: list[str] = field(default_factory=list)
    optional_fields: list[str] = field(default_factory=list)
    top_level_fields: list[str] = field(default_factory=list)  # 要写到 raw 顶层的字段（如 fund_managers）
    sources: list[str] = field(default_factory=list)  # 数据源 · 如 ["akshare", "mx", "ddgs"]
    markets: tuple[str, ...] = ("A", "H", "U")
    cache_ttl_sec: int = 3600
    depends_on: list[str] = field(default_factory=list)  # 依赖的其他 dim 先跑（如 7_industry 需要 0_basic.industry）

    def __post_init__(self):
        if not self.dim_key:
            raise ValueError("FetcherSpec.dim_key 不能为空")
