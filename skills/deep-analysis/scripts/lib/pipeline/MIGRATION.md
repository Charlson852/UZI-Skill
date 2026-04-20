# Pipeline Migration Guide · v3.0.0

> **状态**：Phase 1 完成（骨架 + fund renderer 示范）· 未推到 main · 分支 `refactor/v3.0.0-pipeline-architecture`
>
> **目的**：把 assemble_report.py (3100 行) + run_real_test.py (1800 行) + 22 个 fetch_*.py 逐步迁移到管道式架构 · 零回归 · 逐个 dim 推进

---

## 背景 · 为什么重构

v2.15.x 连续 5 个 hotfix 都落在同一区域（基金持仓 / 14_moat / 数据落地字段位置），根因是屎山：

1. `assemble_report.py` 单文件 3100 行 · render + data access + profile 逻辑混杂
2. `run_real_test.py` 1800 行 · stage1 + stage2 + collect + score + synthesize + autofill 全在一起
3. 22 个 fetcher 没有统一接口 · 每个返回的 dict 结构不同
4. 空值约定不一（`None` / `0` / `"—"` / `""` / error dict 混用）· render 端得猜
5. 数据落地位置不一 · `fund_managers` 既在 `raw.dimensions.6_fund_holders.data` 又在 `raw` 顶层（v2.15.1 bug 根因）

---

## 新架构总览

```
skills/deep-analysis/scripts/
├── lib/
│   ├── pipeline/                  # 🆕 v3.0.0 管道架构
│   │   ├── schema.py              # DimResult / FetcherSpec / Quality
│   │   ├── base_fetcher.py        # BaseFetcher ABC
│   │   ├── validators.py          # is_empty_value / normalize_data / validate_result
│   │   ├── MIGRATION.md           # 本文档
│   │   └── renderer/
│   │       ├── base.py            # SectionRenderer + RenderContext
│   │       └── fund.py            # ✅ Phase 1 已迁移（含 v2.15.1 + v2.15.2 修复）
│   └── ... (老代码不动)
├── fetch_basic.py ~ fetch_contests.py  # 22 fetcher 暂保留 · 逐步迁移
├── assemble_report.py              # 3100 行巨文件 · 逐个 section 抽出
├── run_real_test.py                # 1800 行 · stage1/stage2 暂保留
└── tests/
    └── pipeline/                   # 🆕 新架构测试
```

**coexistence 原则**：新老代码同时存在 · 老入口（`run.py`, `stage1()`, `stage2()`）保持工作 · 每次迁移一个 fetcher/renderer 测试不降级。

---

## 迁移一个 fetcher（标准流程）

以 `fetch_basic.py` 为例迁移到 `lib/pipeline/fetchers/basic.py`：

### Step 1 · 分析老 fetcher

```bash
# 看老代码字段
python3 -c "from fetch_basic import main; r = main('300470.SZ'); print(list(r.get('data',{}).keys()))"
# 输出字段清单
```

### Step 2 · 声明 FetcherSpec

```python
# lib/pipeline/fetchers/basic.py
from lib.pipeline import BaseFetcher
from lib.pipeline.schema import FetcherSpec

SPEC = FetcherSpec(
    dim_key="0_basic",
    required_fields=["name", "price", "market_cap", "pe_ttm"],
    optional_fields=["industry", "eps", "pb", "actual_controller", "listed_date"],
    top_level_fields=[],  # 0_basic 没有顶层溢出字段
    sources=["akshare:stock_individual_basic_info_xq", "akshare:stock_individual_spot_xq"],
    markets=("A",),
    cache_ttl_sec=900,  # 15 min（realtime 数据）
)
```

### Step 3 · 实现 `_fetch_raw`

```python
class BasicFetcher(BaseFetcher):
    spec = SPEC

    def _fetch_raw(self, ticker):
        # 直接调老 fetcher 的 main · 取 data 部分
        from fetch_basic import main as legacy_main
        result = legacy_main(ticker)
        return result.get("data", {})  # 框架会自动 normalize + validate
```

### Step 4 · 加测试

```python
# tests/pipeline/fetchers/test_basic.py
def test_basic_fetcher_returns_dim_result(monkeypatch):
    from lib.pipeline.fetchers.basic import BasicFetcher
    # mock 老 main · 避免真实网络
    monkeypatch.setattr("fetch_basic.main", lambda t: {
        "data": {"name": "中密控股", "price": 36.52, "market_cap": "76.3亿"}
    })
    r = BasicFetcher().fetch("300470.SZ")
    assert r.dim_key == "0_basic"
    assert r.quality.value in ("full", "partial")  # partial if optional 缺
    assert r.data["name"] == "中密控股"
```

### Step 5 · 切换老路径引用

在 `run_real_test.py::collect_raw_data` 里逐个换：

```python
# 旧
from fetch_basic import main
raw_data["0_basic"] = main(ticker)

# 新
from lib.pipeline.fetchers.basic import BasicFetcher
raw_data["0_basic"] = BasicFetcher().fetch(ticker).to_dict()
```

不动老 `fetch_basic.py` · 只在上层换引用 · 零风险。

---

## 迁移一个 renderer（标准流程）

以 `render_peer_comparison` 为例：

### Step 1 · 抽取函数到 `renderer/peers.py`

```python
# lib/pipeline/renderer/peers.py
from .base import SectionRenderer, RenderContext

class PeersRenderer(SectionRenderer):
    section_id = "peers"
    section_title = "同行对比"

    def render_full(self, ctx: RenderContext) -> str:
        peer_table = ctx.data.get("peer_table") or []
        if not peer_table:
            return self.render_gap(ctx, "无同行数据")
        # ... 把 assemble_report.render_peer_comparison 的 HTML 生成代码挪过来
```

### Step 2 · assemble_report.py 改调用

```python
# 旧
peer_html = render_peer_comparison(raw["dimensions"]["4_peers"]["data"])

# 新
from lib.pipeline.renderer.peers import PeersRenderer
peer_html = PeersRenderer().render(RenderContext(
    ticker=ti.full, name=basic.get("name"),
    data=raw["dimensions"]["4_peers"]["data"],
    quality=raw["dimensions"]["4_peers"].get("quality", "full"),
))
```

### Step 3 · 老函数可以标 `# deprecated` 不删除 · 下次清理 phase 再删

---

## 迁移优先级（推荐顺序）

**高价值高风险先做**（单 section · 受伤最多）：
1. ✅ `renderer/fund.py` · v2.15.1 bug 温床 · **已完成 Phase 1 示范**
2. `renderer/moat.py` · v2.15.1 Bug 2 · 直接含 `_SUPERSTAR_POLLUTERS` 逻辑
3. `renderer/peers.py` · 4_peers 历史 bug 多
4. `renderer/industry.py` · 7_industry "综合" 误判
5. `renderer/sentiment.py` · 17_sentiment

**基础设施**（低风险 · 建立规范）：
6. `fetchers/basic.py` · 最简 · 给其他 fetcher 做模板
7. `fetchers/financials.py`
8. `fetchers/fund_holders.py` · 新增 fund_code enrich 逻辑

**复杂但最后做**（动 stage1/stage2 骨架）：
9. `pipeline/collect.py` · wave 编排 + resume + preflight
10. `pipeline/score.py` · Rules 引擎 + 51 评委
11. `pipeline/synthesize.py` · stage2 合并 agent_analysis

**最终废弃**：
- `run_real_test.py` 除 cli 入口外全部迁移完 → 可删
- `assemble_report.py` 所有 section 迁完 → 只剩最外层 shell 组装

---

## 零回归原则

**每次 PR 必须**：
1. 老入口 `run.py <ticker>` 生成的报告跟之前视觉 / 数据一致
2. pytest 全量 passed（含老 tests + 新 pipeline tests）
3. 至少 1 个端到端实测（Playwright 核对报告）
4. 迁移的 function 保留老签名（至少第一轮） · 标 `@deprecated`

**禁止**：
- 一次 PR 动 2 个 dim 以上
- 跳过测试推 main
- 改数据 schema 不加 `DimResult.from_dict` 兼容

---

## 完成标准（Phase N · 未来某个 session）

当满足以下条件，`refactor/v3.0.0-pipeline-architecture` 可以合回 main：

- [ ] 22 个 fetcher 全部迁移到 `fetchers/*.py`
- [ ] 所有 section renderer 迁移到 `renderer/*.py`
- [ ] `run_real_test.py` 瘦身到 < 200 行（只保留 stage1/stage2 入口 · 内部全调 pipeline）
- [ ] `assemble_report.py` 瘦身到 < 400 行（只保留最外层 HTML shell）
- [ ] pytest 全量 passed · 测试覆盖率 > 70%
- [ ] 新增 1 个真机端到端：`tests/e2e/test_full_analysis.py` · 每个 PR 前跑

---

## FAQ

**Q: 为什么不用 pydantic 做 schema？**
A: 要零新依赖（已有 akshare/playwright/requests 够多了）· dataclass + enum 够用。如果后续需要 JSON Schema 生成再加。

**Q: 老 `fetch_*.py` 能不能内部直接改 BaseFetcher？**
A: 不推荐 · 迁移要可逆。当前流程：新文件继承 BaseFetcher · 内部复用老 fetcher 的 `main()` · 外层切换引用。稳定后再删老文件。

**Q: renderer/ 里要不要放 CSS？**
A: 暂不 · 保留 assemble_report.py 的 CSS 大字符串 · 后续 Phase 单独抽。

**Q: 我在未来 session 想继续迁移，怎么接上？**
A:
1. `git checkout refactor/v3.0.0-pipeline-architecture`
2. 读本文档 + 已有 `renderer/fund.py` 作示范
3. 按"迁移一个 fetcher/renderer"流程做 1-2 个 dim · 加测试 · commit
4. 永远不要 merge 到 main 直到全部完成

---

## 本次 Phase 1 交付

✅ `lib/pipeline/schema.py` (111 行) · DimResult + FetcherSpec + Quality
✅ `lib/pipeline/base_fetcher.py` (81 行) · BaseFetcher ABC
✅ `lib/pipeline/validators.py` (86 行) · is_empty_value + normalize + validate
✅ `lib/pipeline/renderer/base.py` (55 行) · SectionRenderer + RenderContext
✅ `lib/pipeline/renderer/fund.py` (220 行) · FundRenderer + FUND_CODE_TO_MANAGER
✅ `tests/pipeline/*` · 31 新 case
✅ 本文档

**pytest 全量 286 passed**（255 baseline + 31 新 · 零回归）

老入口未动：`run.py`, `stage1()`, `stage2()`, `assemble_report.py`, 22 个 `fetch_*.py` 全部保持工作。
