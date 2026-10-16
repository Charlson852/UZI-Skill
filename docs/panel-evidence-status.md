# 评委弃权与证据状态

`signal=skip` 表示本次弃权，不等于看空或0分。生成评委结果时同时写入
`abstention_category`、`skip_reason` 和尚缺的 `required_evidence`。

| category | 含义 |
|---|---|
| market_not_applicable | 模型不覆盖该市场 |
| method_not_applicable | 本轮方法不适用该业务 |
| strategy_out_of_scope | 已核验的策略容量不适配 |
| missing_data | 尚缺可采集数据 |
| unverified_data | 材料已有但不能核验 |
| missing_model | 尚未定义或验证模型、概率及回测 |
| school_locked | 用户锁定其他流派 |

已知交易条件不满足，应在适用框架内形成等待、中性或负面判断，不应伪装成
数据缺失或市值超出范围。`conditions_not_met` 可作为 `decision_status`。
章盟主的大盘例外以 `seat_db` 为准；persona市值偏好不是硬上限。

龙虎榜区分 `ok / no_records / partial / error / not_applicable`。
只有通过完整日期/分页响应验证后，才能将空结果记为 `verified=true` 的0次。
上榜次数按唯一日期统计；营业部记录条数另列。席位明细不完整时，不生成
完整净买入额，也不以未知席位覆盖策略容量过滤。全市场龙虎榜不能冒充同板块。

补抓数据后应保存原序列、来源、日期、币种、会计口径及采样频率。
港股供应商抽样估值的分位不能标为每日分位。CAS与IFRS数据分别保留。
不得将扩产资本开支直接解释为维持性开支，或将未披露客户/良率填成已知。

复核评委后同步 `panel.json`、`agent_analysis.json`、核验快照与输入指纹。
仅补证据的评委复核不能宣称完成22维深度报告。保留无法补齐的缺口，
不为减少跳过人数强迫所有席位投票。

## 全部上市市场的投票网页

`python render_panel_web.py --tickers 688981.SH,00981.HK` 从同一发行人的多个缓存
生成可独立打开的 HTML，保留132席理由、来源、弃权类别和市场基准；支持市场、
流派、判断及文字搜索。导出时校验投票计数，同一公司以外的市场不可合并。
已声明分析师复核的面板必须通过当前 raw_data 指纹校验。

发布选项是 `--publish-dir <静态站点根目录> --base-url <站点URL>`；只有用户已
授权相应访问范围时使用。发布仅复制这个独立 HTML，不暴露缓存、会话或配置。
发布目录使用随机32位十六进制标识，生成 `.published.json` 记录网页URL。
随机链接不是身份验证；需要群成员访问限制的部署应由网关执行鉴权。

飞书应交付 `[全部市场评委投票](https://...)` 这种网页URL，不能把服务器本地
绝对路径、file:// 或仅在桌面环境有效的链接当作手机交付地址。
