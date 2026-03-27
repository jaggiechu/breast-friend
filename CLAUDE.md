# Breast Friend — Project Instructions

## Auto-refresh Data
**IMPORTANT:** At the start of every conversation in this project, BEFORE answering any user questions, run:
```bash
cd "/Users/chenzhu/Claude Code Projects/breast-friend" && source .venv/bin/activate && python3 update.py --skip-sheets --skip-html 2>&1
```
This ensures the database has the latest data from iCloud Baby Tracker.

Then read the latest daily_summary.csv to have context for answering questions:
```bash
cat "/Users/chenzhu/Claude Code Projects/breast-friend/daily_summary.csv"
```

## Project Overview
- Baby feeding tracker for Liz (born 2026-01-29)
- Data source: Baby Tracker app → iCloud `.btbk` backups (auto-synced)
- Pipeline: `.btbk` (zip of SQLite) → parse → `breast_friend.db` → daily_summary
- Outputs: CSV, Google Sheet, HTML dashboard (GitHub Pages)

## Key Data Notes
- Nursing ml tracking started 2/18/26, reliable from 2/20/26
- Days before 2/20 have no nursing transfer volume — exclude from nursing-related comparisons
- Incomplete days (last record before 11pm) should not be compared with complete days
- `bm_supply_ml` = nursing_vol_ml + pump_total_ml
- `Pumped` table in EasyLog = expressed breast milk fed to baby (not pump output)

## Triple-Feed Workflow (每轮喂奶流程)

每一轮按顺序（每步都是 optional）：
1. **叫醒宝宝** → sleep 结束时间点
2. **换尿布** → diaper 数据点
3. **亲喂 nursing**（可选）→ 30-50 分钟，喂前喂后称重得出 transfer ml（记在备注；`est` = 忘记称重为估算值）。半夜或妈妈不在时跳过
4. **补瓶喂**（可选）→ 亲喂 transfer 已达目标则不需要补。否则：先用泵奶库存补（expressed），不足的用水奶补（formula）
5. **妈妈泵奶**（可选）→ 可能紧接着泵，也可能等一段时间再泵以获得更大奶量

瓶喂可能由 nanny 执行，所以 expressed/formula 的时间戳可能与妈妈泵奶时间重叠。

### 泵奶时机
- 妈妈可以选择每轮后立即泵，或在两轮之间等待
- 如果妈妈需要睡觉或出门，可以跳过泵奶
- 半夜 3-5 点那轮：妈妈**只泵奶不亲喂**，宝宝只瓶喂

### 夜间时间戳例外
- 每天最后一轮约 11pm-12am，有时超过午夜
- 如果超过 12 点，时间戳会调整到 **11:57/58/59 PM**，备注记录真实时间

### 母乳产量目标
1. **Frequent milk removal** → 增加产量
2. **不超过 3-4 小时不 removal** → 避免堵奶

## 分轮规则 (Round Grouping)

**一轮 = 宝宝吃了一顿饭。** 不要用固定时间间隔（如90分钟）定义轮次。

判断方法：沿时间轴累加宝宝摄入（nursing_ml + expressed + formula），当累计达到当天每顿目标（~90-100ml）时，这一轮结束。紧跟的 pump 归入同一轮。下一个 feeding event 开始新轮。

关键点：
- 轮不一定以 nursing 开始 — 可以从瓶喂开始（妈妈跳过亲喂）
- 纯瓶喂 + pump = 完整一轮
- 时间间隔只是辅助信号（>3-4小时几乎肯定是新轮），不是定义标准
- 两顿饭可能只隔 60-70 分钟

## 库存追踪规则 (Inventory Tracking)

检查有没有漏记泵奶时，**必须按每一笔 event 的时间顺序逐笔计算**，不能按轮汇总。

原因：同一轮里 pump 经常在 expressed 之后发生。如果按轮汇总看起来可能没问题，但在 expressed 那个 moment 库存已经是负数了 — 说明有漏记。

规则：
- Pump: `inventory += total_ml`
- Expressed: `inventory -= amount_ml`（检查备注中 "Given Xml" 用实际给出量）
- 每次 inventory 变负就标记 — 那个时间点之前必有漏记
- Collector（亲喂时接奶器接另一侧）通常 5-15ml，可能未记录为 pump。库存差额 ≤15ml 优先考虑 collector 漏记

## Key Files
- `config.py` — all paths and constants
- `ingestion/icloud_reader.py` — reads .btbk from iCloud
- `data/loader.py` — orchestrates data loading
- `data/aggregator.py` — builds daily_summary
- `output/html_dashboard.py` — generates interactive HTML
- `output/google_sheets.py` — exports to Google Sheets
- `update.py` — main entry point
