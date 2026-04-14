# 从策略到代码的入门讲解（小白版）

> 结论先行：这个项目里当前的“精简版策略”是**等权做多 BCH/BNB/BTC**，每 14 天再平衡一次，信号只用“动量”这一条线索，但因为配置里 `long_quantile: 1.0`，所以**无论动量好坏都全仓等权持有**，本质上是一个“长期持有 + 固定再平衡”的策略。

---

## 0) 快速扫描：与回测/策略/配置/日志相关的文件

结论：核心入口就 3 类：配置、运行入口、回测/交易引擎。

- 配置文件
  - `config_2025_bch_bnb_btc_equal_combo.yaml`（组合策略配置，回测用）
  - `config.yaml`（实盘/纸盘运行用，保留 `keys`）
- 运行入口
  - `main.py`（实盘/纸盘入口，读 `config.yaml`）
  - `statarb/backtest.py`（回测入口）
  - `run_strategy.ps1`（脚本封装：paper/live/backtest）
- 核心逻辑模块
  - `statarb/factors.py`（信号计算）
  - `statarb/portfolio.py`（把信号变成仓位）
  - `statarb/execution.py`（下单/模拟下单）
  - `statarb/data.py`（拉 K 线）
  - `statarb/paper.py`（纸盘账户）
  - `statarb/risk.py`（风控状态与回撤/日亏）
  - `statarb/account.py`（实盘账户读取）
  - `statarb/config.py`（读取配置）
  - `statarb/utils.py`（小工具）
- 日志与结果
  - `logs/backtest_metrics_2023_combo_slim.txt`
  - `logs/backtest_metrics_2024_combo_slim.txt`
  - `logs/backtest_metrics_2025_combo_slim.txt`

---

## 1) 如何运行逐年回测（复现 2023/2024/2025）

结论：你只需要运行 `statarb/backtest.py`，把起止日期和配置文件传进去。

**命令（逐年）：**
```bash
.\venv\Scripts\python statarb/backtest.py --start 2023-01-01 --end 2023-12-31 --config config_2025_bch_bnb_btc_equal_combo.yaml --out logs/backtest_equity_2023_combo_slim.csv --trades logs/backtest_trades_2023_combo_slim.csv --metrics logs/backtest_metrics_2023_combo_slim.txt --plot logs/backtest_equity_2023_combo_slim.png

.\venv\Scripts\python statarb/backtest.py --start 2024-01-01 --end 2024-12-31 --config config_2025_bch_bnb_btc_equal_combo.yaml --out logs/backtest_equity_2024_combo_slim.csv --trades logs/backtest_trades_2024_combo_slim.csv --metrics logs/backtest_metrics_2024_combo_slim.txt --plot logs/backtest_equity_2024_combo_slim.png

.\venv\Scripts\python statarb/backtest.py --start 2025-01-01 --end 2025-12-31 --config config_2025_bch_bnb_btc_equal_combo.yaml --out logs/backtest_equity_2025_combo_slim.csv --trades logs/backtest_trades_2025_combo_slim.csv --metrics logs/backtest_metrics_2025_combo_slim.txt --plot logs/backtest_equity_2025_combo_slim.png
```

**参数解释（小白版）：**
- `--start / --end`：回测的起止日期
- `--config`：策略参数配置文件
- `--out`：输出资金曲线 CSV
- `--trades`：输出成交明细 CSV
- `--metrics`：输出指标文本（年化/回撤/Sharpe）
- `--plot`：输出资金曲线图片

---

## 2) 指标解释 + 2023-2025 结果对比表

结论：2023/2024 表现强，2025 明显变弱。

**对比表（来自日志）：**

| 年份 | 年化收益 | 最大回撤 | Sharpe |
|---|---:|---:|---:|
| 2023 | 132.2159% | -26.2046% | 4.6735 |
| 2024 | 127.5071% | -41.5700% | 4.0028 |
| 2025 | 22.1316% | -34.3732% | 1.5719 |

**指标怎么理解（小白版）：**
- **年化收益**：把这段时间的收益“换算成一年”的收益率。数字越大越好。
- **最大回撤**：资金从高点到低点最大的跌幅。数字越小（越接近 0）越好。
- **Sharpe**：简单理解为“收益/波动”的比值。越大代表收益更稳。

---

## 3) 策略讲解（先思路，后代码）

### A) 策略思路层（不先抛代码）

结论：这是一个**长仓等权 + 低频再平衡**策略，信号只用动量，但配置让它“永远持有”。

- **交易标的**：`BCH/USDT:USDT`、`BNB/USDT:USDT`、`BTC/USDT:USDT`
  - 位置：`config_2025_bch_bnb_btc_equal_combo.yaml` 的 `symbols`
- **周期与数据**：`timeframe: 6h`，回看窗口 `lookback_hours: 120`
- **信号规则（自然语言）**：
  - 用每个币的“动量”（过去若干周期的涨跌幅）做评分
  - 由于 `long_quantile: 1.0`，**所有币都被选入做多**
- **仓位规则**：
  - 等权做多，`gross_leverage: 1.05`
  - 3 个币等权后，单币约 0.35 仓位
- **再平衡**：`rebalance_every_minutes: 20160`（约 14 天）
- **退出规则**：
  - 这里没有主动止损/止盈；只在再平衡时调整
  - 相关配置在 `risk` 里，但目前都为 0 或 1.0（等于不启用）
- **交易成本与滑点**：
  - 回测时：手续费 `fee_bps: 6`，滑点 `slippage_bps: 2`
  - 实盘/纸盘下单时：`execution.order_type: limit` + `limit_price_offset_bps: 3`

**伪代码（与实际逻辑一致）：**
```
每 14 天：
  取 BCH/BNB/BTC 最近 120 小时的 6h K 线
  计算动量 score
  选前 100% 作为做多（= 全部）
  目标仓位 = 等权 * 1.05 总杠杆
  生成调仓订单并执行
```

### B) 代码实现层（思路映射到真实文件）

结论：**信号在 `statarb/factors.py` 算，仓位在 `statarb/portfolio.py` 算，回测在 `statarb/backtest.py` 跑**。

**关键文件清单与职责：**
- `main.py`：实盘/纸盘入口，循环执行“拉数据 -> 算信号 -> 算仓位 -> 下单”
- `statarb/backtest.py`：回测入口，读取历史 K 线并模拟下单
- `statarb/factors.py`：`compute_scores()` 只计算动量打分
- `statarb/portfolio.py`：`target_weights_from_scores()` 把打分转成等权仓位
- `statarb/data.py`：`fetch_universe()` 拉当前 K 线
- `statarb/execution.py`：`place_orders()` 处理下单与模拟下单
- `statarb/paper.py`：`PaperAccount` 纸盘账户资金变化
- `statarb/risk.py`：`drawdown()`、`daily_loss()` 统计风险
- `statarb/config.py`：`load_config()` 读取 YAML
- `statarb/account.py`：实盘读取权益与持仓

**调用链（ASCII 图）：**
```
K 线数据
   -> compute_scores()  [statarb/factors.py]
      -> target_weights_from_scores() [statarb/portfolio.py]
         -> build_order_intents() [main.py]
            -> place_orders() [statarb/execution.py]
               -> PaperAccount.apply_fills() / 实盘下单
                  -> 资金曲线 & 指标 [statarb/backtest.py]
```

---

## 4) “精简版代码（combo_slim）”说明

结论：当前仓库已经是“精简版”，只保留“动量打分 + 等权做多”的最小路径。

- **保留了什么**：
  - `compute_scores()` 只算动量
  - `target_weights_from_scores()` 只做等权长仓
  - `main.py` / `statarb/backtest.py` 只走一条策略路径
- **移除了什么**：
  - 原先可能存在的“多策略组合、趋势腿、突破腿、资金费率腿、动态杠杆”等分支
  - 在代码中已看不到这些分支逻辑（以 `main.py` 与 `statarb/backtest.py` 为准）

如果你想验证：打开 `main.py`，你会看到只有一段 `compute_scores()` -> `target_weights_from_scores()` 的流程。

---

## 5) 为什么 2025 变弱？（确定 vs 推测）

结论（确定）：**策略结构是“长期等权做多”**，所以业绩几乎等于“这三只币在该年整体涨跌”。

- **确定结论（来自代码）**：
  - `long_quantile: 1.0` 让 3 个币全部被选中做多
  - 没有止损/止盈/趋势过滤，风险参数也未启用
  - 所以策略“不会主动避险”

- **合理推测（结合市场常识）**：
  - 2025 这三只币的整体上涨幅度可能比 2023/2024 小
  - 或者 2025 波动更大、回撤更深
  - 由于策略一直持有，**涨得少就收益低，跌得多就回撤大**

如果你要验证这个推测：可以单独查看 2025 年这三只币的现货价格走势，或者用同样配置做 “单币回测”。

---

## 6) 如何安全改参数 + 验证（小白动手指南）

结论：最安全的改动是“周期、再平衡频率、杠杆大小”。

**最常改的参数（都在 `config_2025_bch_bnb_btc_equal_combo.yaml`）：**
- `rebalance_every_minutes`：再平衡频率
- `timeframe` / `lookback_hours`：K 线周期与回看长度
- `portfolio.gross_leverage`：总杠杆

**改完怎么验证：**
1. 修改配置文件
2. 运行回测命令（见第 1 节）
3. 看 `logs/backtest_metrics_YYYY_combo_slim.txt` 里的年化/回撤/Sharpe

**最小改动示例（安全）：**
- 把 `rebalance_every_minutes` 从 `20160` 改成 `10080`（从 14 天游到 7 天）
  - 你会更频繁调仓，可能减少偏离，但交易成本也会增加

---

## 举例：用 10 根 K 线手工演示一次信号与仓位

结论：即使你不懂量化，也能理解“动量打分 -> 等权持有”的流程。

**为了好算，演示用更小的动量窗口（3 根）**，逻辑和代码一致，只是把 `mom_lookback` 从 12 改成 3 方便示例。

假设 3 个币（BCH/BNB/BTC）最后 4 根收盘价如下（单位随意）：

- BCH: 100, 105, 110, 120
- BNB: 100, 102, 101, 103
- BTC: 100,  98,  99, 100

**步骤 1：算动量（3 根涨跌）**
- BCH: 120 / 105 - 1 = 14.29%
- BNB: 103 / 102 - 1 = 0.98%
- BTC: 100 /  98 - 1 = 2.04%

**步骤 2：做 zscore（标准化）**
- 这里只是把三者放在一起对比高低（代码：`_zscore()`）
- BCH 分最高，BNB 分最低

**步骤 3：选做多（但这里是 100%）**
- `long_quantile: 1.0` => 全部做多
- 所以三个币都进仓

**步骤 4：算仓位（等权 + 1.05 杠杆）**
- 总杠杆 1.05，3 个币平均 = 0.35
- 所以目标仓位约：BCH 35%，BNB 35%，BTC 35%

**步骤 5：下一根 K 线结算收益**
- 如果 BCH/BNB/BTC 下一根都涨 1%，总权益约涨 1% * 1.05 = 1.05%

这就是本策略的核心：**动量打分只是“形式上存在”，仓位仍是等权长持。**

---

## 如何复现实盘/纸盘

结论：`main.py` 始终读 `config.yaml`，由 `paper` 决定是否实盘。

- `config.yaml` 里：
  - `paper: true` => 纸盘
  - `paper: false` => 实盘（需要 `keys`）

直接运行：
```bash
.\venv\Scripts\python main.py
```

---

## 结果汇总 CSV

结论：我已生成一份简单汇总表。

- `results/summary_combo_slim.csv`

---

## 如果你要进一步验证

结论：看两个地方就够了。

- 策略参数：`config_2025_bch_bnb_btc_equal_combo.yaml`
- 回测指标：`logs/backtest_metrics_YYYY_combo_slim.txt`

如果某个细节你在代码里找不到，我建议从 `main.py` 或 `statarb/backtest.py` 里按函数名反查（`compute_scores` / `target_weights_from_scores` / `place_orders`）。
