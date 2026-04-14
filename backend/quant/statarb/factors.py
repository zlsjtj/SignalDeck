import pandas as pd


def _zscore(x: pd.Series, clip: float = 3.0) -> pd.Series:
    mu = x.mean()
    sd = x.std()
    if sd is None or sd == 0:
        return pd.Series(0.0, index=x.index)
    z = (x - mu) / sd
    return z.clip(-clip, clip)


def _safe_last(s: pd.Series, default: float = 0.0) -> float:
    if s is None or s.empty:
        return default
    v = s.iloc[-1]
    if pd.isna(v):
        return default
    return float(v)


def compute_scores(
    data,
    w_reversal: float = 1.0,
    w_momentum: float = 0.0,
    w_trend: float = 0.0,
    w_flow: float = 0.0,
    w_volz: float = 0.4,
    w_volume: float = 0.2,
    lookback: int = 24,
    mom_lookback: int = 12,
    trend_lookback: int = 72,
    flow_lookback: int = 24,
    vol_lookback: int = 12,
    volume_lookback: int = 24,
    zscore_clip: float = 3.0,
    use_market_neutral: bool = False,
    benchmark_symbol: str | None = None,
    min_notional_usdt: float = 0.0,
    max_vol: float = 0.0,
) -> pd.Series:
    symbols = list(data.keys())
    if not symbols:
        return pd.Series(dtype=float)

    last_ts = min(df.index.max() for df in data.values())
    need_bars = max(
        int(lookback) + 5,
        int(mom_lookback) + 5,
        int(trend_lookback) + 5,
        int(flow_lookback) + 5,
        int(vol_lookback) + 5,
        int(volume_lookback) + 5,
    )
    aligned = {s: data[s].loc[:last_ts].tail(need_bars) for s in symbols}

    close_df = pd.DataFrame({s: aligned[s]["close"] for s in symbols}).dropna()
    if close_df.empty:
        return pd.Series(0.0, index=symbols)
    volume_df = pd.DataFrame({s: aligned[s]["volume"] for s in symbols}).reindex(close_df.index).fillna(0.0)

    returns_1 = close_df.pct_change()

    mom_lb = max(3, int(mom_lookback))
    trend_lb = max(3, int(trend_lookback))
    flow_lb = max(3, int(flow_lookback))
    vol_lb = max(3, int(vol_lookback))
    volume_lb = max(3, int(volume_lookback))

    ret_mom = close_df.pct_change(mom_lb).iloc[-1].fillna(0.0)
    # Reversal uses latest 1-bar return with opposite sign.
    ret_reversal = -returns_1.iloc[-1].fillna(0.0)

    trend_ret = {}
    for s in symbols:
        c = close_df[s].tail(trend_lb)
        if len(c) < trend_lb:
            trend_ret[s] = 0.0
            continue
        ma = float(c.mean())
        last = float(c.iloc[-1])
        trend_ret[s] = 0.0 if ma <= 0 else (last / ma - 1.0)
    trend_ret = pd.Series(trend_ret)

    flow_raw = {}
    for s in symbols:
        r = returns_1[s].tail(flow_lb).fillna(0.0)
        v = volume_df[s].tail(flow_lb).fillna(0.0)
        flow_raw[s] = float((r * v).sum())
    flow_raw = pd.Series(flow_raw)

    vol_raw = returns_1.tail(vol_lb).std().replace([float("inf"), float("-inf")], pd.NA).fillna(0.0)
    # Lower short-term vol receives a higher score.
    vol_pref = -vol_raw

    volume_raw = {}
    for s in symbols:
        v = volume_df[s].tail(volume_lb)
        if len(v) < volume_lb:
            volume_raw[s] = 0.0
            continue
        cur = _safe_last(v, default=0.0)
        avg = float(v.mean())
        volume_raw[s] = 0.0 if avg <= 0 else (cur / avg - 1.0)
    volume_raw = pd.Series(volume_raw)

    if use_market_neutral and benchmark_symbol and benchmark_symbol in close_df.columns:
        bench_mom = float(ret_mom.get(benchmark_symbol, 0.0))
        bench_rev = float(ret_reversal.get(benchmark_symbol, 0.0))
        bench_trend = float(trend_ret.get(benchmark_symbol, 0.0))
        bench_flow = float(flow_raw.get(benchmark_symbol, 0.0))
        bench_vol = float(vol_pref.get(benchmark_symbol, 0.0))
        bench_volume = float(volume_raw.get(benchmark_symbol, 0.0))
        ret_mom = ret_mom - bench_mom
        ret_reversal = ret_reversal - bench_rev
        trend_ret = trend_ret - bench_trend
        flow_raw = flow_raw - bench_flow
        vol_pref = vol_pref - bench_vol
        volume_raw = volume_raw - bench_volume

    momentum_z = _zscore(ret_mom, clip=zscore_clip)
    reversal_z = _zscore(ret_reversal, clip=zscore_clip)
    trend_z = _zscore(trend_ret, clip=zscore_clip)
    flow_z = _zscore(flow_raw, clip=zscore_clip)
    volz = _zscore(vol_pref, clip=zscore_clip)
    volume_z = _zscore(volume_raw, clip=zscore_clip)

    score = (
        w_reversal * reversal_z
        + w_momentum * momentum_z
        + w_trend * trend_z
        + w_flow * flow_z
        + w_volz * volz
        + w_volume * volume_z
    )

    if min_notional_usdt > 0:
        last_notional = (close_df.iloc[-1] * volume_df.iloc[-1]).reindex(score.index).fillna(0.0)
        score = score.where(last_notional >= float(min_notional_usdt), other=0.0)

    if max_vol and max_vol > 0:
        ann_vol = returns_1.std().reindex(score.index).fillna(0.0) * (365.0 ** 0.5)
        score = score.where(ann_vol <= float(max_vol), other=0.0)

    score = score.fillna(score.median())
    return score
