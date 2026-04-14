import pandas as pd


def target_weights_from_scores(
    score: pd.Series,
    long_q=0.2,
    short_q=0.2,
    gross_leverage=1.2,
    max_w=0.12,
    min_score_spread=0.0,
    long_high_score: bool = False,
    weight_mode: str = "equal",
    score_weight_clip: float = 3.0,
) -> pd.Series:
    n = len(score)
    k_long = 0 if long_q <= 0 else max(1, int(n * long_q))
    k_short = 0 if short_q <= 0 else max(1, int(n * short_q))

    if min_score_spread and n > 1:
        spread = float(score.quantile(0.9) - score.quantile(0.1))
        if spread < float(min_score_spread):
            return pd.Series(0.0, index=score.index)

    if long_high_score:
        long_syms = score.sort_values(ascending=False).head(k_long).index
        short_syms = score.sort_values(ascending=True).head(k_short).index
    else:
        long_syms = score.sort_values(ascending=True).head(k_long).index
        short_syms = score.sort_values(ascending=False).head(k_short).index

    w = pd.Series(0.0, index=score.index)
    if len(long_syms) > 0:
        if weight_mode == "score":
            long_scores = score.loc[long_syms].clip(-score_weight_clip, score_weight_clip)
            long_scores = long_scores.clip(lower=0.0)
            if long_scores.sum() > 0:
                w.loc[long_syms] = long_scores / long_scores.sum()
            else:
                w.loc[long_syms] = 1.0 / len(long_syms)
        else:
            w.loc[long_syms] = 1.0 / len(long_syms)
    if len(short_syms) > 0:
        if weight_mode == "score":
            short_scores = score.loc[short_syms].clip(-score_weight_clip, score_weight_clip)
            short_scores = (-short_scores).clip(lower=0.0)
            if short_scores.sum() > 0:
                w.loc[short_syms] = -(short_scores / short_scores.sum())
            else:
                w.loc[short_syms] = -1.0 / len(short_syms)
        else:
            w.loc[short_syms] = -1.0 / len(short_syms)

    gross = w.abs().sum()
    if gross > 0:
        w = w / gross
    w = w * gross_leverage

    w = w.clip(-max_w, max_w)
    gross2 = w.abs().sum()
    if gross2 > 0:
        w = w * (gross_leverage / gross2)
    return w
