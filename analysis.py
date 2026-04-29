"""
H1: Basis mean-reverts faster on Binance than Bybit
H2: Directional asymmetry in correction speed (contango vs backwardation)
H3: Volatility moderates efficiency, moderation differs between exchanges
"""

import sqlite3
import os
import pandas as pd
import numpy as np
from scipy import stats
from statsmodels.tsa.stattools import adfuller, grangercausalitytests
from statsmodels.tsa.vector_ar.vecm import coint_johansen
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
import warnings
warnings.filterwarnings('ignore')

# ── Nature-branded colour palette ────────────────────────────────────────────
BLUE   = '#0C5DA5'   # Binance
RED    = '#D62728'    # Bybit  (Nature red)
TEAL   = '#00B7A7'    # accent / low-vol
OLIVE  = '#6B8E23'    # accent / high-vol
GREY   = '#888888'
DARK   = '#333333'

COLORS = {'binance': BLUE, 'bybit': RED}

CHART_DIR = 'charts'
os.makedirs(CHART_DIR, exist_ok=True)

DB_PATH = './data/funding_rate.db'
OVERLAP_START = 1706572860000
OVERLAP_END   = 1769414400000

# ── Global matplotlib styling (Nature-like) ──────────────────────────────────
plt.rcParams.update({
    'font.family': 'sans-serif',
    'font.sans-serif': ['Helvetica', 'Arial', 'DejaVu Sans'],
    'font.size': 9,
    'axes.linewidth': 0.8,
    'axes.edgecolor': DARK,
    'axes.labelcolor': DARK,
    'axes.grid': False,
    'xtick.color': DARK,
    'ytick.color': DARK,
    'xtick.major.width': 0.8,
    'ytick.major.width': 0.8,
    'xtick.direction': 'out',
    'ytick.direction': 'out',
    'legend.frameon': False,
    'figure.dpi': 300,
    'savefig.dpi': 300,
    'savefig.bbox': 'tight',
    'savefig.pad_inches': 0.15,
})


def load_data(exchange, start=OVERLAP_START, end=OVERLAP_END):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT timestamp, spotPrice, futuresPrice, indexPrice, premiumIndex, "
        "estimatedFundingRate, realisedFundingRate FROM snapshots "
        "WHERE exchange=? AND pair='BTC-USDT' AND timestamp>=? AND timestamp<=? "
        "ORDER BY timestamp",
        conn, params=(exchange, start, end)
    )
    conn.close()
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df.set_index('datetime', inplace=True)
    df['basis_bps'] = (df['futuresPrice'] - df['spotPrice']) / df['spotPrice'] * 10000
    df['abs_basis_bps'] = df['basis_bps'].abs()
    df['log_ret'] = np.log(df['spotPrice'] / df['spotPrice'].shift(1))
    return df


def section(title):
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}")


def half_life(series):
    """AR(1) half-life of mean reversion on a 1-minute basis series."""
    s = series.dropna()
    y = s.values[1:]
    x = s.values[:-1]
    slope, intercept, _, p_val, _ = stats.linregress(x, y)
    phi = slope
    beta = phi - 1.0
    if beta >= 0:
        return phi, beta, np.inf, p_val
    hl = -np.log(2) / beta
    return phi, beta, hl, p_val


# ══════════════════════════════════════════════════════════════════════════════
section("LOADING DATA")
binance = load_data('binance')
bybit   = load_data('bybit')

ts_inner = max(binance.index.min(), bybit.index.min())
ts_outer = min(binance.index.max(), bybit.index.max())
binance = binance.loc[ts_inner:ts_outer]
bybit   = bybit.loc[ts_inner:ts_outer]

for name, df in [('Binance', binance), ('Bybit', bybit)]:
    print(f"\n{name}: {len(df):,} obs, "
          f"{df.index.min().strftime('%Y-%m-%d')} → {df.index.max().strftime('%Y-%m-%d')}, "
          f"Spot ${df['spotPrice'].min():,.0f}–${df['spotPrice'].max():,.0f}")

# ══════════════════════════════════════════════════════════════════════════════
section("PREREQUISITE: DESCRIPTIVE STATISTICS")

for name, df in [('Binance', binance), ('Bybit', bybit)]:
    b = df['basis_bps']
    print(f"\n{name} Basis (bps):")
    print(f"  Mean={b.mean():.4f}  Median={b.median():.4f}  Std={b.std():.4f}")
    print(f"  Min={b.min():.2f}  Max={b.max():.2f}  MAB={df['abs_basis_bps'].mean():.4f}")
    print(f"  Skew={b.skew():.4f}  Kurt={b.kurtosis():.4f}")
    print(f"  %Positive={100*((b>0).sum()/len(b)):.2f}%  %Negative={100*((b<0).sum()/len(b)):.2f}%")

# ══════════════════════════════════════════════════════════════════════════════
section("PREREQUISITE: STATIONARITY (ADF)")

hourly_bin = binance.resample('h').first().dropna()
hourly_byb = bybit.resample('h').first().dropna()

for name, h in [('Binance', hourly_bin), ('Bybit', hourly_byb)]:
    print(f"\n{name}:")
    for col, label in [('basis_bps', 'Basis'), ('spotPrice', 'Spot'), ('futuresPrice', 'Futures')]:
        adf_stat, p, _, _, _, _ = adfuller(h[col].dropna(), maxlag=24, autolag='AIC')
        print(f"  {label:10s}: ADF={adf_stat:.3f}  p={p:.6f} {'***' if p<0.01 else '**' if p<0.05 else '*' if p<0.10 else ''}")
    ret = h['spotPrice'].pct_change().dropna()
    adf_stat, p, _, _, _, _ = adfuller(ret, maxlag=24, autolag='AIC')
    print(f"  {'Spot Ret':10s}: ADF={adf_stat:.3f}  p={p:.6f} {'***' if p<0.01 else ''}")

# ══════════════════════════════════════════════════════════════════════════════
section("PREREQUISITE: COINTEGRATION")

for name, h in [('Binance', hourly_bin), ('Bybit', hourly_byb)]:
    spot = h['spotPrice'].dropna()
    fut  = h['futuresPrice'].dropna()
    common = spot.index.intersection(fut.index)
    spot, fut = spot.loc[common], fut.loc[common]

    slope, intercept, _, p_eg, _ = stats.linregress(spot, fut)
    resid = fut - (slope * spot + intercept)
    adf_res, p_res, *_ = adfuller(resid, maxlag=24, autolag='AIC')

    joh = coint_johansen(np.column_stack([spot.values, fut.values]), det_order=0, k_ar_diff=2)
    trace_0 = joh.lr1[0]
    cv_0    = joh.cvt[0, 1]

    print(f"\n{name}:")
    print(f"  Engle-Granger: Futures = {slope:.4f} × Spot + {intercept:.2f}  (resid ADF p={p_res:.6f})")
    print(f"  Johansen trace: {trace_0:.2f} vs 5% cv {cv_0:.2f} → {'Reject' if trace_0>cv_0 else 'Fail'} H0 of no coint.")

# ══════════════════════════════════════════════════════════════════════════════
section("H1: MEAN REVERSION SPEED COMPARISON")

results_h1 = {}
for name, df in [('Binance', binance), ('Bybit', bybit)]:
    phi, beta, hl, p = half_life(df['basis_bps'])
    results_h1[name] = {'phi': phi, 'beta': beta, 'half_life_min': hl, 'half_life_hrs': hl/60, 'p': p}
    print(f"\n{name} — full-sample AR(1) on minute basis:")
    print(f"  φ = {phi:.6f}, β = {beta:.6f}, half-life = {hl:.1f} min ({hl/60:.2f} hrs), p(slope) = {p:.2e}")

hl_diff = results_h1['Bybit']['half_life_hrs'] - results_h1['Binance']['half_life_hrs']
pct_diff = hl_diff / results_h1['Bybit']['half_life_hrs'] * 100
print(f"\nDifference: Binance is {hl_diff:.2f} hrs faster ({pct_diff:.1f}% faster)")

# Granger causality
section("H1: GRANGER CAUSALITY (FR → Basis Change)")
for name, h in [('Binance', hourly_bin), ('Bybit', hourly_byb)]:
    print(f"\n{name}:")
    basis_chg = h['basis_bps'].diff().dropna()
    fr = h['estimatedFundingRate'].dropna()
    common = basis_chg.index.intersection(fr.index)
    data_gc = pd.DataFrame({'basis_chg': basis_chg.loc[common], 'fr': fr.loc[common]}).dropna()
    for lag in [1, 2, 4]:
        result = grangercausalitytests(data_gc[['basis_chg', 'fr']], maxlag=lag, verbose=False)
        fstat = result[lag][0]['ssr_ftest'][0]
        pval  = result[lag][0]['ssr_ftest'][1]
        print(f"  Lag {lag}: F={fstat:.2f}, p={pval:.6f} {'***' if pval<0.01 else ''}")

# Rolling MAB comparison
section("H1: ROLLING MAB (30-day)")
roll_bin = binance['abs_basis_bps'].resample('D').mean().rolling(30).mean().dropna()
roll_byb = bybit['abs_basis_bps'].resample('D').mean().rolling(30).mean().dropna()
common_idx = roll_bin.index.intersection(roll_byb.index)
roll_bin_c = roll_bin.loc[common_idx]
roll_byb_c = roll_byb.loc[common_idx]

t_stat, p_paired = stats.ttest_rel(roll_bin_c, roll_byb_c)
print(f"Paired t-test on 30-day rolling MAB (daily): t={t_stat:.3f}, p={p_paired:.6f}")
print(f"Mean rolling MAB — Binance: {roll_bin_c.mean():.4f} bps, Bybit: {roll_byb_c.mean():.4f} bps")

# Hourly MAB paired test
hourly_mab_bin = binance['abs_basis_bps'].resample('h').mean().dropna()
hourly_mab_byb = bybit['abs_basis_bps'].resample('h').mean().dropna()
common_h = hourly_mab_bin.index.intersection(hourly_mab_byb.index)
t_h, p_h = stats.ttest_rel(hourly_mab_bin.loc[common_h], hourly_mab_byb.loc[common_h])
print(f"Paired t-test on hourly MAB: t={t_h:.3f}, p={p_h:.6f}")

# ══════════════════════════════════════════════════════════════════════════════
section("H2: DIRECTIONAL ASYMMETRY")

results_h2 = {}
for name, df in [('Binance', binance), ('Bybit', bybit)]:
    pos = df.loc[df['basis_bps'] > 0, 'basis_bps']
    neg = df.loc[df['basis_bps'] < 0, 'basis_bps']

    phi_p, beta_p, hl_p, p_p = half_life(pos)
    phi_n, beta_n, hl_n, p_n = half_life(neg)

    results_h2[name] = {
        'pos_hl_min': hl_p, 'neg_hl_min': hl_n,
        'pos_hl_hrs': hl_p/60, 'neg_hl_hrs': hl_n/60,
        'pos_phi': phi_p, 'neg_phi': phi_n,
        'pos_n': len(pos), 'neg_n': len(neg),
    }

    print(f"\n{name}:")
    print(f"  Contango  (basis>0): n={len(pos):,}, φ={phi_p:.6f}, HL={hl_p:.1f} min ({hl_p/60:.2f} hrs)")
    print(f"  Backwrd   (basis<0): n={len(neg):,}, φ={phi_n:.6f}, HL={hl_n:.1f} min ({hl_n/60:.2f} hrs)")
    ratio = hl_p / hl_n if hl_n != np.inf and hl_n > 0 else np.inf
    print(f"  Contango/Backwardation HL ratio: {ratio:.3f}")

# Wilcoxon test: correction magnitudes by direction per exchange
print("\nCorrection magnitude comparison (|Δbasis| by direction):")
for name, df in [('Binance', binance), ('Bybit', bybit)]:
    delta = df['basis_bps'].diff()
    pos_mask = df['basis_bps'].shift(1) > 0
    neg_mask = df['basis_bps'].shift(1) < 0
    corr_from_pos = delta[pos_mask].abs().dropna()
    corr_from_neg = delta[neg_mask].abs().dropna()
    u_stat, p_mw = stats.mannwhitneyu(corr_from_pos.sample(min(50000, len(corr_from_pos)), random_state=42),
                                       corr_from_neg.sample(min(50000, len(corr_from_neg)), random_state=42),
                                       alternative='two-sided')
    print(f"  {name}: median|Δ| from contango={corr_from_pos.median():.4f}, "
          f"from backwardation={corr_from_neg.median():.4f}, Mann-Whitney p={p_mw:.6f}")

# Interaction: exchange × direction
print("\nInteraction (exchange × direction) — half-life differences:")
bin_asym = results_h2['Binance']['pos_hl_hrs'] - results_h2['Binance']['neg_hl_hrs']
byb_asym = results_h2['Bybit']['pos_hl_hrs'] - results_h2['Bybit']['neg_hl_hrs']
print(f"  Binance contango-backwardation HL gap: {bin_asym:+.2f} hrs")
print(f"  Bybit   contango-backwardation HL gap: {byb_asym:+.2f} hrs")
print(f"  Difference in asymmetry: {abs(bin_asym - byb_asym):.2f} hrs")

# ══════════════════════════════════════════════════════════════════════════════
section("H3: VOLATILITY REGIME MODERATION")

results_h3 = {}
for name, df in [('Binance', binance), ('Bybit', bybit)]:
    daily_ret = df['log_ret'].resample('D').sum()
    vol_20d = daily_ret.rolling(20).std() * np.sqrt(365) * 100
    vol_daily = vol_20d.dropna()
    vol_median = vol_daily.median()

    df_daily_vol = vol_daily.to_frame('vol')
    df_daily_vol['regime'] = np.where(df_daily_vol['vol'] <= vol_median, 'low', 'high')

    daily_mab = df['abs_basis_bps'].resample('D').mean()
    merged = df_daily_vol.join(daily_mab.rename('mab'), how='inner')

    low  = merged.loc[merged['regime'] == 'low',  'mab']
    high = merged.loc[merged['regime'] == 'high', 'mab']

    t_vol, p_vol = stats.ttest_ind(low, high, equal_var=False)
    u_vol, p_u   = stats.mannwhitneyu(low, high, alternative='two-sided')

    results_h3[name] = {
        'vol_median': vol_median,
        'mab_low': low.mean(), 'mab_high': high.mean(),
        'pct_increase': (high.mean() - low.mean()) / low.mean() * 100,
        't_stat': t_vol, 'p_ttest': p_vol,
        'u_stat': u_vol, 'p_mann': p_u,
        'n_low': len(low), 'n_high': len(high),
    }

    print(f"\n{name} (vol median = {vol_median:.1f}%):")
    print(f"  Low-vol MAB:  {low.mean():.4f} bps (n={len(low)} days)")
    print(f"  High-vol MAB: {high.mean():.4f} bps (n={len(high)} days)")
    print(f"  Increase: {(high.mean()-low.mean())/low.mean()*100:.1f}%")
    print(f"  Welch t={t_vol:.3f}, p={p_vol:.6f}")
    print(f"  Mann-Whitney U={u_vol:.0f}, p={p_u:.6f}")

# Half-life by regime
section("H3: HALF-LIFE BY VOLATILITY REGIME")
for name, df in [('Binance', binance), ('Bybit', bybit)]:
    daily_ret = df['log_ret'].resample('D').sum()
    vol_20d = daily_ret.rolling(20).std() * np.sqrt(365) * 100
    vol_expanded = vol_20d.reindex(df.index, method='ffill')
    vol_median = vol_20d.dropna().median()

    low_mask  = vol_expanded <= vol_median
    high_mask = vol_expanded > vol_median

    phi_l, beta_l, hl_l, _ = half_life(df.loc[low_mask, 'basis_bps'])
    phi_h, beta_h, hl_h, _ = half_life(df.loc[high_mask, 'basis_bps'])

    results_h3[name]['hl_low_min'] = hl_l
    results_h3[name]['hl_high_min'] = hl_h
    results_h3[name]['hl_low_hrs'] = hl_l / 60
    results_h3[name]['hl_high_hrs'] = hl_h / 60

    print(f"\n{name}:")
    print(f"  Low-vol  HL: {hl_l:.1f} min ({hl_l/60:.2f} hrs)")
    print(f"  High-vol HL: {hl_h:.1f} min ({hl_h/60:.2f} hrs)")
    print(f"  Ratio (high/low): {hl_h/hl_l:.3f}")

# Interaction: exchange × regime
print("\nInteraction (exchange × regime):")
bin_regime_gap = results_h3['Binance']['mab_high'] - results_h3['Binance']['mab_low']
byb_regime_gap = results_h3['Bybit']['mab_high'] - results_h3['Bybit']['mab_low']
print(f"  Binance MAB increase in high-vol: {bin_regime_gap:.4f} bps")
print(f"  Bybit   MAB increase in high-vol: {byb_regime_gap:.4f} bps")
print(f"  Difference in deterioration: {abs(bin_regime_gap - byb_regime_gap):.4f} bps")

# Extreme events by regime
section("H3: EXTREME EVENTS BY REGIME")
for name, df in [('Binance', binance), ('Bybit', bybit)]:
    daily_ret = df['log_ret'].resample('D').sum()
    vol_20d = daily_ret.rolling(20).std() * np.sqrt(365) * 100
    vol_expanded = vol_20d.reindex(df.index, method='ffill')
    vol_median = vol_20d.dropna().median()

    extreme = df['abs_basis_bps'] > 30
    low_extreme = (extreme & (vol_expanded <= vol_median)).sum()
    high_extreme = (extreme & (vol_expanded > vol_median)).sum()
    total_low = (vol_expanded <= vol_median).sum()
    total_high = (vol_expanded > vol_median).sum()

    print(f"\n{name} (|basis| > 30 bps):")
    print(f"  Low-vol:  {low_extreme} events  ({low_extreme/total_low*10000:.2f} per 10k min)")
    print(f"  High-vol: {high_extreme} events ({high_extreme/total_high*10000:.2f} per 10k min)")

# ══════════════════════════════════════════════════════════════════════════════
section("ADDITIONAL: REALISED FUNDING RATE STATS")
for name, df in [('Binance', binance), ('Bybit', bybit)]:
    rfr = df['realisedFundingRate'].dropna()
    rfr_unique = rfr.drop_duplicates()
    periods = df.resample('8h').first().dropna()
    rfr_period = periods['realisedFundingRate']
    print(f"\n{name} Realised FR (per 8h period):")
    print(f"  Unique periods: {len(rfr_period)}")
    print(f"  Mean: {rfr_period.mean()*100:.4f}%   Median: {rfr_period.median()*100:.4f}%")
    print(f"  Std:  {rfr_period.std()*100:.4f}%")
    print(f"  Min:  {rfr_period.min()*100:.4f}%   Max: {rfr_period.max()*100:.4f}%")
    print(f"  %Positive: {100*(rfr_period>0).sum()/len(rfr_period):.2f}%")
    print(f"  %Negative: {100*(rfr_period<0).sum()/len(rfr_period):.2f}%")
    ann = rfr_period.mean() * 3 * 365 * 100
    print(f"  Annualised carry: {ann:.2f}%")

# Correlation estimated vs realised
for name, df in [('Binance', binance), ('Bybit', bybit)]:
    periods = df.resample('8h').last().dropna()
    corr = periods['estimatedFundingRate'].corr(periods['realisedFundingRate'])
    print(f"{name} Est-Real FR correlation: {corr:.4f}")

# ══════════════════════════════════════════════════════════════════════════════
section("ADDITIONAL: MONTHLY MAB (for appendix)")
for name, df in [('Binance', binance), ('Bybit', bybit)]:
    monthly = df['abs_basis_bps'].resample('M').mean()
    print(f"\n{name} Monthly MAB (bps):")
    for dt, val in monthly.items():
        print(f"  {dt.strftime('%Y-%m')}: {val:.4f}")

# Cross-exchange basis correlation
section("ADDITIONAL: CROSS-EXCHANGE METRICS")
common = binance.index.intersection(bybit.index)
corr_basis = binance.loc[common, 'basis_bps'].corr(bybit.loc[common, 'basis_bps'])
corr_sf_bin = binance['spotPrice'].corr(binance['futuresPrice'])
corr_sf_byb = bybit['spotPrice'].corr(bybit['futuresPrice'])
print(f"Cross-exchange basis correlation: {corr_basis:.6f}")
print(f"Binance spot-futures correlation: {corr_sf_bin:.8f}")
print(f"Bybit   spot-futures correlation: {corr_sf_byb:.8f}")

# Event study around funding settlements
section("ADDITIONAL: EVENT STUDY (funding settlements)")
for name, df in [('Binance', binance), ('Bybit', bybit)]:
    settlement_hours = [0, 8, 16]
    df_tmp = df.copy()
    df_tmp['hour'] = df_tmp.index.hour
    df_tmp['minute'] = df_tmp.index.minute

    settlements = df_tmp[(df_tmp['hour'].isin(settlement_hours)) & (df_tmp['minute'] == 0)].index
    print(f"\n{name} ({len(settlements)} settlements):")

    for window in [30, 60, 120]:
        before_vals, after_vals = [], []
        for s in settlements:
            pre = df_tmp.loc[s - pd.Timedelta(minutes=window):s - pd.Timedelta(minutes=1), 'abs_basis_bps']
            post = df_tmp.loc[s + pd.Timedelta(minutes=1):s + pd.Timedelta(minutes=window), 'abs_basis_bps']
            if len(pre) > window*0.8 and len(post) > window*0.8:
                before_vals.append(pre.mean())
                after_vals.append(post.mean())
        before_arr = np.array(before_vals)
        after_arr  = np.array(after_vals)
        t_es, p_es = stats.ttest_rel(before_arr, after_arr)
        pct_chg = (after_arr.mean() - before_arr.mean()) / before_arr.mean() * 100
        print(f"  {window:3d}-min window: before={before_arr.mean():.3f}, after={after_arr.mean():.3f}, "
              f"chg={pct_chg:+.2f}%, t={t_es:.3f}, p={p_es:.4f}")


# ══════════════════════════════════════════════════════════════════════════════
# CHARTS
# ══════════════════════════════════════════════════════════════════════════════
section("GENERATING CHARTS")


def clean_axes(ax):
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)


# ── Chart 1: Rolling 30-day MAB time series (H1) ────────────────────────────
fig, ax = plt.subplots(figsize=(7, 3.5))
ax.plot(roll_bin_c.index, roll_bin_c.values, color=BLUE, linewidth=1.2, label='Binance')
ax.plot(roll_byb_c.index, roll_byb_c.values, color=RED,  linewidth=1.2, label='Bybit')
ax.set_ylabel('Mean Absolute Basis (bps)')
ax.set_xlabel('')
ax.legend(loc='upper right', fontsize=8)
clean_axes(ax)
ax.set_title('30-Day Rolling MAB', fontsize=10, fontweight='bold', pad=10)
fig.tight_layout()
fig.savefig(f'{CHART_DIR}/fig1_rolling_mab.png')
plt.close()
print("  Saved fig1_rolling_mab.png")

# ── Chart 2: Basis distribution histogram (H1) ──────────────────────────────
fig, ax = plt.subplots(figsize=(7, 3.5))
bins_range = np.linspace(-25, 25, 120)
ax.hist(binance['basis_bps'].clip(-25, 25), bins=bins_range, alpha=0.6,
        color=BLUE, label='Binance', density=True)
ax.hist(bybit['basis_bps'].clip(-25, 25), bins=bins_range, alpha=0.6,
        color=RED, label='Bybit', density=True)
ax.axvline(0, color=GREY, linestyle='--', linewidth=0.7)
ax.set_xlabel('Basis (bps)')
ax.set_ylabel('Density')
ax.legend(fontsize=8)
clean_axes(ax)
ax.set_title('Basis Distribution', fontsize=10, fontweight='bold', pad=10)
fig.tight_layout()
fig.savefig(f'{CHART_DIR}/fig2_basis_distribution.png')
plt.close()
print("  Saved fig2_basis_distribution.png")

# ── Chart 3: Half-life bar chart (H2) ───────────────────────────────────────
fig, ax = plt.subplots(figsize=(6, 4))
labels = ['Full\nSample', 'Contango\n(basis > 0)', 'Backwardation\n(basis < 0)']
bin_vals = [results_h1['Binance']['half_life_hrs'],
            results_h2['Binance']['pos_hl_hrs'],
            results_h2['Binance']['neg_hl_hrs']]
byb_vals = [results_h1['Bybit']['half_life_hrs'],
            results_h2['Bybit']['pos_hl_hrs'],
            results_h2['Bybit']['neg_hl_hrs']]

x = np.arange(len(labels))
width = 0.32
bars1 = ax.bar(x - width/2, bin_vals, width, color=BLUE, label='Binance')
bars2 = ax.bar(x + width/2, byb_vals, width, color=RED,  label='Bybit')

for bars in [bars1, bars2]:
    for bar in bars:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, h + 0.3, f'{h:.1f}',
                ha='center', va='bottom', fontsize=7.5)

ax.set_ylabel('Half-Life (hours)')
ax.set_xticks(x)
ax.set_xticklabels(labels, fontsize=8)
ax.legend(fontsize=8)
clean_axes(ax)
ax.set_title('Basis Mean Reversion Half-Life', fontsize=10, fontweight='bold', pad=10)
fig.tight_layout()
fig.savefig(f'{CHART_DIR}/fig3_halflife_directional.png')
plt.close()
print("  Saved fig3_halflife_directional.png")

# ── Chart 4: Correction trajectories — contango vs backwardation (H2) ──────
section("H2: CORRECTION TRAJECTORY ANALYSIS")

def compute_correction_curve(df, direction, max_lag=120):
    """Average basis trajectory after entering contango/backwardation."""
    basis = df['basis_bps'].values
    if direction == 'contango':
        entries = np.where((basis[:-1] <= 0) & (basis[1:] > 0))[0] + 1
    else:
        entries = np.where((basis[:-1] >= 0) & (basis[1:] < 0))[0] + 1

    trajectories = []
    for idx in entries:
        if idx + max_lag < len(basis):
            trajectories.append(basis[idx:idx+max_lag])
    if not trajectories:
        return np.full(max_lag, np.nan)
    return np.nanmean(trajectories, axis=0)

max_lag = 120
traj = {}
for name, df in [('Binance', binance), ('Bybit', bybit)]:
    traj[(name, 'contango')]      = compute_correction_curve(df, 'contango', max_lag)
    traj[(name, 'backwardation')] = compute_correction_curve(df, 'backwardation', max_lag)
    print(f"{name}: contango entries → peak={traj[(name,'contango')].max():.2f} bps at min {np.argmax(traj[(name,'contango')])}")
    print(f"{name}: backwardation entries → trough={traj[(name,'backwardation')].min():.2f} bps at min {np.argmin(traj[(name,'backwardation')])}")

fig, axes = plt.subplots(1, 2, figsize=(7, 3.5), sharey=False)

ax = axes[0]
ax.plot(range(max_lag), traj[('Binance', 'contango')], color=BLUE, linewidth=1.2, label='Binance')
ax.plot(range(max_lag), traj[('Bybit', 'contango')],   color=RED,  linewidth=1.2, label='Bybit')
ax.axhline(0, color=GREY, linewidth=0.5, linestyle='--')
ax.set_xlabel('Minutes after entry')
ax.set_ylabel('Basis (bps)')
ax.set_title('Contango Entry', fontsize=9, fontweight='bold')
ax.legend(fontsize=7)
clean_axes(ax)

ax = axes[1]
ax.plot(range(max_lag), traj[('Binance', 'backwardation')], color=BLUE, linewidth=1.2, label='Binance')
ax.plot(range(max_lag), traj[('Bybit', 'backwardation')],   color=RED,  linewidth=1.2, label='Bybit')
ax.axhline(0, color=GREY, linewidth=0.5, linestyle='--')
ax.set_xlabel('Minutes after entry')
ax.set_title('Backwardation Entry', fontsize=9, fontweight='bold')
ax.legend(fontsize=7)
clean_axes(ax)

fig.suptitle('Basis Correction Trajectories', fontsize=10, fontweight='bold', y=1.02)
fig.tight_layout()
fig.savefig(f'{CHART_DIR}/fig4_correction_trajectories.png')
plt.close()
print("  Saved fig4_correction_trajectories.png")

# ── Chart 5: MAB by volatility regime (H3) ──────────────────────────────────
fig, ax = plt.subplots(figsize=(5, 3.5))

labels_r = ['Low Volatility', 'High Volatility']
bin_r = [results_h3['Binance']['mab_low'], results_h3['Binance']['mab_high']]
byb_r = [results_h3['Bybit']['mab_low'],   results_h3['Bybit']['mab_high']]

x_r = np.arange(len(labels_r))
width_r = 0.32
b1 = ax.bar(x_r - width_r/2, bin_r, width_r, color=BLUE, label='Binance')
b2 = ax.bar(x_r + width_r/2, byb_r, width_r, color=RED,  label='Bybit')

for bars in [b1, b2]:
    for bar in bars:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, h + 0.05, f'{h:.2f}',
                ha='center', va='bottom', fontsize=7.5)

ax.set_ylabel('MAB (bps)')
ax.set_xticks(x_r)
ax.set_xticklabels(labels_r, fontsize=8)
ax.legend(fontsize=8)
clean_axes(ax)
ax.set_title('MAB by Volatility Regime', fontsize=10, fontweight='bold', pad=10)
fig.tight_layout()
fig.savefig(f'{CHART_DIR}/fig5_mab_by_regime.png')
plt.close()
print("  Saved fig5_mab_by_regime.png")

# ── Chart 6: Scatter — volatility vs MAB (H3) ──────────────────────────────
fig, ax = plt.subplots(figsize=(6, 4))

for name, df, color in [('Binance', binance, BLUE), ('Bybit', bybit, RED)]:
    daily_ret = df['log_ret'].resample('D').sum()
    vol_20d = daily_ret.rolling(20).std() * np.sqrt(365) * 100
    daily_mab = df['abs_basis_bps'].resample('D').mean()
    merged = pd.DataFrame({'vol': vol_20d, 'mab': daily_mab}).dropna()

    ax.scatter(merged['vol'], merged['mab'], alpha=0.25, s=8, color=color, label=name)
    z = np.polyfit(merged['vol'], merged['mab'], 1)
    xline = np.linspace(merged['vol'].min(), merged['vol'].max(), 100)
    ax.plot(xline, np.polyval(z, xline), color=color, linewidth=1.5, linestyle='-')

ax.set_xlabel('20-Day Annualised Volatility (%)')
ax.set_ylabel('Daily MAB (bps)')
ax.legend(fontsize=8)
clean_axes(ax)
ax.set_title('Volatility vs. Price Alignment', fontsize=10, fontweight='bold', pad=10)
fig.tight_layout()
fig.savefig(f'{CHART_DIR}/fig6_vol_vs_mab.png')
plt.close()
print("  Saved fig6_vol_vs_mab.png")


print("\n" + "="*80)
print("  ALL ANALYSIS COMPLETE")
print("="*80)
