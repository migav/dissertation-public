# Bitcoin Perpetual Futures — Funding Rate Efficiency Analysis

Replication code and data for the dissertation:

> **"Evaluating the Efficiency of the Funding Rate Mechanism in Bitcoin Perpetual Futures on Binance vs Bybit"**  
> BSc Business Management with Finance, University of Birmingham Dubai, 2026

---

## Repository Structure

```
btc-funding-rate-analysis/
├── collect_binance_funding_rate.js   # Pulls historical BTC-USDT perp data from Binance API
├── collect_bybit_funding_rate.js     # Pulls historical BTC-USDT perp data from Bybit API
├── analysis.py                       # All statistical tests and chart generation (Python)
├── package.json                      # Node.js dependencies (data collection)
├── requirements.txt                  # Python dependencies (analysis)
│
├── connector/                        # Exchange API connector layer (Node.js)
│   ├── futuresExchanges/             #   BinancePerp.js, BybitPerp.js
│   └── spotExchanges/                #   BinanceSpot.js, Bybit.js
│
├── modules/                          # Shared Node.js modules
│   ├── DB.js                         #   SQLite read/write helpers
│   └── FundingRateHistory.js         #   Batch history fetcher
│
├── data/
│   └── funding_rate.db               # SQLite database (~253 MB) — see note below
│
└── charts/                           # Pre-generated output figures (PNG)
    ├── fig1_rolling_mab.png
    ├── fig2_basis_distribution.png
    ├── fig3_halflife_directional.png
    ├── fig4_correction_trajectories.png
    ├── fig5_mab_by_regime.png
    └── fig6_vol_vs_mab.png
```

---

## Data

The SQLite database (`data/funding_rate.db`) contains **~2.1 million minute-level observations** of BTC-USDT perpetual futures from Binance and Bybit, spanning **30 January 2024 – 26 January 2026**.

### Database schema

```sql
CREATE TABLE snapshots (
    id                   INTEGER PRIMARY KEY,
    exchange             TEXT,          -- 'binance' or 'bybit'
    pair                 TEXT,          -- 'BTC-USDT'
    timestamp            INTEGER,       -- Unix ms
    spotPrice            REAL,
    futuresPrice         REAL,
    indexPrice           REAL,
    premiumIndex         REAL,
    estimatedFundingRate REAL,          -- real-time estimated rate
    realisedFundingRate  REAL,          -- actual settlement rate (every 8 h)
    UNIQUE(exchange, pair, timestamp)
);
```

> **Note on file size:** `funding_rate.db` is ~253 MB and is tracked via [Git LFS](https://git-lfs.github.com/). Run `git lfs pull` after cloning to download it. Alternatively, you can re-collect the data yourself using the collection scripts below.

---

## Reproducing the Results

### 1. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 2. Run the analysis

```bash
python analysis.py
```

This will:
- Load data from `data/funding_rate.db`
- Run all hypothesis tests (ADF, Johansen cointegration, AR(1) half-life, Granger causality, Mann-Whitney U)
- Print all numerical results to stdout (tables for H1, H2, H3)
- Save all 6 figures to `charts/`

Expected runtime: ~3–5 minutes on a modern laptop.

---

## Re-collecting the Data (optional)

If you want to rebuild the database from scratch using the exchange APIs:

### Install Node.js dependencies

```bash
npm install
```

### Collect Bybit data

```bash
node collect_bybit_funding_rate.js
```

### Collect Binance data

```bash
node collect_binance_funding_rate.js
```

Both scripts poll the official public REST APIs with no authentication required. They write to `data/funding_rate.db`, resuming from the last stored timestamp if the database already exists. The full collection takes several hours due to API rate limits.

---

## Hypotheses Tested

| # | Hypothesis | Method |
|---|---|---|
| H1 | Basis mean-reverts significantly faster on Binance than Bybit | AR(1) half-life, Granger causality, rolling MAB |
| H2 | Funding rate mechanism exhibits directional asymmetry (contango vs backwardation) differing across exchanges | Split-sample AR(1), Mann-Whitney U |
| H3 | Market volatility significantly moderates mechanism efficiency, with deterioration differing between exchanges | Volatility regime split, MAB comparison, half-life by regime |

---

## Dependencies

### Python (`requirements.txt`)

| Package | Purpose |
|---|---|
| pandas | Data loading and manipulation |
| numpy | Numerical computation |
| scipy | Statistical tests (Mann-Whitney U, t-tests, linregress) |
| statsmodels | ADF, Granger causality, Johansen cointegration |
| matplotlib | Chart generation |

### Node.js (`package.json`)

| Package | Purpose |
|---|---|
| sqlite3 | SQLite database driver |
| bybit-api | Bybit REST API client |
| axios | HTTP client for Binance REST API |

---

## License

MIT
