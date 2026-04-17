# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**XRP Grid Brain** is a regime-aware grid trading decision engine for XRP/USD. It detects market regimes via Gaussian Mixture Model clustering, applies policy rules, and generates ranked grid trading candidates for downstream execution.

## Running the Project

```bash
# Activate virtual environment first
source .venv/bin/activate

# Single run — generates latest grid decision
python scripts/run.py

# Full lifecycle (data fetch → decision → eval → reporting → signal)
bash scripts/run_cycle.sh

# Backtest/replay historical decisions
python scripts/backtest_replay.py
```

There is no test suite or linter configured. Validation is done via backtesting in `scripts/evaluate.py`.

## Architecture: 5-Stage Pipeline

```
OHLCV CSV → Features → GMM Clusters → Policy → Grid Variants → Scored Output
```

**Stage 1 — Data loading** (`src/io/loaders.py`): Reads `data/raw/xrp_full_hourly_clean.csv`, normalizes columns, returns sorted DataFrame.

**Stage 2 — Feature engineering** (`src/features/regime_features.py`): Computes 15 rolling-window features — returns (1h/6h/12h/24h), volatility (std), trend (ATR, MAs, slopes), mean reversion (z-score), range structure (high/low width, position), and range expansion.

**Stage 3 — Regime detection** (`src/models/gmm_regime.py`): Fits a 5-component GMM (scikit-learn) on StandardScaler-normalized features. Outputs cluster ID (0–4) + confidence probability per bar.

**Stage 4 — Policy mapping** (`src/policy/regime_policy.py`): Maps cluster → operational regime and risk mode:
- Cluster 0 → `RANGE_GOOD` (ideal for grid)
- Cluster 1 → `RANGE_TREND_UP`
- Cluster 2 → `RANGE_TREND_DOWN`
- Cluster 3 → `TREND`
- Cluster 4 → `NO_TRADE`
- Risk mode: `OFF` (<55% confidence), `TRANSITION` (55–70%), `NORMAL` (>70%)

**Stage 5 — Grid building** (`src/grid/candidate_builder.py`): For each bar, generates 3 variants (tight ×0.90, base ×1.00, wide ×1.12). Grid width = ATR × regime_multiplier × width_scalar. Always 6 levels. Center = close × (1 − 0.003). Candidates scored on 8 factors (profitability, spacing, confidence, regime bonus, activity, edge penalty, ATR alignment) and filtered by `est_profit_per_level > 0.003`.

**Pipeline orchestration:** `src/pipelines/run_brain.py` runs stages 1–4; `src/pipelines/run_grid.py` runs stage 5 over a 24-hour lookback and exports the best candidate.

## Key Constants

| Constant | Value | Location |
|---|---|---|
| `FEE_PCT` | 0.002 | `src/grid/candidate_builder.py` |
| `LOOKBACK_HOURS` | 24 | `src/pipelines/run_grid.py` |
| `TOP_N_EXPORT` | 15 | `src/pipelines/run_grid.py` |
| `WARMUP_BARS` | 200 | `src/pipelines/run_brain.py` |
| GMM components | 5 | `src/models/gmm_regime.py` |
| Grid levels | 6 | `src/grid/candidate_builder.py` |

## Outputs

| File | Contents |
|---|---|
| `outputs/latest_decision.json` | Best grid candidate (JSON) |
| `outputs/latest_decision.csv` | Best grid candidate (tabular) |
| `outputs/candidate_pool_latest.csv` | Top 15 scored candidates |
| `outputs/decision_history.csv` | Appended historical best decisions |
| `outputs/candidate_pool_history.csv` | Appended historical candidates |
| `outputs/evaluation_history.csv` | 24h performance evaluations |
| `outputs/lifecycle_decision.csv` | Active grid management state |
| `outputs/opentrader_signal.json` | Signal for execution layer |

## Full Lifecycle Scripts

`scripts/run_cycle.sh` runs these in sequence:
1. `scripts/update_data.py` — fetches latest OHLCV from Kraken API
2. `scripts/run.py` — generates latest decision
3. `scripts/evaluate.py` — evaluates prior decisions vs realized prices
4. `scripts/eval_summary.py` — summary stats by regime bucket
5. `scripts/lifecycle.py` — manages active grid state / entry-exit signals
6. `scripts/build_opentrader_signal.py` — formats signal for OpenTrader
7. `scripts/update_google_sheet.py` — pushes to monitoring dashboard
