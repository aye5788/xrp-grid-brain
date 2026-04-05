# xrp-grid-brain
# Grid Brain

Regime-aware grid trading decision engine.

Pipeline:
1. Load market data
2. Generate regime features
3. Detect regimes (GMM clustering)
4. Map to policy
5. Generate grid candidates

Outputs:
- Regime brain dataset
- Grid candidate setups

Execution layer (OpenTrader) will consume outputs.
