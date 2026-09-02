# NIFTY + BANKNIFTY Index Money Flow Engine v1.0

Separate Railway collector for NIFTY and BANKNIFTY.

## Default logic
- 09:18 IST baseline.
- For each index, look near spot and select the strike with the highest combined CE+PE traded value as the **Money-Flow ATM**.
- Freeze 3 OTM calls above that ATM and 3 OTM puts below it.
- Write 3-minute option, futures/OI, PCR/IV and futures aggression snapshots to separate Neon tables.
- Preserve the user's 50% OI-side comparison as `oi_50pct_state`.

## Neon tables
- `index_money_flow_universe`
- `index_engine_snapshots`
- `index_option_snapshots`
- `index_futures_aggression_snapshots`

## Railway variables
Copy `.env.example` values into Railway Variables. Use the same `UPSTOX_TOKEN` and `NEON_DATABASE_URL` as your existing services.

## Start command
`python index_money_flow_engine.py`
