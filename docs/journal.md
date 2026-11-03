## Phase 2 — Chaos simulator

Measured across 30 days (2026-10-05 to 2026-11-03), seed 42:
2,330 order rows, 5,747 line items, 500 customers snapshotted daily.

| Defect | Configured | Measured |
|---|---|---|
| Same order twice in one file | 3% | 82 orders |
| Order re-emitted next day | 3% | 50 orders |
| Late arrival | 5% | 155 rows (6.7%), lag 1-7 days |
| Negative quantity | 1% | 77 lines |
| Null email / phone | 2% / 8% | 1.0% / 6.4% |
| Header total != sum of lines | 2% | 55 orders (2.5%) |
| Schema drift | off | enabled in Phase 11 |
| Customers who changed region | 15% | 79 of 500 |

Decisions:
- No wall-clock timestamps anywhere. Regenerating a single day produced a
  byte-identical file to the backfill's, verified by checksum.
- Simulator is stateless; it never reads yesterday's output.
- `updated_at` tracks the version's effective date, not the batch date.
  Otherwise dbt snapshot would record a new version daily for every customer.
- `unit_cost` sits on the order line, not the dimension, so margin does not
  restate when a product is repriced.

Fixed during the phase: the first run produced only 5 region changes because
moves were spread over four years. Added `history_anchor` / `history_days` so
moves cluster inside the generated window, plus a test that fails below 5%.