# Arbitrage Configurability and Discord Alerts Implementation Plan

**Goal:** Turn arbitrage into a preset-driven, highly configurable scanner and alerting system with practical recommended defaults, explicit cross-DC behavior, rich Discord card alerts, and a mechanical plan-compliance gate before push/deploy.

**Architecture:** Extend the existing profile arbitrage settings, scanner query, opportunity model, digest state, and alert delivery pipeline. Preserve the current changed-only digest behavior, but replace the Discord presentation with structured cards. Batch the already-local table-header tooltip fix and `AGENTS.md` plan-compliance documentation with this work.

---

## Phase 0: Plan Compliance Setup

- Before code changes, create a checklist mapping every requirement in this plan to implementation location, verification command/search, and status.
- Keep the checklist current during implementation.
- Before any push/deploy, reread this document and verify every item is mapped.
- Do not push if any item is unmapped, deferred, or blocked without explicit user approval.

---

## Phase 1: Schema and API Foundations

Add migrations and SeaORM entity fields for new settings.

### Profile arbitrage settings

| Field | Type | Default | Meaning |
|---|---:|---:|---|
| `preset_name` | string | `BALANCED` | Active preset name; becomes `CUSTOM` after manual edits |
| `destination_world_scope` | string | `HOME_WORLD` | Where the scanner may sell |
| `seller_world_ids` | json | `[]` | Explicit worlds where the user can sell via alts/retainers |
| `weekly_velocity_threshold` | double | `0.0` | Minimum average units/day over 7 days; disabled at 0 |
| `same_dc_travel_minutes` | integer | `2` | Same-DC travel cost minutes |
| `cross_dc_travel_minutes` | integer | `8` | Cross-DC travel cost minutes |
| `reference_price_scope` | string | `DESTINATION_DC` | Market scope used for min/avg reference prices |
| `sell_price_strategy` | string | `LOWER_OF_ASK_AND_MEDIAN` | Price used for sell-side profit |
| `min_markdown_pct` | double | `0.0` | Minimum markdown vs reference/target; disabled at 0 |
| `digest_format` | string | `CARDS` | Discord output format |
| `digest_changed_only` | bool | `true` | Suppress previously delivered unchanged rows |
| `digest_max_clean` | integer | `8` | Max clean cards per digest |
| `digest_max_review` | integer | `4` | Max review cards per digest |
| `digest_include_review` | bool | `true` | Include volatile/theoretical review cards |
| `digest_include_universalis_links` | bool | `true` | Include Universalis item links |
| `digest_include_ultros_links` | bool | `true` | Include Ultros item links |

### Separate table and alert configuration

Table behavior and alert behavior must be configurable separately. Do not reuse one setting when the user may reasonably want different behavior while browsing the table versus receiving Discord alerts.

Add separate table settings:

| Field | Type | Default | Meaning |
|---|---:|---:|---|
| `table_grouping_strategy` | string | `BEST_PLUS_SAME_DC` | How duplicate item/HQ opportunities are grouped for the table |
| `table_max_rows_per_item` | integer | `2` | Maximum table rows for the same item/HQ |
| `table_include_same_dc_best` | bool | `true` | Include the best same-DC option in addition to best overall when different |
| `table_show_theoretical` | bool | `false` | Whether theoretical sell-target rows are shown in the table |

Add separate alert settings:

| Field | Type | Default | Meaning |
|---|---:|---:|---|
| `alert_grouping_strategy` | string | `BEST_PLUS_SAME_DC` | How duplicate item/HQ opportunities are grouped for alerts |
| `alert_max_rows_per_item` | integer | `2` | Maximum alert rows for the same item/HQ |
| `alert_include_same_dc_best` | bool | `true` | Include the best same-DC option in addition to best overall when different |
| `alert_show_theoretical` | bool | `false` | Whether theoretical rows are eligible for alerts |
| `alert_profit_improvement_threshold_gil` | bigint | `1` | Minimum net-profit improvement needed to re-alert the same item/HQ |
| `alert_profit_improvement_threshold_pct` | double | `0.0` | Optional percent improvement needed to re-alert; disabled at 0 |
| `alert_frequency_mode` | string | `DIGEST_INTERVAL` | Alert cadence: immediate, interval digest, scanner-complete digest, or scheduled digest |
| `alert_digest_interval_minutes` | integer | `60` | Interval digest cadence when `alert_frequency_mode = DIGEST_INTERVAL` |
| `alert_schedule_cron` | string/null | null | Optional cron-like schedule when `alert_frequency_mode = SCHEDULED` |
| `alert_send_empty_digest` | bool | `false` | Whether to send "no changed opportunities" digests |
| `alert_immediate_threshold_enabled` | bool | `true` | Allow urgent immediate alerts even when normal cadence is digest/interval based |
| `alert_immediate_min_net_profit` | bigint | `500000` | Net-profit threshold that triggers an immediate alert |
| `alert_immediate_min_markdown_pct` | double | `0.0` | Optional markdown threshold for immediate alerts; disabled at 0 |
| `alert_immediate_min_velocity` | double | `0.0` | Optional current-velocity threshold for immediate alerts; disabled at 0 |
| `alert_immediate_max_per_hour` | integer | `3` | Rate limit for immediate arbitrage pings per profile |

### Arbitrage notification destinations

Add a profile-level join table for arbitrage digest destinations instead of sending to every endpoint implicitly:

| Field | Type | Meaning |
|---|---:|---|
| `profile_id` | integer | Profile that owns the arbitrage digest configuration |
| `endpoint_id` | integer | Existing `notification_endpoint` row selected for arbitrage alerts |

Rules:

- Reuse the existing notification endpoint model and Discord channel picker used by the Alerts tab.
- Existing profiles with no explicit arbitrage destination rows should default to the current behavior during migration/backfill: all profile endpoints remain eligible until the user saves a narrowed selection.
- After a user saves arbitrage destinations, only selected endpoints receive arbitrage digests.
- Legacy `alert_channel_webhook` and `alert_channel_dm` remain fallback-only for profiles without notification endpoints.
- Test-send and preview must use the same selected endpoint/channel list as real digest delivery.

### Opportunity and digest state

Add matching opportunity fields:

- `dest_low_ask_price`
- `selected_sell_reference_price`
- `source_ask_avg`
- `dest_ask_avg`
- `reference_min_price`
- `reference_avg_price`
- `markdown_pct`
- `execution_status`
- `travel_minutes`

Add these fields to `arbitrage_digest_state` where they affect changed-only alert behavior. Snapshot hashing must include source ask, destination ask, selected sell reference, net profit, quantity, volatility flag, latest sale timestamp, sales counts, median/recent sale metrics, weekly velocity, markdown percent, execution status, and reference min/avg.

Add item-level alert memory so Discord does not resend lower-profit alternate-source rows for the same item:

| Field | Type | Meaning |
|---|---:|---|
| `profile_id` | integer | Profile that received the alert |
| `item_id` | integer | Item identity |
| `hq` | bool | Quality bucket |
| `best_alerted_net_profit` | bigint | Highest net profit previously delivered for this item/HQ |
| `best_alerted_snapshot_hash` | string | Snapshot hash for the delivered best item/HQ opportunity |
| `last_alerted_at` | timestamp | Delivery time |

Discord resend rule: a new opportunity for the same profile + item + HQ is alert-eligible only if its grouped alert row has higher net profit than `best_alerted_net_profit` by at least the configured gil and percent thresholds, or if digest history is reset.

Add alert scheduling state so interval/scheduled digests do not resend continuously:

| Field | Type | Meaning |
|---|---:|---|
| `profile_id` | integer | Profile whose arbitrage alert cadence is tracked |
| `last_digest_sent_at` | timestamp/null | Last interval/scheduled digest delivery time |
| `last_immediate_sent_at` | timestamp/null | Last immediate alert delivery time |
| `immediate_sent_count_window_start` | timestamp/null | Start of current immediate rate-limit window |
| `immediate_sent_count` | integer | Immediate alerts sent in the current window |

Add a pending digest queue for interval/scheduled delivery:

| Field | Type | Meaning |
|---|---:|---|
| `id` | integer | Primary key |
| `profile_id` | integer | Profile that owns the pending row |
| `item_id` | integer | Item identity |
| `hq` | bool | Quality bucket |
| `source_world_id` | integer | Source world for the grouped row |
| `dest_world_id` | integer | Destination world for the grouped row |
| `snapshot_hash` | string | Snapshot queued for future digest |
| `net_profit` | bigint | Profit used for ordering and improvement checks |
| `section` | string | `CLEAN`, `REVIEW`, or `THEORETICAL` |
| `queued_at` | timestamp | First time this snapshot became eligible |
| `updated_at` | timestamp | Last time this pending snapshot changed |

Pending queue rules:

- Interval/scheduled modes enqueue eligible grouped alert rows when cadence is not due.
- Queue key is `profile_id + item_id + hq + source_world_id + dest_world_id`.
- If a higher-profit grouped row for the same item/HQ appears before the digest is due, replace the lower-profit pending row for that item/HQ according to alert grouping rules.
- Immediate-threshold alerts update delivered alert memory immediately and must not be sent again in the later digest unless they improve again.

Add per-endpoint delivery attempts:

| Field | Type | Meaning |
|---|---:|---|
| `id` | integer | Primary key |
| `profile_id` | integer | Profile that attempted delivery |
| `endpoint_id` | integer/null | Notification endpoint, null only for legacy fallback webhook/DM |
| `delivery_kind` | string | `DIGEST`, `IMMEDIATE`, `TEST`, or `PREVIEW_SEND` |
| `snapshot_batch_hash` | string | Hash identifying the delivered batch |
| `success` | bool | Whether this endpoint accepted the message |
| `error_message` | string/null | Failure reason if delivery failed |
| `attempted_at` | timestamp | Attempt time |

Delivery success rules:

- Mark opportunity/digest snapshots delivered only for the successful batch after at least one selected endpoint succeeds.
- Record every selected endpoint attempt, including failures.
- If all selected endpoints fail, keep pending rows queued and do not update delivered snapshot or best-alerted profit.
- If some endpoints fail, show/log partial failure while still updating delivered state for the successful batch.

### API shapes

Extend existing profile settings APIs and add arbitrage-specific endpoints:

- `GET /api/v1/profiles/{id}/settings/arbitrage`
  - returns the full settings object.
- `PUT /api/v1/profiles/{id}/settings/arbitrage`
  - accepts the full settings object plus selected endpoint ids.
  - validates ownership and enum values.
  - queues scanner after save.
- `GET /api/v1/profiles/{id}/settings/arbitrage/destinations`
  - returns available notification endpoints and selected arbitrage destination endpoint ids.
- `PUT /api/v1/profiles/{id}/settings/arbitrage/destinations`
  - persists selected arbitrage destination endpoint ids.
- `POST /api/v1/profiles/{id}/settings/arbitrage/apply-preset`
  - body: `{ "preset_name": "CONSERVATIVE" | "BALANCED" | "AGGRESSIVE" }`
  - applies preset defaults and returns updated settings.
- `POST /api/v1/profiles/{id}/settings/arbitrage/preview`
  - body may override settings for preview only.
  - returns the exact summary/card DTOs that Discord delivery would render, without sending.
- `POST /api/v1/profiles/{id}/settings/arbitrage/test`
  - sends synthetic card-style test digest to selected arbitrage endpoints.
- `POST /api/v1/profiles/{id}/settings/arbitrage/reset-delivery-state`
  - clears digest state, item-level alert memory, pending queue, and scheduling counters for the profile.
- `GET /api/v1/profiles/{id}/arbitrage/status`
  - returns scanner progress.
- `GET /api/v1/profiles/{id}/arbitrage/alert-status`
  - returns scanner progress plus cadence state: next digest due time, pending rows, last digest time, immediate rate-limit counters, and last endpoint failures.

Security and ownership rules:

- Every endpoint id in arbitrage destination selection must belong to the authenticated Discord user.
- Discord channel endpoints must pass the existing bot-postability/admin validation used by the endpoint system.
- Profiles cannot select endpoints owned by another user.
- Test-send and reset require ownership of the profile.

### Migration details

- Migration must add all new columns with non-null defaults where possible so existing rows remain valid.
- Backfill existing settings:
  - `preset_name = CUSTOM` for profiles whose current values do not match the new Balanced defaults.
  - `preset_name = BALANCED` only for profiles with unset/zero legacy arbitrage gates.
  - `destination_world_scope = HOME_WORLD` when `require_home_world_sell_target = true`.
  - `destination_world_scope = ACTIVE_DC` when `require_home_world_sell_target = false`.
- Add unique constraints:
  - arbitrage destination join: `profile_id + endpoint_id`
  - item-level alert memory: `profile_id + item_id + hq`
  - alert scheduling state: `profile_id`
  - pending queue: `profile_id + item_id + hq + source_world_id + dest_world_id`
- Add indexes:
  - `active_listing(world_id, item_id, hq, timestamp, price_per_unit)`
  - `sale_history(world_id, sold_item_id, hq, sold_date, price_per_item)`
  - pending queue by `profile_id, net_profit DESC`
  - delivery attempts by `profile_id, attempted_at DESC`
- Down migrations must drop new indexes/tables before dropping columns.

---

## Phase 2: Presets and Validation Rules

Implement presets as recommended defaults, while allowing power users to tune every field.

| Preset | Min Net | Min Gross | Current Velocity | Weekly Velocity | Source Scope | Destination Scope | Travel Cost | Volatility |
|---|---:|---:|---:|---:|---|---|---:|---|
| Conservative | 250,000 | 250,000 | 2.0 | 1.0/day | Same DC | Home world | 15,000/min | Suppress |
| Balanced | 100,000 | 100,000 | 1.0 | 0.0/day | Same DC | Home world | 10,000/min | Demote to review |
| Aggressive | 50,000 | 50,000 | 0.5 | 0.0/day | Same region | Active DC | 5,000/min | Alert with warning |

Rules:

- New profiles use `BALANCED`.
- Applying a preset overwrites all preset-controlled fields.
- Editing any preset-controlled field sets `preset_name = CUSTOM`.
- `Configured seller worlds` requires at least one world.
- Duplicate seller worlds are removed on save.
- Seller worlds outside the user's region are rejected unless destination scope is `SAME_REGION_THEORETICAL`.
- `SAME_REGION_THEORETICAL` rows are review-only by default and must not be fast-lane alerts.
- Destination scope precedence:
  1. `HOME_WORLD` uses only profile home world.
  2. `ACTIVE_DC` uses all worlds in active data center.
  3. `CUSTOM` uses configured seller worlds only.
  4. `SAME_REGION` uses all worlds in the home-world region and marks non-seller destinations theoretical where appropriate.
- Same-DC means the destination/sell data center for the profile:
  - home-world DC when destination scope is `HOME_WORLD` or `CUSTOM`,
  - active DC when destination scope is `ACTIVE_DC`,
  - home-world region grouping for `SAME_REGION_THEORETICAL`.
- A same-DC fallback row is one whose source world is in the same DC as the destination/sell world.

Reference pricing rules:

- `reference_price_scope = DESTINATION_WORLD`: reference min/avg are computed from the destination world only.
- `reference_price_scope = DESTINATION_DC`: reference min/avg are computed across all worlds in the destination DC.
- `reference_price_scope = ACTIVE_REGION`: reference min/avg are computed across all worlds in the active/home-world region.
- Reference min uses current low asks.
- Reference avg uses recent sale prices over the last 7 days when available.
- If 7-day sales are unavailable, reference avg falls back to current ask cluster average.
- If both sales and ask-cluster data are unavailable, the row is excluded from markdown filtering but may still be shown with markdown as unavailable when `min_markdown_pct = 0`.
- Markdown percent formula: `(reference_price - selected_sell_reference_price) / reference_price * 100`.
- If reference price is zero or unavailable, markdown is null and does not pass any positive `min_markdown_pct`.

Sell price strategy rules:

- `LOWER_OF_ASK_AND_MEDIAN`: selected sell reference is `min(destination low ask, 48h median sale price)`.
- `DESTINATION_LOW_ASK`: selected sell reference is destination low ask.
- `MEDIAN_SALE`: selected sell reference is 48h median sale price; row is excluded if median is unavailable.
- Default remains `LOWER_OF_ASK_AND_MEDIAN`.

---

## Phase 3: Scanner and Opportunity Logic

Extend world resolution and scanner filtering.

- Resolve source worlds from `source_world_scope`: home world, same DC, or same region.
- Resolve destination worlds from `destination_world_scope`.
- Classify each row with `execution_status`:
  - `EXECUTABLE`
  - `CROSS_DC_TRAVEL_REQUIRED`
  - `THEORETICAL_SELL_TARGET`
- Use configurable same-DC and cross-DC travel minutes in net-profit deduction.
- Apply `weekly_velocity_threshold` after current velocity filtering.
- Compute destination low ask separately from selected sell reference.
- Compute source/destination ask averages from low ask clusters.
- Compute reference min/avg from the configured reference scope.
- Compute markdown percent against the configured reference/target price.
- Apply `min_markdown_pct` when greater than 0.
- Keep over-budget rows visible but flagged.
- Never fast-lane volatile or theoretical rows.

Apply duplicate item grouping after scanner gates and before table/API output:

- Group opportunities by `profile_id + item_id + hq`.
- For each group, select the highest net-profit opportunity as `BEST_OVERALL`.
- Also select the highest net-profit same-DC opportunity as `BEST_SAME_DC` when it is different from `BEST_OVERALL`.
- If `BEST_OVERALL` is already same-DC, include only that one row.
- Do not include more than the configured per-surface row limit for the same item/HQ.
- Apply table grouping settings for API/table output.
- Apply alert grouping settings separately for Discord/digest output.
- Default result: at most two rows per item/HQ, one best overall and one best same-DC fallback.

Apply alert frequency after alert grouping:

- `IMMEDIATE`: send eligible changed/improved rows right after scanner completion, subject to immediate threshold and rate limit.
- `DIGEST_INTERVAL`: accumulate eligible changed/improved rows and send only when `alert_digest_interval_minutes` has elapsed since the last digest.
- `SCANNER_COMPLETE_DIGEST`: send one digest after each scanner completion when eligible rows exist.
- `SCHEDULED`: send eligible accumulated rows only when the configured schedule is due.
- Independently of the normal frequency mode, if `alert_immediate_threshold_enabled` is true, send an urgent immediate alert for any eligible grouped alert row that meets the immediate net-profit threshold and any enabled immediate markdown/velocity thresholds.
- Never send an empty digest unless `alert_send_empty_digest` is enabled.
- Changed-only and profit-improvement rules still apply inside every frequency mode.
- Immediate threshold alerts still obey changed-only, same-item profit-improvement, destination selection, and per-hour rate limits.
- Immediate alerts update item-level alert memory and delivered snapshot state immediately after successful delivery.
- Immediate alerts are not included in later interval/scheduled digests unless the same item/HQ improves again above configured thresholds.
- Profile item cooldown applies per item/HQ across both immediate and digest modes; immediate threshold alerts can bypass the normal digest cadence but not the per-item cooldown unless a future setting explicitly enables cooldown bypass.
- Scheduled mode uses a simple cron expression interpreted in the profile/user timezone when known; fallback timezone is UTC.
- If `alert_schedule_cron` is invalid, saving settings fails with a validation error.

Performance safeguards:

- Add/verify indexes that support world/item/HQ/timestamp lookups for active listings and sale history.
- Log candidate counts, filtered counts, scope sizes, and scan duration.
- Cap theoretical same-region scans behind the explicit destination scope only.
- If scanner runtime exceeds the existing loop interval, status should show that the scanner is still running instead of queueing overlapping scans.

Phased local checkpoints:

- Checkpoint A: migrations/entities/API types compile; no scanner behavior changes.
- Checkpoint B: scanner computes new fields and grouping correctly; no Discord card delivery yet.
- Checkpoint C: settings UI saves/loads new config and preview/reset/test endpoints work locally.
- Checkpoint D: Discord card delivery and cadence modes work in tests/local smoke.
- Deploy only once after all checkpoints pass unless the user explicitly approves a partial deploy.

---

## Phase 4: Settings UI

Rework Arbitrage settings into preset-first sections.

- Add preset segmented control: Conservative, Balanced (Recommended), Aggressive, Custom.
- Add "Restore preset defaults" action.
- Add advanced sections:
  - Execution Scope
  - Profit Gates
  - Liquidity and Velocity
  - Reference Pricing
  - Volatility and Review
  - Item and World Filters
  - Table Display
  - Digest and Alert Delivery
- Advanced sections must be collapsible, with a compact summary row for each section showing the active preset/default or the number of custom overrides.
- Validation errors must appear next to the setting that caused them and block save when the server would reject the value.
- The page must remain usable on mobile: settings grids collapse to one column, controls retain stable sizes, and long labels/help text wrap without overlap.
- Surface existing hidden filters: category allow/block lists, world exclusions, item exclusions, max listing age, stale panel.
- Add independent Table Display controls for grouping strategy, max rows per item, same-DC fallback inclusion, and theoretical row visibility.
- Add arbitrage alert destination selection using the same endpoint/channel UX as the Alerts tab:
  - selectable Discord DM endpoints,
  - selectable Discord channel endpoints,
  - selectable webhook endpoints,
  - create/test/remove endpoint flow available from the arbitrage settings section,
  - clear indication of which endpoints will receive arbitrage digests.
- Add independent Alert controls for grouping strategy, max rows per item, same-DC fallback inclusion, theoretical alert eligibility, changed-only delivery, profit-improvement re-alert thresholds, digest limits, and link inclusion.
- Add Alert Frequency controls:
  - immediate,
  - every X minutes,
  - after every scanner completion,
  - scheduled digest,
  - immediate threshold toggle,
  - immediate net-profit threshold,
  - optional immediate markdown threshold,
  - optional immediate velocity threshold,
  - immediate per-hour rate limit,
  - empty digest toggle.
- Every setting must show:
  - visible info icon tooltip,
  - native `title`,
  - recommended default,
  - implication/tradeoff text.
- Arbitrage tables keep dense native `title` attributes only; no info icons in table cells or headers.
- Add a digest history reset button for the active profile.
- Add alert preview/test-send controls:
  - preview card format without sending,
  - send test digest to selected endpoint/channel.

---

## Phase 5: Discord Card Delivery

Replace the arbitrage Discord digest body with structured card output while preserving non-Discord fallback.

Discord behavior:

- Send one summary embed first:
  - profile name,
  - changed deal count,
  - clean/review/theoretical counts,
  - top net profit,
  - active preset,
  - scan timestamp.
- Send one card embed per opportunity.
- Group same-item/HQ opportunities before sending cards using alert grouping settings; by default send only best overall plus best same-DC fallback when different.
- Do not resend a new opportunity for the same item/HQ solely because the source world changed; resend only when the grouped alert row beats the previously alerted best net profit by the configured improvement threshold.
- Respect alert frequency mode before delivery; interval/scheduled digests should accumulate eligible rows until their cadence is due.
- Send urgent immediate threshold cards as soon as scanner completion finds an eligible grouped row, even when normal digest cadence is interval or scheduled.
- Card title: `Deal Alert: {item name} ({HQ/NQ} #{item_id})`.
- Card URL: Universalis link when enabled; otherwise Ultros item link when enabled.
- Fields:
  - Profit potential
  - Markdown
  - Sale velocity
  - Source market: world, min, avg
  - Sell target: world, min, avg
  - Reference market: scope, min, avg
  - Quantity and total cost
  - Risk and execution status
- Card colors:
  - green for clean executable rows,
  - amber for volatile/review rows,
  - orange/red for theoretical or high-risk rows.
- Include Discord timestamp on each card using `computed_at` or latest sale/listing timestamp.

Discord limits:

- Max 10 embeds per Discord message.
- Max 25 fields per embed.
- Max 6,000 total characters per message.
- Max 4,096 description characters per embed.
- Max 1,024 characters per field value.
- Chunk into multiple messages when needed.
- Send no more than one summary message plus enough card chunks to satisfy `digest_max_clean` and `digest_max_review`.
- If a card would exceed limits, shorten optional fields before dropping the card.
- Add an omitted-count summary when clean/review rows exceed configured max.

Fallback behavior:

- Non-Discord endpoints receive the existing plain-text summary format.
- Webhooks receive structured embeds.
- Discord DM/channel endpoints receive structured embeds through Serenity.
- Deliver only to the arbitrage-selected endpoint/channel list once configured.
- Do not mark digest snapshots delivered unless at least one configured destination succeeds.

---

## Phase 6: Observability and User Feedback

- Scanner status should show selected preset, source scope, destination scope, scan phase, profile progress, candidate count, accepted count, and alert count.
- Settings changes should immediately mark scanner as queued.
- Arbitrage table should show loading/progress while a scan is running.
- Scanner status/logs should show duplicate grouping counts: raw accepted rows, grouped table rows, grouped alert rows, omitted lower-profit duplicates, and omitted lower-profit same-item alert candidates.
- Status/logs should show alert cadence decisions: frequency mode, next digest due time, accumulated eligible row count, immediate rate-limit count, and skipped-empty-digest count.
- Status/logs should show immediate-threshold decisions: rows checked, rows passing threshold, rows blocked by changed-only/profit-improvement rules, and rows blocked by rate limit.
- Logs should include:
  - profile id,
  - preset,
  - source/destination scopes,
  - candidates before filters,
  - rows filtered by each gate,
  - digest delivery count,
  - delivery failure reason.

---

## Phase 7: Verification and Deployment Gate

Required checks:

- `cargo fmt --all`
- `cargo check -p ultros-app --target wasm32-unknown-unknown --no-default-features --features hydrate`
- Git Bash `./check_ci.sh`
- Run E2E if routing, hydration, settings UI, or alert preview UI changes materially.

Manual smoke:

- Settings page shows all new fields, tooltips, defaults, and preset behavior.
- Table and alert configuration controls are separate and saving one does not silently overwrite the other.
- Arbitrage alert destination selector shows Discord DM/channel/webhook endpoints and persists selected channels.
- Saving settings queues scanner and updates progress state.
- Arbitrage table shows execution status, markdown/reference data, dual velocity, and table header `title` tooltips.
- For the same item/HQ with multiple source worlds, the table defaults to at most best overall plus best same-DC fallback.
- Discord preview renders card-style alert.
- Test digest sends only to selected Discord endpoints/channels.
- First digest sends new rows.
- Unchanged rows are suppressed when changed-only is enabled.
- Same-item/HQ Discord alerts are not resent for lower-profit alternate source worlds.
- Same-item/HQ Discord alerts resend when the grouped alert row has higher net profit than previously alerted by the configured threshold.
- Immediate mode sends after scanner completion when eligible rows exist and rate limits allow it.
- Immediate threshold option sends urgent alerts for rows above the configured threshold even when normal alert frequency is interval/scheduled.
- Immediate threshold respects changed-only and profit-improvement rules so lower-profit same-item/source variations do not spam Discord.
- Interval mode does not send before the configured interval elapses, then sends accumulated eligible rows.
- Scanner-complete digest mode sends once per completed scan with eligible rows.
- Scheduled mode sends only when the schedule is due.
- Empty digests are suppressed by default.
- Changed ask/reference/sale summary resends the row.
- Reset digest history causes previous rows to send again.
- Cross-DC rows are labeled correctly.
- Theoretical rows appear only under explicit theoretical destination scope and route to review.

Before push/deploy:

- Produce the plan-compliance checklist as a final report section.
- Stage only intended files.
- Commit only after checks pass.
- Push to GitHub Actions pipeline; do not build directly on the VM.

---

## Explicit Batch Items

Include these already-local corrections in the same implementation/deploy batch:

- Native `title` attributes on arbitrage and market dashboard table headers.
- `AGENTS.md` Plan Compliance Gate documentation.
