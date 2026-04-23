# CueDbt — Project Principles & Patterns

This file is loaded by Claude Code automatically. Read it before writing any model.

---

## Architecture

Three-layer pattern. Never skip layers or go sideways.

```
Sources (raw Redshift) → Staging → Intermediate → Marts
```

| Layer | Prefix | Materialization | Rule |
|---|---|---|---|
| Staging | `stg_` | view | 1:1 from source. Rename, cast, dedup. No business logic. |
| Intermediate | `int_` | view | Joins + business logic. Never queried directly by analysts. |
| Marts | `mbt_` / `fct_` | table | Final output. BI tools and CueBI query these. |

---

## Domains

| Domain | Folder | Mart models | Source systems |
|---|---|---|---|
| Marketing | `marketing/` | `mbt_leads`, `mbt_opportunity`, `mbt_payments` | LSQ, Superleap, data_models.payment |
| MathGym | `mathgym/` | `fct_mathgym_*` | RevenueCat, event_analytics.events |
| MathFit Tests | `mathfit_tests/` | `fct_mathfit_tests_*` | application_service_concepts.* |

---

## Key Rules

**1. source() in staging only, ref() everywhere else.**
Never use a raw table name in intermediate or marts. If you need a raw table in a mart, create a staging model first.

**2. Macros replace copy-pasted SQL.**
Use `{{ channel_attribution(...) }}`, `{{ landingpage_grouping(...) }}`, `{{ utm_medium_grouping(...) }}`, `{{ utc_to_ist(...) }}`, `{{ safe_divide(...) }}`. Never inline these CASE blocks.

**3. The dual-source architecture.**
LSQ era = pre-2026-04-01. Superleap era = post-2026-04-01. `mbt_leads` is a UNION of both. `mbt_opportunity` is Superleap only. The cutover date is `var("superleap_cutover_date")`.

**4. state is source of truth for MathFit, timestamps are source of truth for MathGym.**
For MathFit: `user_node.state` (COMPLETED/IN_PROGRESS/NOT_STARTED/LOCKED) is definitive. For MathGym: RevenueCat event timestamps are definitive.

**5. SANDBOX events are excluded in intermediate, not staging.**
`stg_revenuecat__events` passes through all environments. `int_mathgym__subscription_events` filters `environment != 'SANDBOX'`.

**6. Test users (MathGym) are identified by any event before 2026-02-11.**
ALL events from that `app_user_id` are excluded — not just the pre-launch ones.

**7. UTM IST conversion: always use the macro.**
`{{ utc_to_ist('column_name') }}` not `column_name + interval '330 minutes'`.

**8. NULLIF for all division.**
`{{ safe_divide(numerator, denominator) }}` or `round(100.0 * n / nullif(d, 0), 2)`. Never divide without NULLIF.

**9. NULL != 'value' evaluates to NULL in Redshift, not true.**
Always add `OR column IS NULL` when excluding a value from a nullable column. See step_8 in `fct_mathgym_monthly_funnel.sql`.

**10. Singular tests go in tests/{domain}/. Name them assert_*.**
Three categories: source structure (does source behave as expected?), source behaviour (do derived columns hold?), business logic (does my CASE statement work?).

---

## Known Data Issues

| Issue | Location | Exclusion |
|---|---|---|
| Test ID 879094ce has NOT_STARTED section with score | `int_mathfit_tests__section_detail` | Hard-coded WHERE clause with comment |

---

## Migration Status (Periscope → dbt)

| Table | Status | Notes |
|---|---|---|
| `data_playground.mbt_leads` | dbt built | Run in parallel while validating |
| `data_playground.mbt_payments_2024` | dbt built | Run in parallel while validating |
| `data_playground.mathgym_funnel_events` | dbt built (incremental) | Replaces cron DROP+INSERT |
| `thirdparty_crm_superleap` tables | dbt built | Superleap staging + opportunity mart |
| Periscope [landingpage_grouping] macro | dbt built | `macros/landingpage_grouping.sql` |
| Periscope [channel_test_referral_logic] macro | dbt built | `macros/channel_attribution.sql` |
| Periscope [segment_referral_new] macro | dbt built | `macros/segment_referral.sql` |
| Periscope [Channel_sales] macro | dbt built | Folded into `channel_attribution.sql` |

---

## Run Order

```bash
dbt run --select staging.*          # staging first — views only, cheap
dbt test --select staging.*         # catch source issues early
dbt run --select intermediate.*     # intermediate next
dbt test --select intermediate.*
dbt run --select marts.*            # marts last
dbt test --select marts.*
```

Or simply: `dbt run && dbt test`

---

## Team Branching

- `main` = production. Protected. No direct commits.
- Branch naming: `feat/<name>-<feature>` e.g. `feat/vineeth-mbt-leads`
- PR requires: `dbt compile` passes + `dbt test --select state:modified+` passes + 1 reviewer
- Squash merge only

---

## Profiles

Copy `profiles_template.yml` to `~/.dbt/profiles.yml`. Fill in credentials. Never commit credentials.

Dev schema: `<your_name>_dev` (e.g. `vineeth_dev`, `ranjosh_dev`). Never write to shared schemas in dev.
