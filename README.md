# CueDbt

Cuemath's org-level dbt project. Owns all analytics transformations across Marketing, MathGym, and MathFit Tests — replacing Periscope DROP+INSERT scripts and runtime macros with version-controlled, tested, documented SQL.

---

## What This Repo Does

| Before | After |
|---|---|
| `data_playground.mbt_leads` built by Periscope DROP+INSERT | `marketing.mbt_leads` built by dbt, tested on every run |
| Channel attribution in 4 Periscope runtime macros | `macros/channel_attribution.sql` — versioned, reusable, testable |
| `mathgym_funnel_events` rebuilt daily via cron script | Incremental dbt model — only processes new events |
| No lineage — 400-line queries with no traceability | Full DAG: `dbt docs serve` shows every dependency |
| No tests — bad data reaches BI tools silently | 14 singular tests + schema tests on all primary keys |

---

## Project Structure

```
CueDbt/
├── models/
│   ├── staging/          # Layer 1: 1:1 from source, views, no business logic
│   │   ├── lsq/          # LeadSquared CRM (public.lsq_lead_data)
│   │   ├── superleap/    # Superleap CRM (post-Apr 2026)
│   │   ├── app/          # user_source_log + parent_profile
│   │   ├── payments/     # data_models.payment
│   │   ├── events/       # event_analytics.events (incremental)
│   │   ├── revenuecat/   # RevenueCat subscription events
│   │   ├── concepts/     # MathFit test data
│   │   └── intelenrollment/ # Student profiles
│   ├── intermediate/     # Layer 2: joins + business logic, views
│   │   ├── marketing/    # Lead enrichment, channel attribution, payment normalisation
│   │   ├── mathgym/      # Subscription events + user status
│   │   └── mathfit_tests/ # Section detail + student test status
│   └── marts/            # Layer 3: final tables, queried by BI + CueBI
│       ├── marketing/    # mbt_leads, mbt_opportunity, mbt_payments
│       ├── mathgym/      # fct_mathgym_*
│       └── mathfit_tests/ # fct_mathfit_tests_*
├── macros/               # Reusable SQL — replaces all Periscope runtime macros
├── tests/                # Singular SQL tests (data quality assertions)
├── seeds/                # Static lookup CSVs
├── snapshots/            # Historical CRM state capture
└── .github/workflows/    # CI: runs on every PR
```

---

## Domains

### Marketing
Tracks the full lead-to-payment funnel. Dual-source architecture handles the LSQ → Superleap CRM migration cutover on 2026-04-01.

| Model | Grain | Replaces |
|---|---|---|
| `mbt_leads` | 1 qualified lead | `data_playground.mbt_leads` |
| `mbt_opportunity` | 1 Superleap CRM opp | Existing Superleap script |
| `mbt_payments` | 1 payment transaction | `data_playground.mbt_payments_2024` |

### MathGym
Tracks the MathGym subscription funnel from app open to paid conversion and churn.

| Model | Grain |
|---|---|
| `fct_mathgym_monthly_funnel` | 1 row per month (funnel steps 1-12) |
| `fct_mathgym_user_subscription_status` | 1 row per user |
| `fct_mathgym_monthly_subscription_summary` | 1 row per trial_start_month cohort |

### MathFit Tests
Tracks MathFit test assignment, completion, and section-level performance.

| Model | Grain |
|---|---|
| `fct_mathfit_tests_net_adoption` | 1 row per region (lifetime) |
| `fct_mathfit_tests_monthly_summary` | 1 row per month |
| `fct_mathfit_tests_section_performance` | 1 row per section_title per month |
| `fct_mathfit_tests_student_monthly_engagement` | 1 row per student per month |

---

## Setup

### 1. Prerequisites

- Python 3.11
- Access to Cuemath Redshift (ask the data infra team for credentials)

### 2. Install

```bash
git clone https://github.com/Git-Vineeth/CueDbt.git
cd CueDbt

python3.11 -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate

pip install dbt-redshift==1.10.1
dbt deps                          # installs dbt-utils
```

### 3. Configure connection

```bash
cp profiles_template.yml ~/.dbt/profiles.yml
```

Open `~/.dbt/profiles.yml` and fill in:
- `host` — Redshift cluster endpoint
- `user` — your Redshift username
- `password` — use `env_var('DBT_PASSWORD')` and set the env var, or paste directly (**never commit**)
- `dbname` — the database name
- `schema` — set to `<your_name>_dev` (e.g. `vineeth_dev`)

Test the connection:
```bash
dbt debug
# Expected: "All checks passed"
```

---

## Daily Usage

### Run everything

```bash
dbt run        # builds all models in dependency order
dbt test       # runs all schema tests + singular tests
```

### Run a specific domain

```bash
dbt run --select staging.*
dbt run --select marts.marketing.*
dbt run --select marts.mathgym.*
dbt run --select marts.mathfit_tests.*
```

### Run a model and all its upstream dependencies

```bash
dbt run --select +mbt_leads
dbt run --select +fct_mathgym_user_subscription_status
```

### Run only changed models (fast, for development)

```bash
dbt run --select state:modified+
dbt test --select state:modified+
```

### Check source freshness

```bash
dbt source freshness
# Warns if raw tables haven't been updated in 24h, errors at 48h
```

### Generate and view documentation

```bash
dbt docs generate
dbt docs serve
# Opens http://localhost:8080 with full lineage graph + column docs
```

---

## PR Review with Claude (`/ultrareview`)

Every PR in this repo can be reviewed by Claude Code's multi-agent cloud reviewer before merging.

### How to run it

**On your local branch (no GitHub PR needed):**
```
/ultrareview
```
Bundles the current branch diff and runs a full multi-agent review locally.

**On an open GitHub PR:**
```
/ultrareview <PR number>
```
Example: `/ultrareview 12`

### What it checks

The reviewer runs multiple specialist agents in parallel across the diff:

- **SQL correctness** — logic errors, wrong join types, fan-out risks, NULL handling
- **dbt best practices** — ref() vs source() misuse, missing tests, hardcoded schema names
- **Business logic** — channel attribution gaps, dual-source cutover edge cases, grain violations
- **Performance** — missing sort/dist keys, full table scans on large models, incremental strategy issues
- **Test coverage** — untested primary keys, missing accepted_values on enum columns

### When to use it

Run `/ultrareview` before merging any PR that touches:
- `macros/` — attribution logic change affects every downstream mart
- `models/intermediate/marketing/` — business logic changes
- `models/marts/marketing/mbt_leads.sql` — the most critical table in the project
- Any model with cross-domain dependencies (e.g. anything that refs `mbt_leads` from mathgym)

### Note

`/ultrareview` is user-triggered from Claude Code (CLI or desktop app). It is billed per run. It does not run automatically in CI — that is handled by the GitHub Actions workflow which runs `dbt compile + run + test` on every PR.

---

## Git Workflow

```
main              ← protected, production only
feat/<name>-<feature>  ← your working branch

# Start a new feature
git checkout -b feat/vineeth-mbt-leads

# When done — push and open a PR
git push origin feat/vineeth-mbt-leads

# CI will automatically:
#   1. dbt compile (syntax check)
#   2. dbt run --select state:modified+
#   3. dbt test --select state:modified+
```

**PR rules:**
- CI must pass (compile + run + test)
- At least 1 reviewer must approve
- Squash merge into `main`
- Never commit directly to `main`

---

## Macros (Replaces Periscope Runtime Macros)

| Macro | Replaces | Usage |
|---|---|---|
| `channel_attribution(...)` | `[channel_test_referral_logic]` | Returns `channel_ref`: Referrals / Performance / Organic / Brand Partnership / Others |
| `landingpage_grouping(col)` | `[landingpage_grouping]` | Returns landing page category: Content / Intent / Brand / Supply / Referral / Others |
| `utm_medium_grouping(med, src)` | Inline CASE blocks | Returns normalised UTM medium: google_brand / google_other / meta / whatsapp / BTL / Influencer / others |
| `segment_referral(...)` | `[segment_referral_new]` | Returns granular sub-segment within each channel_ref |
| `utc_to_ist(col)` | `col + interval '330 minutes'` | Converts UTC timestamp to IST |
| `safe_divide(num, den)` | Manual NULLIF patterns | `round(100.0 * num / nullif(den, 0), 2)` |

---

## Tests

**14 singular tests** covering three categories:

| Category | What it checks | Example |
|---|---|---|
| Source structure | Does the source table behave as expected? | `assert_one_test_per_student_per_month` |
| Source behaviour | Do derived columns (scores, timestamps) hold their invariants? | `assert_section_score_only_on_completed_sections` |
| Business logic | Does the CASE statement produce consistent output? | `assert_cohort_logic`, `assert_full_completion_logic` |

Run tests for a specific domain:
```bash
dbt test --select marts.marketing.*
dbt test --select marts.mathgym.*
dbt test --select marts.mathfit_tests.*
```

---

## Migration Status (Periscope → dbt)

| Periscope Script / Macro | dbt Equivalent | Status |
|---|---|---|
| `data_playground.mbt_leads` DROP+INSERT | `marts/marketing/mbt_leads.sql` | ✅ Built — validate before decommissioning |
| `data_playground.mbt_payments_2024` DROP+INSERT | `marts/marketing/mbt_payments.sql` | ✅ Built — validate before decommissioning |
| `data_playground.mathgym_funnel_events` cron | `staging/events/stg_events__mathgym_funnel.sql` | ✅ Built (incremental) |
| `[landingpage_grouping]` Periscope macro | `macros/landingpage_grouping.sql` | ✅ Replaced |
| `[channel_test_referral_logic]` Periscope macro | `macros/channel_attribution.sql` | ✅ Replaced |
| `[segment_referral_new]` Periscope macro | `macros/segment_referral.sql` | ✅ Replaced |
| `[Channel_sales]` Periscope macro | Folded into `channel_attribution.sql` | ✅ Replaced |

**Validation approach:** dbt builds to `marketing.*` schema. Periscope still builds to `data_playground.*`. Run both in parallel, compare outputs, then flip BI tools to point at the new schema.

---

## Environment Schemas

| Target | Who | Schema | Example |
|---|---|---|---|
| `dev` | Each analyst | `<name>_dev` | `vineeth_dev.mbt_leads` |
| `ci` | GitHub Actions | `dbt_ci` | `dbt_ci.mbt_leads` |
| `prod` | dbt prod run | Domain-specific | `marketing.mbt_leads` |

---

## Key Design Decisions

- **One repo, three domains** — Marketing, MathGym, MathFit Tests. Shared macros and cross-domain joins (MathGym attribution uses `mbt_leads`) make one repo the right call at Cuemath's current team size.
- **Incremental for `event_analytics.events`** — hundreds of millions of rows; only new events processed on each run.
- **`public.lsq_lead_data` as source** — this is a security view with phone/email masking applied. dbt sources from here rather than the raw `thirdparty_lsq.lsq_lead_data` table.
- **Dual-source marketing** — LSQ era (pre-2026-04-01) and Superleap era (post-2026-04-01) are processed separately in intermediate and unioned in `mbt_leads`.
- **Cross-domain ref** — `int_mathgym__user_subscription_status` references `{{ ref('mbt_leads') }}` for marketing attribution. This is intentional; run with `dbt run --select +fct_mathgym_user_subscription_status` to ensure the full chain builds in order.
