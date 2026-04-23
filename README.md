# CueDbt

Cuemath's dbt project for analytics transformations across Marketing, MathGym, and MathFit Tests.

---

## Setup

### 1. Install

```bash
git clone https://github.com/Git-Vineeth/CueDbt.git
cd CueDbt

python3.11 -m venv venv
source venv/bin/activate

pip install dbt-redshift==1.10.1
dbt deps
```

### 2. Configure connection

```bash
cp profiles_template.yml ~/.dbt/profiles.yml
# Fill in host, dbname, user, password
# Set schema to <your_name>_dev (e.g. vineeth_dev)
```

```bash
dbt debug   # should say "All checks passed"
```

---

## Daily Usage

```bash
dbt run                              # build all models
dbt test                             # run all tests

dbt run --select staging.*           # staging only
dbt run --select marts.marketing.*   # one domain
dbt run --select +mbt_leads          # model + all upstream

dbt source freshness                 # check raw table staleness
dbt docs generate && dbt docs serve  # lineage + column docs at localhost:8080
```

---

## Project Structure

```
CueDbt/
├── models/
│   ├── staging/                  # Layer 1 — 1:1 from source, views, no business logic
│   │   ├── lsq/                  # LeadSquared CRM (public.lsq_lead_data)
│   │   ├── superleap/            # Superleap CRM (post-Apr 2026)
│   │   ├── app/                  # user_source_log + parent_profile
│   │   ├── payments/             # data_models.payment
│   │   ├── events/               # event_analytics.events (incremental)
│   │   ├── revenuecat/           # RevenueCat subscription events
│   │   ├── concepts/             # MathFit test data
│   │   └── intelenrollment/      # Student profiles
│   ├── intermediate/             # Layer 2 — joins + business logic, views
│   │   ├── marketing/
│   │   ├── mathgym/
│   │   └── mathfit_tests/
│   └── marts/                    # Layer 3 — final tables for BI + CueBI
│       ├── marketing/
│       ├── mathgym/
│       └── mathfit_tests/
├── macros/                       # Reusable SQL functions
├── tests/                        # Data quality assertions (assert_*.sql)
├── seeds/                        # Static lookup CSVs
├── snapshots/                    # Historical state capture
└── .github/workflows/            # CI — runs on every PR
```

### Domains & Mart Models

**Marketing** — lead-to-payment funnel

| Model | Grain |
|---|---|
| `mbt_leads` | 1 qualified lead |
| `mbt_opportunity` | 1 Superleap CRM opportunity |
| `mbt_payments` | 1 payment transaction |

**MathGym** — subscription funnel from app open to paid conversion

| Model | Grain |
|---|---|
| `fct_mathgym_monthly_funnel` | 1 row per month (steps 1–12) |
| `fct_mathgym_user_subscription_status` | 1 row per user |
| `fct_mathgym_monthly_subscription_summary` | 1 row per trial cohort month |

**MathFit Tests** — test assignment, completion, and section performance

| Model | Grain |
|---|---|
| `fct_mathfit_tests_net_adoption` | 1 row per region (lifetime) |
| `fct_mathfit_tests_monthly_summary` | 1 row per month |
| `fct_mathfit_tests_section_performance` | 1 row per section per month |
| `fct_mathfit_tests_student_monthly_engagement` | 1 row per student per month |

---

## Git Workflow

```bash
# Create a branch
git checkout -b feat/<your-name>-<feature>

# When ready — open a PR
git push origin feat/<your-name>-<feature>
```

- CI runs automatically on every PR: `dbt compile` → `dbt run` → `dbt test` on changed models
- PRs require 1 reviewer approval before merging
- Squash merge into `main`

### PR Review with Claude

```bash
/ultrareview          # review current branch
/ultrareview <PR#>    # review a specific GitHub PR e.g. /ultrareview 12
```
