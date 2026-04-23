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
models/
├── staging/        # 1:1 from source — rename, cast, dedup. No business logic.
├── intermediate/   # Joins + business logic. Not queried directly.
└── marts/          # Final tables for BI and CueBI.
    ├── marketing/      # mbt_leads, mbt_opportunity, mbt_payments
    ├── mathgym/        # fct_mathgym_*
    └── mathfit_tests/  # fct_mathfit_tests_*

macros/             # Reusable SQL functions
tests/              # Data quality assertions (assert_*.sql)
seeds/              # Static lookup CSVs
snapshots/          # Historical state capture
```

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
