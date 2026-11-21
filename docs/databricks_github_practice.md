# Databricks ↔ GitHub working practice

How the team keeps code in GitHub and runs it in Databricks without overwriting each other's work. For connecting the CLI or SDK from a laptop, see [databricks_api_token_setup.md](databricks_api_token_setup.md).

## The model

| What | Lives in | Source of truth |
|---|---|---|
| Notebooks, `src/` code, docs, README | GitHub repo `Angie629/data-dev-exercise` | GitHub `master` |
| Tables (bronze, silver, gold) and raw files | Unity Catalog `workspace.dsmlc` | The notebook that writes them |
| Dashboard | Databricks AI/BI | Exported to `dashboard/` before submission |

Code moves through git; data never does. `dataset/` is in `.gitignore`.

## One-time setup (each person)

1. **Link GitHub to Databricks.** User Settings → Linked accounts → Git integration → GitHub. Use the GitHub App or OAuth option if offered. Otherwise use a GitHub fine-grained token scoped to this repo only, with **Contents: Read and write** and an expiry of a few days.
2. **Create your own Git folder.** Workspace → your user folder → Create → Git folder → paste `https://github.com/Angie629/data-dev-exercise.git`.
3. **Use source format for notebooks** so diffs are readable and outputs are never committed. Look for a default notebook file format setting under User Settings → Developer and choose Source (`.py`). The exact menu name may differ in Free Edition; check that new notebooks show up as `.py` files in git.

Never work in someone else's Git folder. Everyone gets their own clone.

## Who owns which file

One owner per notebook. Others read it, but only the owner edits it. This avoids merge conflicts with no extra process.

| Notebook | Owner | Writes tables |
|---|---|---|
| `notebooks/01_clean_daily.py` | Angie | `silver_daily`, `cleaning_log` |
| `notebooks/01_clean_jobs_failures.py` | Jacob | `silver_jobs`, `silver_failures` |
| `notebooks/01_downtime_features.py` | Ananya | `downtime_bucket_map`, `silver_downtime`, `feat_daily_sensor` |
| `notebooks/01_daily_checks.py` | Ananya | `gold_failure_downtime` |
| `notebooks/03_repeat_failures.py` | Jacob | `gold_failure_gap`, `gold_recency_tier` |
| `notebooks/02_ranking.py` | Angie | `gold_well_month`, `gold_well_period` |
| `notebooks/03_runlife_payback.py` | Jacob | `gold_failure_rates`, `gold_payback` |
| `notebooks/04_early_warning.py` | Ananya | `gold_alerts` |
| `notebooks/05_weather.py` | Ananya (if go) | `weather_daily` |
| `README.md`, `AI_LOG.md`, `docs/` | Anyone, small edits | — |

Table names after `silver_daily` are proposals; rename here if you change them.

## Daily loop

1. **Pull** before you start: Git folder → branch button → Pull.
2. Work on **your** notebook only.
3. **Commit and push** at least at every checkpoint (Fri 11 PM, Sat 1 PM, Sat 3 PM, Sat 8 PM), and whenever something works.
4. **Pull again** before you push. If someone else changed `README.md` or `AI_LOG.md`, the pull brings their change in first.

Commit messages say what changed in plain words: `silver_daily: cap torque at p99.9`, not `update`.

## Branches

With 15 hours and one owner per file, everyone commits to `master`. Use a short-lived branch only for a risky change (for example rewriting the ranking score). Name it `<name>/<task>`, e.g. `angie/score-per-day`, and merge it the same day.

## Editing in two places

You can edit a notebook in Databricks or locally (VS Code, Claude Code), but **not both at once**. Pick one per session:

- Edited in Databricks → commit and push there → `git pull` locally.
- Edited locally → `git push` → Pull in the Databricks Git folder.

## Table rules

- **Bronze is read-only.** `daily_production`, `well_jobs` and `production_failures` are never dropped or overwritten. Fixes go in silver.
- **A notebook writes only its own tables** (see the ownership table).
- **Silver freezes at Fri 11 PM.** Later data issues go in [known_issues.md](known_issues.md), not into silver.
- Use full names (`workspace.dsmlc.silver_daily`) in code, so it doesn't depend on the current schema.
- Overwriting your own silver or gold table is fine; downstream notebooks rerun after it.

## Shared code in `src/`

Put functions used by more than one notebook (mappings, rolling-window features) in `src/`. Notebooks inside a Git folder can usually import modules from the repo root:

```python
from src.mappings import MAKE_MAP
```

If the import fails, add the repo root to the path at the top of the notebook:

```python
import os, sys
sys.path.append(os.path.abspath(".."))
```

Not yet tested in our workspace.

## What never goes in the repo

- Tokens, passwords, connection strings, workspace URLs with credentials. Use Databricks secrets or your local `~/.databrickscfg`.
- The dataset or any extract of it (CSV dumps, sampled rows).
- Notebook outputs that show data rows. Source format strips outputs; if someone commits `.ipynb`, clear outputs first.
- Large files (charts are fine; data files are not).

## Before submission (Sun 9–11 AM)

1. Everyone pushes their final notebook.
2. One person creates a **fresh** Git folder from GitHub and runs the notebooks in order on new serverless compute:
   `01_clean_daily` → `01_clean_jobs_failures` → `01_downtime_features` → `02_ranking` → `03_runlife_payback` → `04_early_warning` (→ `05_weather`).
3. Export the dashboard definition to `dashboard/` and add screenshots.
4. Check the repo on GitHub in a logged-out browser: public, README complete, no data, no secrets.

## Troubleshooting

| Problem | Fix |
|---|---|
| Pull fails with a conflict | Usually two people edited the same file. Keep the owner's version; the other person reapplies their change. For README/AI log, resolve locally with `git pull` and push. |
| Push rejected (not up to date) | Pull first, then push. |
| Push asks for credentials | GitHub isn't linked, or the GitHub token expired. Redo setup step 1. |
| Notebook opens as plain text | The file is missing the first line `# Databricks notebook source`. |
| `TABLE_OR_VIEW_NOT_FOUND` mid-run | Someone is rebuilding that table. Wait for them to finish, then rerun. |
