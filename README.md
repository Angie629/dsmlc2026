# Bad Actors: Targeting Repeat-Failure Wells

*Working title.* Strathcona Resources × Chirality Research Datathon, November 2026. Theme: Optimization.

**Solution form:** (3) Implementation / recommendation plan, backed by a Databricks AI/BI dashboard.

## Team

- Angie
- Jacob
- Ananya

## Research question

> Which producing wells will drive Strathcona's repeat downhole failures and lost oil next period, and which targeted actions on those wells pay back?

Supporting questions:

1. Does a ranking built on past repair cost and lost oil identify the wells that are costly in a later period, better than random selection and better than ranking by well size?
2. Which equipment and operating factors are associated with higher failure rates on those wells?
3. Which interventions break even, and at what failure-reduction rate?
4. Can simple threshold rules on rod torque, motor current and pump efficiency flag failures before they happen?

## Motivation

Every producing heavy oil well here is lifted by a progressing cavity pump (PCP) driven through a rod string, and rods are the most common failure. Each failure costs a repair job plus the oil not produced while the well is down.

Profiling of the competition data (Nov 20, 2026) shows the cost is concentrated in a small set of wells:

| Measure | Value |
|---|---|
| Rod Truck + Workover repair spend since Jun 2025 (field estimates) | $64.1M over 975 wells, about $46M a year |
| Repair $ held by the top 10% / top 20% of those wells | 33% / 50% |
| Failures followed by another on the same well within 90 days (Jun 2025 on) | 57% (rods 65%, pump and tubing about 30%) |
| Failures from wells with 3 or more failures | 79% |
| Lost oil on downhole downtime codes | 213,499 m³ (3 codes only; full bucket map pending) |

Rod failures are chronic, not one-off. A short, regularly refreshed list of repeat-failure wells lets reliability engineers and field crews focus where the money is.

## Summary of findings

*Pending. Filled in after validation (Saturday 8 PM).*

- Ranking: capture@10% and lift vs random and size baselines
- Equipment: failure-rate differences within operating areas
- Payback: break-even failure reduction for the 2 chosen actions
- Early warning: 2026 hold-out precision, recall and lead time

## Data

Three Parquet files provided by the organizers (not included in this repo):

| File | Grain | Rows | Wells | Dates |
|---|---|---|---|---|
| `daily_production.parquet` | well × day | 5,075,590 | 2,905 | 2021-01-01 to 2026-10-27 |
| `well_jobs.parquet` | one job | 15,610 | 1,903 | 2021-01-01 to 2026-10-27 |
| `production_failures.parquet` | one failure | 2,873 | 825 | 2021-12-14 to 2026-10-26 |

Join key: `uwi`. Events are aggregated to well × day or well × month before joining to the daily table, so rows are not repeated.

Data limits that shape the method:

- The failure log is live only from June 2025 (Jan–Apr 2025: 3 rows, May: 27, then about 160–180 a month). Earlier history uses Rod Truck and Workover jobs as a stand-in; 2,812 of 2,873 failures have one of those jobs on the same well within −3 to +14 days.
- Rod Truck job cost is missing before 2023 (0.1% of jobs costed in 2021, 40% in 2022, 99.5%+ from 2023), so cost-based ranking starts in 2023.
- `failure_start_date` is logged after the well stops: 66% of 2025 rod/pump failures were already down on that day or the day before (median 2 days earlier). Early warning and lost oil are anchored on the stop day.
- Rod torque, motor current and RPM repeat the previous day's value 90–96% of the time, so they behave like periodic manual entries rather than daily sensor readings.
- `failure_end_date` and `actual_final_job_cost` are empty. Repair cost uses `field_est_total`, a field estimate.
- `lost_oil_m3` is an operator estimate and is null on 67.5% of daily rows.

## Repository structure

```
notebooks/        Databricks notebooks, run in order
  01a_clean_daily    bronze → silver_daily, cleaning log
  01b_clean_jobs_failures  bronze → silver_jobs, silver_failures (failures linked to repair jobs)
  01c_downtime_features    downtime buckets, silver_downtime, rolling pump-sensor features (needs silver_daily)
  01d_daily_checks   lost oil by bucket, power-downtime seasonality, downtime per failure from the stop day (gold_failure_downtime)
  02a_ranking        gold_well_month, gold_well_period, cost score and ranks; out-of-time validation (gold_ranking_validation)
  03a_repeat_failures  re-failure@30/90, Rod Truck repeat rate by year, recency tiers (gold_failure_gap, gold_recency_tier)
  03b_runlife_payback  failure rates by pump make, model and RPM within area (gold_failure_rates); payback added next
  04a_early_warning  sensors before failures, threshold grid, holdout evaluation and gold alert outputs
  (05 reserved for weather: not used, power downtime is not seasonal)
src/              shared cleaning and feature functions
dashboard/        AI/BI dashboard: strathcona_reliability_desk.lvdash.json + build_reliability_desk.py (tests every query)
docs/             slides (PDF), figures, known issues
decision/         team planning pages (research question vote, build plan)
.claude/skills/   reference notes used with Claude Code (dataset, domain, rubric)
```

## How to reproduce

1. Upload the three Parquet files to a Unity Catalog volume in a Databricks workspace.
2. Run the `notebooks/01a` to `01d` notebooks in letter order, then `02a_ranking` to `04a_early_warning`, in order on serverless compute.
3. Import `dashboard/strathcona_reliability_desk.lvdash.json` (Dashboards → Create → Import from file), or regenerate it with `python dashboard/build_reliability_desk.py build`.

Credentials are never stored in the notebooks or this repo. Use Databricks secrets or environment variables.

Team docs:

- [Databricks API and token setup](docs/databricks_api_token_setup.md): connecting the CLI or Python SDK from a laptop
- [Databricks ↔ GitHub working practice](docs/databricks_github_practice.md): Git folders, notebook ownership, table rules
- [Known issues](docs/known_issues.md): data problems found after the silver freeze
- [Notebooks and data flow](docs/notebooks_and_data_flow.md): naming conventions, data-flow diagram, and every Databricks table with its grain, writer and users
- [Reliability Desk dashboard JSON](dashboard/strathcona_reliability_desk.json): machine-readable pages, queries and widget layout
- [Reliability Desk dashboard specification](dashboard/strathcona_reliability_desk.md): page brief, palette, formats and acceptance checks
- [Build plan](docs/build_plan.md): tasks, owners, definitions and sourced inputs (snapshot of the team's live plan page)

## Methods

**Cleaning.** Every rule is logged with its row count in a `cleaning_log` table. Summary:

- Duplicates removed: 29 well-days, 23 jobs, 6 failures.
- Tubing pressure placeholder 999999 and pressures outside 0–20,000 kPa set to null.
- Negative oil/water volumes excluded from rate calculations.
- Pump sensor spikes capped (RPM above 1,000; current and torque above the 99.9th percentile).
- Pump make (about 32 spellings), job category, failure type and downtime codes mapped to canonical lists.

**Cost score.** For each well and period:

```
cost_score = repair_$ + downhole_lost_oil_m3 × 6.2898 × netback_per_bbl
```

`repair_$` is the sum of `field_est_total` for Rod Truck and Workover jobs. `netback_per_bbl` is C$32.50 per boe, Strathcona's Q3 2026 operating earnings (company-wide, not heavy oil only; see External resources).

**Equipment comparison.** Failures per producing well-year by pump make and model, compared within operating areas, with Poisson 95% intervals and a minimum group size.

**Payback.** Cost per failure (linked repair job + downhole lost oil from the day the well stopped to 14 days after the logged failure date) against the annualized cost of an action, giving a break-even number of failures avoided per well-year, with a sensitivity grid.

**Early warning.** Rules on runtime instability (partial-run and fully down days in the last week) and on 7-day vs baseline drops in pump efficiency and fluid rate. Torque and current are not used because their values repeat day to day. An alert counts as a hit if the well stops for a failure 2–30 days later. The baseline to beat is a recency watchlist: wells with 2+ failures in the last 90 days.

## Validation

- **Ranking:** out of time. Rank on 2023–2024, measure the share of June 2025 – October 2026 cost captured by the top 5/10/20% of wells. Compare with random selection, with ranking by oil volume, and with a recency watchlist (2+ failures in the last 90 days). 95% intervals by bootstrap over wells.
- **Early warning:** thresholds on runtime instability and efficiency, chosen on June–December 2025 failures (aligned to the stop day) and frozen before testing on January–October 2026. Precision, recall, median lead time and alerts per area per week, compared with the recency watchlist and with random alerts at the same rate.
- **Payback:** sensitivity to netback, action cost and failure-reduction rate. Equipment effects are observational and reported as associations.

## Limitations

*Pending. Filled in Saturday afternoon.*

## External resources

- Organizer materials: Briefing Document, Research Topic Guide, Dataset Handbook
- Strathcona Resources, Q3 2026 results (release dated Nov 8, 2026), as reported by Cantech Letter: operating earnings C$32.50 per boe (value of a lost barrel), WCS price, operating and transport costs, Lloydminster Conventional scale. Link and MD&A page to be added.
- Western Energy Services, Q3 2026 results, as reported by BOE Report: C$921 per service hour, used only to cross-check job cost estimates. Link to be added.
- Weatherford COROD case study, Colombia (six wells): rod run life up 51%, used only to bracket the avoided-failure assumption. Link to be added.
- Weatherford COROD corrosion-inhibition case study, Alberta (one well): qualitative support only. Link to be added.
- PetroWiki: progressing cavity pump systems
- Weather: not used. Power/grid downtime showed no repeatable seasonal pattern (`01d_daily_checks`).

## Links

- Video (5 min): *pending*
- Slides: *pending*
- Dashboard screenshots: `dashboard/`
