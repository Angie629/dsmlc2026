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
| Rod Truck + Workover jobs, 2021–26 (field estimate > 0) | 11,785 jobs, $212.4M |
| Failures from the top 10% / top 20% of wells producing oil since 2025 (1,150 wells) | 40% / 63% |
| Failures from wells with 3 or more failures (377 wells) | 79% |
| Lost oil on downhole downtime codes | 213,499 m³ |
| Top-10% wells by repair job count in 2021–23 still top 10% in 2024–26 | 31% (about 3× random) |

A short, regularly refreshed list of wells lets reliability engineers and field crews focus where the money is.

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

- The failure log is almost entirely 2025–2026 (2 failures before 2025). Earlier history uses Rod Truck and Workover jobs as a stand-in; 2,812 of 2,873 failures have one of those jobs on the same well within −3 to +14 days.
- `failure_end_date` and `actual_final_job_cost` are empty. Repair cost uses `field_est_total`, a field estimate.
- `lost_oil_m3` is an operator estimate and is null on 67.5% of daily rows.

## Repository structure

```
notebooks/        Databricks notebooks, run in order
  01_clean          bronze → silver tables, cleaning log
  02_ranking        gold tables, cost score, out-of-time validation
  03_runlife_payback  failure rates by equipment, break-even payback
  04_early_warning  threshold rules, 2025 tuning, 2026 test
  05_weather        optional weather join
src/              shared cleaning and feature functions
dashboard/        exported AI/BI dashboard definition and screenshots
docs/             slides (PDF), figures, known issues
decision/         team planning pages (research question vote, build plan)
.claude/skills/   reference notes used with Claude Code (dataset, domain, rubric)
```

## How to reproduce

1. Upload the three Parquet files to a Unity Catalog volume in a Databricks workspace.
2. Run `notebooks/01_clean` to `notebooks/04_early_warning` in order on a serverless or single-node cluster.
3. Import the dashboard from `dashboard/`.

Credentials are never stored in the notebooks or this repo. Use Databricks secrets or environment variables.

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

`repair_$` is the sum of `field_est_total` for Rod Truck and Workover jobs. `netback_per_bbl` is Strathcona's reported heavy oil operating netback (source cited under External resources).

**Equipment comparison.** Failures per producing well-year by pump make and model, compared within operating areas, with Poisson 95% intervals and a minimum group size.

**Payback.** Cost per failure (linked repair job + lost oil from day −1 to +14) against the annualized cost of an action, giving a break-even number of failures avoided per well-year, with a sensitivity grid.

**Early warning.** Rules on 7-day vs 30-day baseline changes in rod torque, motor current and pump efficiency, on days the well ran at least 12 hours. An alert counts as a hit if a failure follows within 1–30 days.

## Validation

- **Ranking:** out of time. Rank on 2021–2023, measure the share of 2024–2026 cost captured by the top 5/10/20% of wells. Compare with random selection and with ranking by oil volume. 95% intervals by bootstrap over wells.
- **Early warning:** thresholds chosen on 2025 failures and frozen before testing on 2026 failures. Precision, recall, median lead time and alerts per area per week, compared with random alerts at the same rate.
- **Payback:** sensitivity to netback, action cost and failure-reduction rate. Equipment effects are observational and reported as associations.

## Limitations

*Pending. Filled in Saturday afternoon.*

## External resources

- Organizer materials: Briefing Document, Research Topic Guide, Dataset Handbook
- Strathcona Resources investor reports (netback source; quarter and page to be cited)
- PetroWiki: progressing cavity pump systems
- Weather source, if used

## Links

- Video (5 min): *pending*
- Slides: *pending*
- Dashboard screenshots: `dashboard/`
