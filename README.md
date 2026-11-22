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
dashboard/        AI/BI dashboard: Reliability Investment Dashboard.lvdash.json + build_dashboard.py (tests every query)
docs/             slides (PDF), figures, known issues
decision/         team planning pages (research question vote, build plan)
.claude/skills/   reference notes used with Claude Code (dataset, domain, rubric)
```

## How to reproduce

1. Upload the three Parquet files to a Unity Catalog volume in a Databricks workspace.
2. Run the `notebooks/01a` to `01d` notebooks in letter order, then `02a_ranking` to `04a_early_warning`, in order on serverless compute.
3. Import `dashboard/Reliability Investment Dashboard.lvdash.json` (Dashboards → Create → Import from file), or regenerate it with `python dashboard/build_dashboard.py build`.

Credentials are never stored in the notebooks or this repo. Use Databricks secrets or environment variables.

Team docs:

- [Databricks API and token setup](docs/databricks_api_token_setup.md): connecting the CLI or Python SDK from a laptop
- [Databricks ↔ GitHub working practice](docs/databricks_github_practice.md): Git folders, notebook ownership, table rules
- [Known issues](docs/known_issues.md): data problems found after the silver freeze
- [Notebooks and data flow](docs/notebooks_and_data_flow.md): naming conventions, data-flow diagram, and every Databricks table with its grain, writer and users
- [Checkpoint c2 sign-off](docs/c2_signoff.md): score definition, the 2 actions and the early-warning choice, with evidence
- [Build plan](docs/build_plan.md): tasks, owners, definitions and sourced inputs (snapshot of the team's live plan page)

## Methods

**Cleaning.** Every rule is logged with its row count in a `cleaning_log` table. Summary:

- Duplicates removed: 29 well-days, 23 jobs, 6 failures.
- Tubing pressure placeholder 999999 and pressures outside 0–20,000 kPa set to null.
- Negative oil/water volumes excluded from rate calculations.
- Pump sensor spikes capped (RPM above 1,000; current and torque above the 99.9th percentile).
- Pump make (about 32 spellings), job category, failure type and downtime codes mapped to canonical lists.
- Cleaning does not change the ranking: with and without cleaning, the top 10/25/50/100 wells by 12-month cost (Oct 28, 2025 – Oct 27, 2026) are identical, and total cost moves by 0.01%. The pressure and sensor fixes do not enter the cost score.

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

## Implementation plan (30/60/90 days)

**Value at stake.** Rod Truck and Workover spend on eligible wells was C$47.0M from Jun 2025 to Oct 2026 (about C$33M a year), plus C$8.4M of downhole lost oil at C$32.50/boe. Today's watchlist, the 114 wells with 2+ failures in the last 90 days, holds 24.9% of the last 12 months' repair spend on wells with a logged failure. *Sourced from field estimates in `well_jobs`; not invoices.*

**Pilot areas: Cactus Lake and Winter.** Together they have 62% of failures since Jun 2025 (945 and 826 of 2,841), which gives the pilot enough events to measure in 90 days.
- **Cactus Lake is where the money is.** It accounts for about 42% of Rod Truck and Workover dollars, and 44% of its jobs are workovers.
- **Winter is where failures repeat.** 81.5% of its failures are followed by another on the same well within 90 days, against 57% overall. Most of its jobs are cheaper rod truck jobs.

| Days | What happens | Owner (proposed) |
|---|---|---|
| 0–30 | A weekly reliability review in each pilot area, driven by the dashboard work list: recency first, then 12-month cost. Each well on the list gets a root-cause note. | Area production engineer leads; the data owner refreshes the tables weekly |
| 31–60 | Pick a budget scenario. The top 25 wells cost C$0.85M and pay back in 2.3 years at 25% fewer failures, or 1.1 years at 50%. Run a root-cause review on every well with a second failure within 90 days. For Cactus Lake, also trial slower pump speeds on fast wells (rods fail 0.55/well-year below 150 RPM vs 1.50 at 250–349 RPM). | Operations superintendent approves the budget; rig and rod-truck coordinators schedule the work |
| 61–90 | Measure the KPIs against the 0–30 day baseline. Add the runtime alert rule only in areas whose crews can handle about 9 alerts per area per week. Expand area by area. | Reliability lead |

**Resources.**
- **Money:** the action cost of C$34,152 per well is *assumed* (the 2023+ Workover median), and the 25%/50% failure reduction is *assumed* (it brackets one vendor case of about 34%). The netback (C$32.50/boe, Strathcona Q3 2026 operating earnings) and the repair costs are *sourced*.
- **Manpower** *(our estimate, to confirm with operations)*: the weekly review needs about 1 hour from the area engineer and a foreman. The data owner reruns the notebooks weekly.
- **Logistics** *(proposal)*: do each action when the rig or rod truck is already on the well for its next repair, so most actions need no extra mobilization. Rod Truck and Workover jobs run about 5% lower in Apr–May (2023–2025 average), which leaves room to schedule pilot work then.
- **Time:** savings start with the first avoided failure. Most repeat failures come back within about six weeks, so a change should show in the 90-day re-failure rate within one quarter.

**KPIs.** 90-day re-failure rate, failures per well-year, repair $ per well-year, downhole lost oil (m³), and alerts per area per week.

**Data fixes needed to run this for real.** Record the actual job cost (`actual_final_job_cost` is empty today). Keep a failure log from before Jun 2025 if one exists. Use one spelling per failure type. Get daily torque and current readings instead of repeated manual entries.

## Limitations

Each limitation says how it moves the results. "Conservative" means the true benefit is probably larger than reported.

| Limitation | Effect on results |
|---|---|
| The failure log starts in Jun 2025, so bad actors are picked on 7 months and measured on 10. | Wide intervals on rates and payback. Measuring on the months used to pick would **overstate** the rates (rods 6.99/yr while picking vs 4.05/yr after), so we measure on later months. |
| Rod Truck cost is missing before 2023, and earlier history uses repair jobs as a stand-in for failures. | The ranking starts in 2023. History-based ranking only modestly beats ranking by well size (top 10% catches 16.5% vs 15.9% of later cost). |
| Repair cost is the field estimate (`field_est_total`); invoiced cost is empty. | Direction **unknown**. Payback scales with cost per failure: 20% higher real costs shorten payback by about a sixth. |
| The netback is company-wide operating earnings (C$32.50/boe), not a heavy-oil netback. | Small effect. Netback ±25% moves Action 1's payback from 1.77 to between 1.65 and 1.90 years at 50% fewer failures. Repair dollars, not lost oil, drive the savings. |
| `lost_oil_m3` is an operator estimate and is null on 67.5% of daily rows; nulls count as zero. | Lost-oil value is **understated**, so savings are understated and payback is **conservative**. |
| Downtime codes are only partly mapped to causes; unmapped codes fall in "Other". | Downhole lost oil per failure is likely **understated** (conservative), and the cause split on page 1 is approximate. |
| `failure_start_date` is logged a median 2 days after the well stops. | Handled by anchoring on the stop day. Any stops we miss would **understate** lost oil per failure (conservative). |
| Action cost (C$34,152) and its effect (25% or 50% fewer failures) are assumptions, not measured. | Payback is proportional to both: half the action cost halves payback (1.77 → 0.88 years for Action 1 at 50%). We report the range, not one number. |
| Equipment and RPM comparisons are observational. | Harder wells may get particular pumps or run faster, so switching equipment may deliver **less** than the gap suggests. Reported as associations. |
| Torque, current and RPM repeat day to day, and torque covers 6% of Cactus Lake failures. | The early warning uses runtime only, so its recall (43.6%) is a **lower bound** on what daily sensor readings could give. |

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
- Dashboard screenshots (Databricks AI/BI dashboard, "Reliability Investment Dashboard"):
  - [Cost Drivers](dashboard/Cost%20Drivers.png): where repair dollars and lost oil go
  - [Recurrence Analysis](dashboard/Recurrence%20Analysis.png): how often failures come back
  - [Work Order Priorities](dashboard/Work%20Order%20Priorities.png): this week's ranked work list and runtime alerts
  - [Return on Action – Budget](dashboard/Return%20on%20Action%20-%20Budget.png): payback sensitivity and budget scenarios
  - [Model Validation](dashboard/Model%20Validation.png): out-of-time ranking test and early-warning results
  - [Data Quality](dashboard/Data%20Quality.png): cleaning rules and what the data cannot tell us
