# Bad Actors: Targeting Repeat-Failure Wells

DSMLC Datathon 2026: Strathcona Resources × Chirality Research, November 2026. Theme: Optimization.

**Solution form:** (3) Implementation / recommendation plan, backed by a Databricks AI/BI dashboard.

## Team

- Ananya Jain
- Angie Shin
- Jacob McCarthy

## Research question

> Which producing wells will drive Strathcona's repeat downhole failures and lost oil next period, and which targeted actions on those wells pay back?

Supporting questions:

1. Does a ranking built on past repair cost and lost oil identify the wells that are costly in a later period, better than random selection and better than ranking by well size?
2. Which equipment and operating factors are associated with higher failure rates on those wells?
3. Which interventions break even, and at what failure-reduction rate?
4. Can simple threshold rules on pump runtime and efficiency flag failures before they happen, and does a machine-learning model do better at the same workload? (Rod torque and motor current turned out to repeat day to day, so they could not be used.)

## Motivation

Every producing heavy oil well here is lifted by a progressing cavity pump (PCP) driven through a rod string, and rods are the most common failure. Each failure costs a repair job plus the oil not produced while the well is down.

Profiling of the raw competition data (Nov 20, 2026) shows the cost is concentrated in a small set of wells. The dollar figures in the rest of this README use the cleaned tables and eligible producing wells, so they are lower than the raw totals here.

| Measure                                                                    | Value                                              |
| -------------------------------------------------------------------------- | -------------------------------------------------- |
| Rod Truck + Workover repair spend since Jun 2025 (raw field estimates)     | $64.1M over 975 wells                              |
| Repair $ held by the top 10% / top 20% of those wells                      | 33% / 50%                                          |
| Failures followed by another on the same well within 90 days (Jun 2025 on) | 57% (rods 65%, pump and tubing about 30%)          |
| Failures from wells with 3 or more failures                                | 79%                                                |
| Downhole lost oil, 2021–26 (full downtime-bucket map, `01d_daily_checks`)  | 219,627 m³                                         |

Rod failures are chronic, not one-off. A short, regularly refreshed list of repeat-failure wells lets reliability engineers and field crews focus where the money is.

## Summary of findings

Rod Truck and Workover repairs cost about C$33M a year (field estimates, Jun 2025 – Oct 2026), plus C$8.4M of downhole lost oil over the same period. The money concentrates on wells that keep failing. *Dashboard: [Cost Drivers](dashboard/Cost%20Drivers.png).* Each bullet names the dashboard tab it comes from; the links open the screenshots.

- **Failures repeat, and recent failures are the strongest warning.** 57% of failures are followed by another on the same well within 90 days (rods: about two in three). Wells with 0 / 1 / 2 / 3+ failures in the last 90 days fail again within 90 days 26% / 40% / 61% / 70% of the time. Today's watchlist (2+ recent failures) is 114 wells holding 24.9% of the last 12 months' repair spend on wells with a logged failure. *Dashboard: [Recurrence Analysis](dashboard/Recurrence%20Analysis.png); watchlist on [Work Order Priorities](dashboard/Work%20Order%20Priorities.png).*
- **Long-history ranking helps only modestly.** Ranked on pre-Jun 2025 history and tested on later cost, the top 10% of wells catch 16.5% of later cost (95% interval 14.3–19.9%, 1.65× random). That is about the same as ranking by well size (15.9%), so the work list leads with recent failures and uses cost second. *Dashboard: [Validation & Data Quality](dashboard/Validation%20%26%20Data%20Quality%20-%201.png).*
- **Two actions pay back in about 2 years if they halve failures.** Rods are 76% of failures but 42% of repair dollars; pump and tubing are 22% of failures but 56% of dollars. A rod string upgrade or corrosion inhibition on 100 rod bad actors pays back in 1.8 years at 50% fewer failures (3.5 at 25%). Pump and tubing failure prevention on 26 wells pays back in 2.0 years (4.1 at 25%). Treating the top 25 wells costs C$0.85M and pays back in 1.1–2.3 years. In Cactus Lake, rods fail 0.55 times a year per well below 150 RPM vs 1.50 at 250–349 RPM, a cheap lever (an association, not a proven cause). *Dashboard: [Return on Action](dashboard/Return%20on%20Action%20-%20Budget.png) (payback, sensitivity, budget scenarios); failure vs dollar shares on [Cost Drivers](dashboard/Cost%20Drivers.png). The RPM rates are in `03b_runlife_payback` (`gold_failure_rates`), not on the dashboard.*
- **The recency watchlist is the early warning; the runtime rule adds reach.** On the 2026 holdout, the watchlist is right 28.8% of the time and catches 35% of failures, a median 22 days ahead. Adding the rule "2+ partial-run days in a week" raises failures caught from 500 to 813 (of 1,432) for about 9 alerts per area per week. Alerts where both fire are right 34.1% of the time, so crews work those first. *Dashboard: [Validation & Data Quality](dashboard/Validation%20%26%20Data%20Quality%20-%201.png); current alerts on [Work Order Priorities](dashboard/Work%20Order%20Priorities.png). The tier split is in `04b_ml_challenger`, not on the dashboard.*
- **A simple model beats the rules at the same workload, but only modestly.** Trained on 2025 and tested on 2026 with the same number of alerts as the rules, logistic regression and gradient-boosted trees gain 3–7 points of precision and catch 64–113 more failures (for example 36.5% vs 30.7% at the watchlist's volume). With short failure history and patchy torque data, we recommend the rules now and a model pilot as the next step. *Not on the dashboard: see [Alert tiers and an ML challenger](#alert-tiers-and-an-ml-challenger-2026-holdout) and `notebooks/04b_ml_challenger.py`.*

## Data

Three Parquet files provided by the organizers (not included in this repo):

| File                          | Grain       | Rows      | Wells | Dates                    |
| ----------------------------- | ----------- | --------- | ----- | ------------------------ |
| `daily_production.parquet`    | well × day  | 5,075,590 | 2,905 | 2021-01-01 to 2026-10-27 |
| `well_jobs.parquet`           | one job     | 15,610    | 1,903 | 2021-01-01 to 2026-10-27 |
| `production_failures.parquet` | one failure | 2,873     | 825   | 2021-12-14 to 2026-10-26 |

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
notebooks/        Databricks notebooks, run in order (04b is plain Python)
  01a_clean_daily    bronze → silver_daily, cleaning log
  01b_clean_jobs_failures  bronze → silver_jobs, silver_failures (failures linked to repair jobs)
  01c_downtime_features    downtime buckets, silver_downtime, rolling pump-sensor features (needs silver_daily)
  01d_daily_checks   lost oil by bucket, power-downtime seasonality, downtime per failure from the stop day (gold_failure_downtime)
  02a_ranking        gold_well_month, gold_well_period, cost score and ranks; out-of-time validation (gold_ranking_validation)
  03a_repeat_failures  re-failure@30/90, Rod Truck repeat rate by year, recency tiers (gold_failure_gap, gold_recency_tier)
  03b_runlife_payback  failure rates by pump make, model and RPM within area (gold_failure_rates); cost per failure and payback for 2 actions (gold_payback)
  03c_budget_scenarios  bad-actor wells ranked with Gamma–Poisson shrinkage; 25/50/100/200-well budget scenarios (gold_budget_wells, gold_budget_scenarios)
  04a_early_warning  sensors before failures, threshold grid, holdout evaluation and gold alert outputs
  04b_ml_challenger  alert tiers (n6) and a time-boxed ML challenger (n7) on the 2026 holdout (gold_alert_tiers, gold_model_eval, gold_model_weights)
  (05 reserved for weather: not used, power downtime is not seasonal)
dashboard/        AI/BI dashboard: Reliability Investment Dashboard.lvdash.json, build_dashboard.py (rebuilds it and tests every query), page screenshots
docs/             slides (.pptx and PDF), presentation script, figures, team docs
decision/         team planning pages (research question vote, build plan)
.claude/skills/   reference notes for Claude Code (dataset, domain, rubric) and sync-dashboard (copies dashboard-editor changes into build_dashboard.py)
AI_LOG.md         AI usage log (required disclosure)
```

## How to reproduce

1. Upload the three Parquet files to a Unity Catalog volume in a Databricks workspace.
2. Run the `notebooks/01a` to `01d` notebooks in letter order, then `02a_ranking` through `04b_ml_challenger` in order, on serverless compute.
3. Import `dashboard/Reliability Investment Dashboard.lvdash.json` (Dashboards → Create → Import from file), or rebuild it with `python dashboard/build_dashboard.py build` (`test` runs every dataset query on the SQL warehouse first).

Credentials are never stored in the notebooks or this repo. Use Databricks secrets or environment variables.

Team docs:

- [Databricks API and token setup](docs/databricks_api_token_setup.md): connecting the CLI or Python SDK from a laptop
- [Databricks ↔ GitHub working practice](docs/databricks_github_practice.md): Git folders, notebook ownership, table rules
- [Known issues](docs/known_issues.md): data problems found after the silver freeze
- [Notebooks and data flow](docs/notebooks_and_data_flow.md): naming conventions, data-flow diagram, and every Databricks table with its grain, writer and users
- [Checkpoint c2 sign-off](docs/c2_signoff.md): score definition, the 2 actions and the early-warning choice, with evidence
- [Build plan](docs/build_plan.md): tasks, owners, definitions and sourced inputs (snapshot of the team's live plan page)
- [Presentation script](docs/presentation_script.md): 5-minute video script with the dashboard demo, plus notes for a 10-minute finals version

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

**Payback.** Cost per failure (linked repair job + downhole lost oil from the day the well stopped to 14 days after the logged failure date) against the cost of an action, giving a break-even number of failures avoided per well-year, with a sensitivity grid. Bad actors are picked on Jun–Dec 2025 failures and their rate is measured on Jan–Oct 2026, so picking does not inflate the rate. Budget scenarios rank wells by expected savings, shrinking each well's failure rate toward its area rate (Gamma–Poisson empirical Bayes).

**Early warning.** Candidate rules on runtime instability (partial-run and fully down days in the last week) and on drops in pump efficiency, run only on producing days. The rule chosen on 2025 is 2+ partial-run days in a week; down-day and efficiency rules did not add precision. Torque and current are not used because their values repeat day to day. An alert counts as a hit if the well stops for a rod, pump or tubing failure 2–30 days later, with one alert per well per 14 days. The baseline to beat is a recency watchlist: wells with 2+ failures in the last 90 days.

## Validation

- **Ranking:** out of time. Rank on 2023–2024, measure the share of June 2025 – October 2026 cost captured by the top 5/10/20% of wells. Compare with random selection, with ranking by oil volume, and with a recency watchlist (2+ failures in the last 90 days). 95% intervals by bootstrap over wells.
- **Early warning:** thresholds on runtime instability and efficiency, chosen on June–December 2025 failures (aligned to the stop day) and frozen before testing on January–October 2026. Precision, recall, median lead time and alerts per area per week, compared with the recency watchlist and with random alerts at the same rate.
- **Payback:** sensitivity to netback, action cost and failure-reduction rate. Equipment effects are observational and reported as associations.

### Alert tiers and an ML challenger (2026 holdout)

We lead with transparent rules and kept machine learning to one time-boxed challenger, scored at the same workload as the rules. Both checks run in `notebooks/04b_ml_challenger.py`, which re-runs the `04a` alert test and asserts that it matches `gold_warning_eval` exactly (watchlist 28.8%, runtime rule 17.0%, either rule 21.6%; 500, 625 and 813 of 1,432 failures caught). The tables below are from the Databricks run (`gold_alert_tiers`, `gold_model_eval`), which the Validation & Data Quality tab reads.

**Alert tiers (n6).** We split the combined alert stream by which rule fired on the alert day:

| Tier                  | Alerts | Precision | Failures caught (of 1,432) |
| --------------------- | ------ | --------- | -------------------------- |
| 1 · both rules        | 273    | **34.1%** | 105                        |
| 2 · watchlist only    | 2,064  | 28.1%     | 442                        |
| 3 · runtime rule only | 2,914  | 12.9%     | 403                        |

Crews should work tier 1 first, then tier 2. Tier 3 alerts are right about one time in eight, so they are only worth it where crews have spare capacity.

**ML challenger (n7).** The model has one row per producing well every 14 days and is trained on Jun–Dec 2025 snapshots.

- **Label:** a rod, pump or tubing stop 2–30 days after the snapshot.
- **Features, all from before the snapshot:** recent failures, days since the last failure, last failure type, repair $, partial-run and down days, efficiency drift, area and pump make.
- **Test:** Jan–Oct 2026 snapshots, with no tuning on 2026. We take as many top-scored snapshots as the rule flags on the same snapshots, so the workload is the same.

| Same workload as | Method                 | Flagged | Precision | Failures caught (of 1,520) |
| ---------------- | ---------------------- | ------- | --------- | -------------------------- |
| Watchlist        | Recency watchlist      | 2,157   | 30.7%     | 477                        |
|                  | Logistic regression    | 2,157   | **36.5%** | 541                        |
|                  | Gradient-boosted trees | 2,157   | 34.1%     | 555                        |
| Either rule      | Either rule            | 3,182   | 24.3%     | 588                        |
|                  | Logistic regression    | 3,182   | **31.4%** | 688                        |
|                  | Gradient-boosted trees | 3,182   | 30.2%     | **701**                    |

At the same workload, the models catch 64–113 more failures than the rules (3–7 points more precision). That beats the rules, but only modestly. The logistic regression's largest weights are operating area (Winter higher, Fenn Big Valley lower), down days in the last 30 days (which lower the score) and repair $. So part of the gain is learning each area's base rate, which a rule could also use. It is one time split with no tuning. We keep the rules as the recommendation for now and treat the model as the next step to pilot once more failure history exists. We would adopt it only if it holds this edge without more alerts.

## Implementation plan (30/60/90 days)

**Value at stake.** Rod Truck and Workover spend on eligible wells was C$47.0M from Jun 2025 to Oct 2026 (about C$33M a year), plus C$8.4M of downhole lost oil at C$32.50/boe. Today's watchlist, the 114 wells with 2+ failures in the last 90 days, holds 24.9% of the last 12 months' repair spend on wells with a logged failure. _Sourced from field estimates in `well_jobs`; not invoices._

**Pilot areas: Cactus Lake and Winter.** Together they have 62% of failures since Jun 2025 (945 and 826 of 2,841), which gives the pilot enough events to measure in 90 days.

- **Cactus Lake is where the money is.** It accounts for about 42% of Rod Truck and Workover dollars, and 44% of its jobs are workovers.
- **Winter is where failures repeat.** 81.5% of its failures are followed by another on the same well within 90 days, against 57% overall. Most of its jobs are cheaper rod truck jobs.

| Days  | What happens                                                                                                                                                                                                                                                                                                                          | Owner (proposed)                                                                                |
| ----- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| 0–30  | A weekly reliability review in each pilot area, driven by the dashboard work list: recency first, then 12-month cost. Each well on the list gets a root-cause note.                                                                                                                                                                   | Area production engineer leads; the data owner refreshes the tables weekly                      |
| 31–60 | Pick a budget scenario. The top 25 wells cost C$0.85M and pay back in 2.3 years at 25% fewer failures, or 1.1 years at 50%. Run a root-cause review on every well with a second failure within 90 days. For Cactus Lake, also trial slower pump speeds on fast wells (rods fail 0.55/well-year below 150 RPM vs 1.50 at 250–349 RPM). | Operations superintendent approves the budget; rig and rod-truck coordinators schedule the work |
| 61–90 | Measure the KPIs against the 0–30 day baseline. Add the runtime alert rule only in areas whose crews can handle about 9 alerts per area per week. Expand area by area.                                                                                                                                                                | Reliability lead                                                                                |

**Resources.**

- **Money:** the action cost of C$34,152 per well is _assumed_ (the 2023+ Workover median), and the 25%/50% failure reduction is _assumed_ (it brackets one vendor case of about 34%). The netback (C$32.50/boe, Strathcona Q3 2026 operating earnings) and the repair costs are _sourced_.
- **Manpower** _(our estimate, to confirm with operations)_: the weekly review needs about 1 hour from the area engineer and a foreman. The data owner reruns the notebooks weekly.
- **Logistics** _(proposal)_: do each action when the rig or rod truck is already on the well for its next repair, so most actions need no extra mobilization. Rod Truck and Workover jobs run about 5% lower in Apr–May (2023–2025 average), which leaves room to schedule pilot work then.
- **Time:** savings start with the first avoided failure. Most repeat failures come back within about six weeks, so a change should show in the 90-day re-failure rate within one quarter.

**KPIs.** 90-day re-failure rate, failures per well-year, repair $ per well-year, downhole lost oil (m³), and alerts per area per week.

**Data fixes needed to run this for real.** Record the actual job cost (`actual_final_job_cost` is empty today). Keep a failure log from before Jun 2025 if one exists. Use one spelling per failure type. Get daily torque and current readings instead of repeated manual entries.

## Limitations

Each limitation says how it moves the results. "Conservative" means the true benefit is probably larger than reported.

| Limitation                                                                                              | Effect on results                                                                                                                                                                     |
| ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| The failure log starts in Jun 2025, so bad actors are picked on 7 months and measured on 10.            | Wide intervals on rates and payback. Measuring on the months used to pick would **overstate** the rates (rods 6.99/yr while picking vs 4.05/yr after), so we measure on later months. |
| Rod Truck cost is missing before 2023, and earlier history uses repair jobs as a stand-in for failures. | The ranking starts in 2023. History-based ranking only modestly beats ranking by well size (top 10% catches 16.5% vs 15.9% of later cost).                                            |
| Repair cost is the field estimate (`field_est_total`); invoiced cost is empty.                          | Direction **unknown**. Payback scales with cost per failure: 20% higher real costs shorten payback by about a sixth.                                                                  |
| The netback is company-wide operating earnings (C$32.50/boe), not a heavy-oil netback.                  | Small effect. Netback ±25% moves Action 1's payback from 1.77 to between 1.65 and 1.90 years at 50% fewer failures. Repair dollars, not lost oil, drive the savings.                  |
| `lost_oil_m3` is an operator estimate and is null on 67.5% of daily rows; nulls count as zero.          | Lost-oil value is **understated**, so savings are understated and payback is **conservative**.                                                                                        |
| Downtime codes are only partly mapped to causes; unmapped codes fall in "Other".                        | Downhole lost oil per failure is likely **understated** (conservative), and the cause split on the Cost Drivers tab is approximate.                                                                 |
| `failure_start_date` is logged a median 2 days after the well stops.                                    | Handled by anchoring on the stop day. Any stops we miss would **understate** lost oil per failure (conservative).                                                                     |
| Action cost (C$34,152) and its effect (25% or 50% fewer failures) are assumptions, not measured.        | Payback is proportional to both: half the action cost halves payback (1.77 → 0.88 years for Action 1 at 50%). We report the range, not one number.                                    |
| Equipment and RPM comparisons are observational.                                                        | Harder wells may get particular pumps or run faster, so switching equipment may deliver **less** than the gap suggests. Reported as associations.                                     |
| Torque, current and RPM repeat day to day, and torque covers 6% of Cactus Lake failures.                | The early warning uses runtime only, so its recall (43.6%) is a **lower bound** on what daily sensor readings could give.                                                             |

## External resources

- Organizer materials: Briefing Document, Research Topic Guide, Dataset Handbook
- Strathcona Resources, Q3 2026 results (release dated Nov 8, 2026), as reported by [Cantech Letter](https://www.cantechletter.com/newswires/strathcona-resources-ltd-reports-third-quarter-2026-financial-and-operating-results-and-announces-quarterly-dividend/): operating earnings C$32.50 per boe (value of a lost barrel), WCS price, operating and transport costs, Lloydminster Conventional scale.
- Western Energy Services, Q3 2026 results, as reported by [BOE Report](https://boereport.com/2026/10/28/western-energy-services-corp-releases-third-quarter-2026-financial-and-operating-results/): C$921 per service hour, used only to cross-check job cost estimates.
- [Weatherford COROD MER case study](https://www.weatherford.com/documents/real-result/production/artificial-lift-systems/corod-mer-continuous-sucker-rod-increases-average-run-life-of-rod-strings-by-51-in-six-highly-corr) (six highly corrosive wells): rod run life up 51%, used only to bracket the avoided-failure assumption.
- [Weatherford corrosion-inhibition case study](https://www.weatherford.com/documents/real-result/production/artificial-lift-systems/effective-inhibition-program-provides-significant-increase-in-corod-continuous-rod-run-life-in-high) (COROD run life with an inhibition program): qualitative support only.
- [PetroWiki: progressing cavity pump (PCP) systems](https://petrowiki.spe.org/Progressing_cavity_pump_(PCP)_systems)
- Weather: not used. Power/grid downtime showed no repeatable seasonal pattern (`01d_daily_checks`).

## Links

- 5-minute project summary video: [Google Drive](https://drive.google.com/file/d/14bfAo_ZX2ACkp-aLTRX4uevqV8wnodu_/view?usp=sharing)
- Presentation slides: [Google Drive](https://drive.google.com/file/d/12yT-Dv0-XvuMdgJEM8Tx5yQf2_jJ9fFr/view?usp=sharing); copy in the repo: [Bad Actors - DSMLC Datathon 2026.pdf](docs/Bad%20Actors%20-%20DSMLC%20Datathon%202026.pdf) ([.pptx](docs/Bad%20Actors%20-%20DSMLC%20Datathon%202026.pptx), with speaker notes)
- Script: [docs/presentation_script.md](docs/presentation_script.md)
- AI usage log: [AI_LOG.md](AI_LOG.md)
- Dashboard screenshots (Databricks AI/BI dashboard, "Reliability Investment Dashboard"):
  - [Overview](dashboard/Overview.png): the whole story on one page, with the well map
  - [Cost Drivers](dashboard/Cost%20Drivers.png): where repair dollars and lost oil go
  - [Recurrence Analysis](dashboard/Recurrence%20Analysis.png): how often failures come back
  - [Work Order Priorities](dashboard/Work%20Order%20Priorities.png): this week's ranked work list and runtime alerts
  - [Return on Action](dashboard/Return%20on%20Action.png): payback per action and the sensitivity table
  - [Return on Action – Budget](dashboard/Return%20on%20Action%20-%20Budget.png): budget scenarios and the wells to treat
  - [Validation & Data Quality](dashboard/Validation%20%26%20Data%20Quality%20-%201.png): out-of-time tests of the ranking and the alert rules
  - [Validation & Data Quality, continued](dashboard/Validation%20%26%20Data%20Quality%20-%202.png): machine-learning comparison and the cleaning log
