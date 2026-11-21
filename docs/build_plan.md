# Reliability Desk: build and pitch plan

Strathcona datathon · Team plan v2 · Nov 21 · ~15 working hours · 3 people · Submit Sun Nov 22, 11:00 AM (deadline 12:00 PM)

> One project, two deliverables. A Databricks dashboard that tells a foreman which wells to act on this week, and a pitch that costs the rollout. The story: Strathcona's rod failures are chronic, not episodic. 57% of failures are followed by another on the same well within 90 days, so we target repeat-failure wells, show what that saves, and show how to run it.

Live version with the shared checklist: https://claude.ai/artifact/Fs6rV9pyD5qwtpEH3GEozY. This file is a snapshot of that page taken Nov 21; ticks and later edits live on the page.

Every task opens into work instructions: inputs, exact steps, output, a "done when" test, and the evidence it rests on. Tick tasks as you finish; progress is shared with the team. This is v2: new tasks are n1 to n5. Task IDs are unchanged, so ticks from v1 carry over. Owners are suggestions; rebalance as needed.

## Who does what, starting now

Same skeleton as v1: same task IDs, same owners, same checkpoints. Each lane is in the order to work it. Friday tasks (f1 to f6) and Checkpoint 1 are assumed done; if any are not, they come first. Task details are in the timeline below.

**Angie · data, ranking, dashboard, repo**

Start with s1. Jacob and Ananya both need `gold_well_month`.

1. **s1** Monthly well table and cost score v1 (Sat 9–11 AM)
2. **s4** Out-of-time validation. Rank on cost score, then on cost score plus recency, and compare both with the size baseline (11 AM–1 PM)
3. **s7** Dashboard page 2, the weekly work list. Needs `gold_recency_tier` from n2 (1–3 PM)
4. **n5** Dashboard page 1, losses and data trust, then **s10** polish and screenshots (3–5 PM)
5. **s13** README findings and methods (5–8 PM)
6. **u1** check every slide number, **u4** repo cleanup (Sun)

Hands off: `gold_well_month` to Jacob after s1; the ranking to everyone at the 1 PM checkpoint.

**Jacob · repeat failures, payback, budget**

Start with n2. The headline, the work list and the strategy case all lean on it. Aim to finish before 11 AM.

1. **n2** Repeat-failure analysis and recency tiers (Sat 9–11 AM)
2. **s5** Pick 2 actions and compute payback, by failure type and dollars (11 AM–1 PM)
3. **n4** Budget scenarios, then **s8** dashboard page 3 (1–3 PM)
4. **s11** Sensitivity table and limitations (3–5 PM)
5. **s14** Slides, one storyline (5–8 PM)
6. **u2** final slide pass, **u5** upload video and add links (Sun)

Stretch only: **s2** failure rates by equipment, if n2 and s5 land early. It is no longer a must-have.

Hands off: `gold_failure_gap` and `gold_recency_tier` to Angie (s7) and Ananya (n3).

**Ananya · daily file, sensors, strategy, story**

Start with n1. It takes about half an hour and its three tables decide how much of the waterfall and sensor work is real.

1. **n1** Daily-file checks in Databricks (Sat 9–11 AM)
2. **s3** Look at sensors before 50 failures (9–11 AM)
3. **s6** Draft threshold rules, tune on Jun–Dec 2025, must beat the recency baseline (11 AM–1 PM)
4. **s9** Test thresholds on 2026 and build dashboard page 4 (1–3 PM)
5. **n3** Strategy case and 30/60/90 plan; **s12** weather only if the 3 PM check says go (3–5 PM)
6. **s15** Video script and chart exports (5–8 PM)
7. **u3** record the video, **u6** prep 10 judge questions (Sun)

Hands off: n1 results to Angie (n5) and Jacob (downtime days for payback).

| Checkpoint | When | What must be true |
|---|---|---|
| Ranking and actions signed off (c2) | Sat 1 PM | s4 lift chart and s5 payback reviewed together; n2 numbers confirmed. |
| Weather go or no-go (c3) | Sat 3 PM | All dashboard pages work, and n1 shows power downtime is seasonal. Otherwise skip weather. |
| Draft slides and script (c4) | Sat 8 PM | Read aloud and timed; each person challenges one number. |
| Slides and video locked (c5) | Sun 9 AM | No more content changes. |
| Repo link posted (c6) | Sun 11 AM | Posted in #submissions before the 12 PM deadline. |

## Original plan or a new path?

We are following the original plan. The skeleton, owners, checkpoints and locks are unchanged. v2 changes the headline, two data windows and a few details, and adds five tasks.

**Kept**

- Every v1 task ID, owner and checkpoint
- Clean tables with a cleaning log
- Cost score and out-of-time validation against random and size baselines
- Payback for 2 actions with a sensitivity grid
- Databricks dashboard, README, video, slides
- Weather capped at 2 hours

**Changed**

- Headline: repeat failures, not only a bad-actor list
- Train window 2023–24; failure window starts Jun 2025
- Early warning must beat the recency baseline
- Actions chosen by failure type and dollars
- Dashboard is four pages that match the pitch storyline
- Weather points at power downtime in the daily file

**Added or dropped**

- Added: n1 daily-file checks, n2 repeat failures, n3 strategy and 30/60/90, n4 budget scenarios, n5 losses page
- Dropped: failure-to-repair lag test (not measurable)
- Moved to stretch: s2 equipment failure rates

## What v2 changes, and why

We re-profiled the jobs and failures files (15,610 jobs, 2,873 failures) and tested the ideas behind this plan. Eight findings change what we build. Every number below was recomputed from the two files; none of it comes from the daily file.

| Finding | Evidence in the files | What changes |
|---|---|---|
| **Failures repeat** | 57% of failures are followed by another on the same well within 90 days (28% within 30). Rod failures: 66%, median gap 43 days. Pump and tubing: about 30%. Rod Truck jobs show 58–60% in each of 2023, 2024, 2025 and 2026, so it is not a logging artefact. | This is the pitch headline and dashboard page 2. New KPI: 90-day re-failure rate (task n2). |
| **A no-ML watchlist already works** | Wells with 0, 1, 2 and 3+ failures in the last 90 days fail again in the next 90 days 26%, 40%, 61% and 70% of the time (735 wells, 24 two-week snapshots). Flag at 2+: precision 64%, recall 34%. | Sensor rules must beat this baseline (s6, s9). If they do not, the baseline is the early warning. |
| **Failure log starts in June 2025** | Jan–Apr 2025 has 3 rows, May has 27, then 128–197 a month. | Failure window is Jun 2025–Oct 2026 (16.8 months). Tune on Jun–Dec 2025, test on Jan–Oct 2026. Three failures is at least 2.1 a year, not 1.6. |
| **Rod Truck cost is missing before 2023** | Share of Rod Truck jobs with cost above zero: 2021 0.1%, 2022 40%, 2023 onward 99.5% or more. | Train window is 2023-01-01 to 2024-12-31. The $212.4M total and the old “top 10% = 31%” understate 2021–22. Since Jun 2025, top 10% of wells hold 33% of repair $ and top 20% hold 50% (975 wells). |
| **Repair lag is not measurable; downtime before the log is** | 2,137 of 2,812 linked failures (76%) have a job starting the same day. `failure_end_date` is empty, job duration median is 0.4 days. But the daily file shows 66% of 2025 rod/pump failures were already down on the logged day or the day before; for those, the median gap from stop to logged date is 2 days (90th percentile 7). | Drop the failure-to-repair lag test (old H3). Anchor early warning and lost oil on the **stop day**, not `failure_start_date` (s5, s6, n1). |
| **Weather is a weak lead for failures** | Failures run flat at about 170 a month with no winter surge (Jan–Mar 2026: 175, 165, 182; Jun–Sep 2025: 172–178). Apr–May Rod Truck jobs run 8% below expected (1,041 vs 1,128, 2022–25). | Weather stays capped at 2 hours and aims at power/grid downtime in the daily file. The pitch says failures are chronic, not weather-driven. |
| **Rods are the count, pump and tubing are the dollars** | Rods are 76% of failures at a median $5.0k. Pump is $29.9k and tubing $35.8k. Pump and tubing are 22% of failures but about 55% of failure-linked repair $. Fatigue and corrosion both cost about $5.4k median. | Pick the two actions by failure type and dollars first, then by cause (s5). |
| **History-only ranking is a modest lift** | Ranking wells on 2023–24 repair $ puts 16.2% of later repair $ in the top 10% (95% interval 13.3–19.2%), 1.6 times random. A re-run over all 1,358 wells with any repair $ gives 14.2%, so the result depends on the well universe. | Add recency to the score and report the lift as measured. Fix one universe definition in s4. Expect the size baseline to be tight. |
| **Torque, current and RPM are repeated entries** | On rate days they equal the previous day's value 90%, 95% and 96% of the time (median 29, 11 and 13 distinct values per well since Oct 2024). Efficiency repeats 0.6%. Torque covers 61% of 2025 rod/pump failures overall but only 6% in Cactus Lake. | No torque or current drift rules. Early warning uses runtime, efficiency and fluid rate (s6). Pitch recommendation: feed daily historian torque and current into the data. |
| **No sensor drift before failures; runtime instability instead** | Re-aligned to the stop day vs 1,500 normal well-days: median torque and current stay at 1.000–1.005 of baseline. In days −8 to −2: ≥1 partial-run day 34.1% vs 14.9%; ≥2 partial days 11.0% vs 3.5%; ≥1 down day 13.8% vs 3.7%. Efficiency ≤80% of baseline 5.4% vs 4.0%. | s6 rules are built on partial-run and down days plus efficiency, and must still beat the recency watchlist. Charts: `docs/figures/prefailure_*.png`. |

> **Partly re-checked:** the daily file. Sensor behaviour and stop timing were re-checked on Nov 21 (local pandas run, s3). Barrels, pump make and downtime buckets still rest on the Nov 20 profiling; task n1 re-runs them in Databricks. Cactus Lake holds $26.8M of $64.1M repair $ since Jun 2025 (42%) at about $21k per job, against about $7k in Winter. The gap is job mix: workovers are 43% of Cactus Lake jobs and 14% of Winter jobs. Cactus Lake workovers alone are $21.1M, and pump repair or replacement is the largest line ($8.0M, 229 jobs). Druid Main also runs 43% workovers.

## Sourced cost and price inputs

Every input to the payback model, where it comes from, and how solid it is. Show the status on the slide. Dollar figures are Canadian unless marked.

| Input | Value | Source | Status and use |
|---|---|---|---|
| Value of a lost barrel | C$32.50 per boe (Q3 2026); C$28.85 (nine months 2026); C$35.66 (Q2 2026) | Strathcona Q3 2026 results, Nov 8, 2026. Operating earnings, all segments. | Sourced, but company-wide, not heavy-oil only. Base case for the cost score. That is about C$204 per m³ (C$32.50 × 6.2898, derived). |
| Oil price | WCS Hardisty C$100.56 per bbl; WTI US$85.79 per bbl (Q3 2026) | Same release | Sourced. Gross price, not margin. Context for the pitch only (about C$633 per m³ gross, derived). |
| Cost to produce and move a barrel | Operating C$5.77 (energy) and C$10.56 (non-energy) per boe; transport and processing C$9.13 per boe | Same release | Sourced, company-wide. Shows why margin is far below price. |
| Lloydminster Conventional scale | About 20,000 bbl/d, steady quarter over quarter | Same release | Sourced. Release credits the Cactus Lake polymer flood for base performance and cites facility downtime at other fields. |
| Repair cost per job | Rod Truck median $4,374; Workover median $34,152 | Competition jobs file, field estimates, 2023 onward | From the data. Not actual cost; `actual_final_job_cost` is empty. |
| Service rig rate cross-check | C$921 per service hour (Q3 2026); 38% Canadian utilization | Western Energy Services Q3 2026 results, well servicing segment | Sourced. At that rate a $34,152 workover is about 37 service hours and a $4,374 rod truck job about 5. Plausible, but field estimates include parts, so this is a cross-check only. |
| Avoided-failure share | Grid of 25% and 50%. One case: rod run life up 51% across six wells, about 34% fewer failures; workover frequency down 24% | Weatherford COROD MER case study, Colombia | Vendor case, six wells, different geology. Use it to bracket the grid, never as the expected result. |
| Corrosion inhibition effect | One Alberta well: rod run life from 90 days to more than 7 years on 35 L of inhibitor per week, 99% water cut | Weatherford COROD inhibition case study, Alberta | One well, vendor claim, no cost given. Qualitative support only; do not quote as typical. |
| Action cost (rod string upgrade, inhibitor program) | Not found | None | Assumed: Workover median $34,152 as an install-job proxy. Ask a mentor for a real range. |
| Do not transfer | US$68,000 saved per rod intervention | Same Colombia case | Rig-based intervention cost basis. Our rod truck median is $4,374, so this figure does not apply. |

### Sources

- Strathcona Resources, Q3 2026 results (release dated Nov 8, 2026), as reported by Cantech Letter
- Western Energy Services, Q3 2026 results, as reported by BOE Report
- Weatherford, COROD case study: MER (Colombia, six wells)
- Weatherford, COROD case study: corrosion inhibition (Alberta, one well)

Found by the team, not re-checked here. Add the links to the README under External resources, and open Strathcona's MD&A to confirm what the C$32.50 per boe measures before it goes on a slide.

> **The netback choice changes what the score measures.** Downhole lost oil is 219,627 m³ over 2021–26 (n1, full bucket map), about 45,000 m³ a year in 2024–25. At C$204 per m³ that is about C$9M a year, against about C$46M a year of repair spend since Jun 2025. Repair dollars dominate the cost score at this value, and lost oil only looms large at a gross price (about C$633 per m³). Report the two halves separately and say so on the slide.

## What the data already tells us

From our profiling run on Nov 20 (pandas 1.5.3, pyarrow 17). Re-run the numbers in Databricks before they go on a slide.

| Fact | Value | Why it matters for the plan |
|---|---|---|
| Daily rows / wells / days | 5,075,590 / 2,905 / 2,126 | Use Spark. Full file needs ~7.6 GB RAM in pandas. |
| Failures by year | 2021: 1 · 2022: 1 · 2023–24: 0 · 2025: 1,230 · 2026: 1,641 | Failure log is live from Jun 2025 only (see v2 changes). Earlier history has to use jobs. |
| Failures with a Rod Truck/Workover job within −3…+14 days | 2,812 of 2,873 | Jobs are a valid stand-in for failures, and job cost is the failure's repair cost. Don't add both. |
| Rod Truck + Workover jobs with cost > 0 | 11,785 jobs · $212.4M · 1,518 wells | The repair-$ half of the score. Rod Truck cost is missing before 2023, so rank on 2023 onward. |
| Median field estimate (2023 onward, cost above zero) | Rod Truck $4,374 · Workover $34,152 | Typical repair cost; workover cost is a proxy for an install/upgrade job. The old $2,846 and $31,873 were pulled down by jobs with no cost recorded. |
| Lost oil, downhole codes (`Downhole Equipment`, `Downhole`, `RODSPART`) | 213,499 m³ · 1,209 wells | The lost-oil half of the score. Only 3 of 72 codes so far, so it is a floor. |
| Top 10% of 2025+ producers (1,150 wells) | 40% of failures · top 20% = 63% | Concentration is real: targeting a short list is worth it. |
| Wells with ≥3 failures | 374 wells · 79% of failures (Jun 2025 on) | Candidate bad actors for payback. They are 46% of the 814 failing wells; wells with 2+ failures hold 90%. |
| Top-10% by job count 2021–23 still top 10% in 2024–26 | 31% (Spearman 0.30) | ~3× random. A jobs-only re-run gives 35% (Spearman 0.24). It predicts, but must refresh each period. |
| Overlap of top-10% repair-$ and top-10% lost-oil wells | 64 of ~150 | Combine both in the score; neither alone finds all costly wells. |
| Repair $ since Jun 2025 (Rod Truck + Workover) | $64.1M · about $46M a year | Field estimates. Cactus Lake $26.8M, Druid Main $8.2M, Winter $7.5M, Cosine $5.7M, Court $4.4M. |
| Median linked repair cost by failure type | Rods $5.0k · Pump $29.9k · Tubing $35.8k | Pump and tubing are about 55% of failure-linked repair $ with 22% of failures. |
| Rod Truck jobs with cost above zero | 2021 0.1% · 2022 40% · 2023+ 99.5%+ | Ranking and payback use 2023 onward. |
| Rod Truck jobs that are “Parted” (Jun 2025 on) | 1,453 of 2,561 (57%) | Parted rods dominate the work. 89% of these jobs have a failure record within 14 days. |
| Failure causes | Fatigue 1,225 · Corrosion 789 · Load/Excess stress 291 · Erosion 94 · Abrasion 56 | Picks the 2 actions for payback. |
| Failure types | Rods 2,186 · Pump 381 · Tubing 260 | Rods are ~76% of failures; the payback story centres on rods. |
| Failures by area | Cactus Lake 957 · Winter 829 · Cosine 255 · Druid Main 238 · Court 224 · Bodo East 181 | Compare equipment within these areas, not across them. |
| Sensor coverage (null share) | RPM 52% · torque 74% · current 82% · efficiency 67% | Early warning only works on instrumented wells (1,578 ever have RPM). Check coverage on failure wells first. |
| Power/grid downtime rows | `Power Source/Supply` 17,148 · `Sask Power/Grid` 6,861 | The most plausible weather link. |
| `Uneconomic - Long Term Shut In` | 1.47M rows (~29%) · 162k m³ "lost" | Excluded from the score: it is a deliberate economic decision, not a reliability loss. |

## Shared definitions

Everyone uses these exact definitions so the numbers match across notebooks, dashboard and slides.

### Windows

- **Train:** 2023-01-01 to 2024-12-31 (24 months; Rod Truck cost is 99.5% complete from 2023)
- **Test:** 2025-06-01 to 2026-10-27 (~16.8 months, the same span as the failure log)
- **Failure window:** 2025-06-01 to 2026-10-27. Tune on Jun–Dec 2025, test on Jan–Oct 2026.

### Eligible producer

`flow_direction = 'production'` and at least 30 days with `oil_produced_m3 > 0` in **both** train and test windows. Wells that start after 2024 are reported separately as "new, unranked".

### Repair $

Sum of `field_est_total` for jobs with `job_category IN ('Rod Truck','Workover')` and `field_est_total > 0`, after dedupe. Not actual cost (`actual_final_job_cost` is empty).

### Downhole lost oil

Sum of `lost_oil_m3` on days whose normalized downtime bucket is **Downhole** (rods, pump, tubing, torqued, unspecified downhole). Bucket map built in task f6.

### Cost score

```
cost_score = repair_$
           + downhole_lost_oil_m3 × 6.2898 × netback_per_bbl
```

`netback_per_bbl`: C$32.50 per boe, Strathcona's Q3 2026 operating earnings (release dated Nov 8, 2026). It is company-wide, not heavy-oil only; confirm the definition in the MD&A and record the page. Full table of sourced inputs above.

### 90-day re-failure rate

```
re-failure@90 = failures followed by another
                on the same well within 90 days
                / failures with 90 days of follow-up
```

Drop failures in the last 90 days of data (no follow-up yet). Report by failure type and area. Our event-log value: 57% overall, 66% rods.

### Recency tier

Failures on the well in the last 90 days: 0, 1, 2, or 3+. Wells with 2+ are the watchlist baseline: precision 64%, recall 34% for another failure in the next 90 days.

### Capture@k (the headline metric)

```
capture@10% = test-window cost of the top-10%
              (ranked on train) / total test-window cost
random baseline ≈ 10%    lift = capture / 0.10
```

Also beat a **size baseline**: rank by train-window oil volume. Bigger wells may simply cost more.

## MVP scope

**Must-have**

1. **Clean tables** in Databricks (bronze, silver, gold), every cleaning rule logged.
2. **Repeat-failure view:** 90-day re-failure rate by failure type and area, recency tiers, chronic-well list.
3. **Work list** ranked on cost score plus recency, validated out of time: capture@10%, lift, size baseline, bootstrap intervals.
4. **Payback for 2 actions**, picked by failure type and dollars, with 3–4 precomputed budget scenarios and a sensitivity grid.
5. **Early-warning rules** that must beat the recency baseline; otherwise the baseline ships as the watchlist.
6. **Dashboard, 4 pages:** where barrels and dollars go, weekly work list, payback and budget, watchlist and data trust.
7. **Pitch as one storyline** (situation, complication, recommendation, implementation) with a 30/60/90 plan covering money, manpower, logistics and time. 10-minute stage deck, 5-minute video, README, AI log.

**Nice-to-have · only if must-haves done by Sat 3 PM**

- **Weather link**, 2-hour cap, aimed at power/grid downtime in the daily file. Failures show no winter surge, so pull weather only if power downtime is seasonal.
- Genie space on the gold tables for the demo.
- Kaplan–Meier run-life curves (needs the `lifelines` package, a new dependency).

Weather source not confirmed. Jacob checks Databricks Marketplace on Free Edition first. Fallback: Open-Meteo historical API or ECCC daily stations at the 26 field-area centroids.

**Cut**

- ML models
- Canary
- Text mining of job summaries and failure comments (13% of failures name a depth)
- Repair-lag and response-time analysis (failure and repair start the same day in 76% of cases)
- Polymer flood analysis
- Normalizing all 72 downtime codes (top codes by lost oil only)

## Schedule and work instructions

Owners: A = Angie, J = Jacob, N = Ananya. ◆ marks a checkpoint.

| When | ID | Task | Owner | Notebook |
|---|---|---|---|---|
| Fri 8–9 PM | f1 | Load the 3 files into Databricks | Jacob |  |
| Fri 8–9 PM | f1b | Request Databricks credits in #databricks-request | Jacob |  |
| Fri 8–9 PM | f3 | Check weather data availability | Jacob |  |
| Fri 8–9 PM | f2 | Repo skeleton, README sections, AI log | Angie |  |
| Fri 8–9 PM | f3b | Post organizer questions in Discord | Ananya |  |
| Fri 9–11 PM | f4 | Clean daily table (silver_daily) | Angie | `notebooks/01_clean_daily.py` |
| Fri 9–11 PM | f5 | Clean jobs and failures (silver_jobs, silver_failures) | Jacob | `notebooks/01_clean_jobs_failures.py` |
| Fri 9–11 PM | f6 | Downtime buckets + daily sensor features | Ananya | `notebooks/01_downtime_features.py` |
| Fri 11 PM | c1 | Clean tables frozen | Everyone |  |
| Sat 9–11 AM | n1 | Run the daily-file checks in Databricks | Ananya | `notebooks/01_daily_checks.py` |
| Sat 9–11 AM | n2 | Repeat-failure analysis and recency tiers | Jacob | `notebooks/03_repeat_failures.py` |
| Sat 9–11 AM | s1 | Monthly well table + cost score v1 | Angie | `notebooks/02_ranking.py` |
| Sat 9–11 AM | s2 | Failure rates by equipment, within area | Jacob | `notebooks/03_runlife_payback.py` |
| Sat 9–11 AM | s3 | Look at sensors before 50 failures | Ananya | `notebooks/04_early_warning.py` |
| Sat 11 AM–1 PM | s4 | Out-of-time validation of the ranking | Angie | `notebooks/02_ranking.py` (section to add) |
| Sat 11 AM–1 PM | s5 | Pick 2 actions and compute break-even payback | Jacob | `notebooks/03_runlife_payback.py` (section to add) |
| Sat 11 AM–1 PM | s6 | Draft threshold rules, tune on Jun–Dec 2025 | Ananya | `notebooks/04_early_warning.py` (section to add) |
| Sat 1 PM | c2 | Ranking and actions signed off | Everyone |  |
| Sat 1–3 PM | n4 | Budget scenarios (precomputed) | Jacob |  |
| Sat 1–3 PM | s7 | Dashboard page 2: weekly work list and repeat failures | Angie |  |
| Sat 1–3 PM | s8 | Dashboard page 3: payback and budget scenarios | Jacob |  |
| Sat 1–3 PM | s9 | Test thresholds on 2026 + dashboard page 4: watchlist | Ananya | `notebooks/04_early_warning.py` (section to add) |
| Sat 3 PM | c3 | Weather decision made | Everyone |  |
| Sat 3–5 PM | n3 | Strategy case and implementation plan (30/60/90) | Ananya |  |
| Sat 3–5 PM | n5 | Dashboard page 1: where barrels and dollars go | Angie |  |
| Sat 3–5 PM | s10 | Dashboard polish + optional Genie space | Angie |  |
| Sat 3–5 PM | s11 | Sensitivity table + limitations list | Jacob |  |
| Sat 3–5 PM | s12 | Weather join + 3 checks (2 h cap), or threshold polish | Ananya |  |
| Sat 5–8 PM | s13 | README findings and methods | Angie |  |
| Sat 5–8 PM | s14 | Slides: problem, data, method, results | Jacob |  |
| Sat 5–8 PM | s15 | Video script + slide visuals | Ananya |  |
| Sat 8 PM | c4 | Draft slides and script reviewed | Everyone |  |
| Sun 8–9 AM | u1 | Check every slide number against the code | Angie |  |
| Sun 8–9 AM | u2 | Final slide pass | Jacob |  |
| Sun 8–9 AM | u3 | Record the video | Ananya |  |
| Sun 9 AM | c5 | Slides and video final | Everyone |  |
| Sun 9–11 AM | u4 | Repo cleanup; notebooks run end to end | Angie |  |
| Sun 9–11 AM | u5 | Upload video and slides; add links to README | Jacob |  |
| Sun 9–11 AM | u6 | Prep 10 hard judge questions | Ananya |  |
| Sun 11 AM | c6 | Repo link posted | Everyone |  |

### Fri 8–9 PM

#### `f1` Load the 3 files into Databricks · Jacob

**Inputs:** `dataset/*.parquet` (202 MB + 3.5 MB + 91 KB)

1. Upload the 3 files to a Unity Catalog volume, e.g. `/Volumes/main/strathcona/raw/`.
2. Create bronze Delta tables as-is: `bronze_daily`, `bronze_jobs`, `bronze_failures`.
3. Confirm row counts match the handbook: 5,075,590 / 15,610 / 2,873.
4. Share the catalog/schema names in the team chat.

**Output:** 3 bronze tables, row counts posted.

**Done when:** `SELECT COUNT(*)` matches all three counts.

#### `f1b` Request Databricks credits in #databricks-request · Jacob

1. Post team name, member names and Databricks Workspace ID in `#databricks-request`.
2. Watch Discord DMs for instructions.

**Output:** Credits applied.

**Done when:** Workspace shows the extra credits.

#### `f3` Check weather data availability · Jacob

1. Search Databricks Marketplace for free historical daily weather covering SK/AB, 2021–2026 (min/max temp, precipitation, snow).
2. Confirm Free Edition can install it (some listings need a paid workspace).
3. If not available: test one call to the Open-Meteo historical API, or one ECCC daily station download, for one area centroid.
4. Note in chat: source, variables, date coverage, and licence.

**Output:** One-line verdict: source + variables, or 'not available'.

**Done when:** We know by 9 PM whether weather is possible, and from where.

> **Evidence:** Weather is nice-to-have. Even if a source exists, the join only starts after the Sat 3 PM go/no-go.

#### `f2` Repo skeleton, README sections, AI log · Angie

1. Create a public GitHub repo. Keep `dataset/` in `.gitignore` (raw data stays out; ask organizers before publishing it).
2. Folders: `notebooks/` (01_clean, 02_ranking, 03_runlife_payback, 04_early_warning, 05_weather), `src/` (shared cleaning functions), `dashboard/` (exported dashboard JSON + screenshots), `docs/` (slides PDF, figures).
3. README sections required by the brief: title, team members, research question, motivation, summary of findings, methods, validation, external resources, links to video and slides, AI disclosure.
4. Add `AI_LOG.md` with columns: time, tool, what we asked, what we used, how we verified.
5. Add `docs/known_issues.md` for data fixes found after the freeze.

**Output:** Repo URL with skeleton, shared with the team.

**Done when:** Every required README heading exists, even if empty.

#### `f3b` Post organizer questions in Discord · Ananya

1. Post the 9 questions from our notes: failures only 2025+; `choke_open_pct` scale; negative volumes; whether `field_est_total` is full cost and a price to use; preliminary rubric and slide limits; video length; repo snapshot timing and raw data in public repo; Databricks/Genie judged; AI disclosure detail.
2. Record answers in the rubric notes as they come in.

**Output:** Questions posted.

**Done when:** Answers logged, or a follow-up time set.

### Fri 9–11 PM

#### `f4` Clean daily table (silver_daily) · Angie · `notebooks/01_clean_daily.py`

**Inputs:** `bronze_daily`

1. Cast: `production_date` to date; `surface_latitude`, `surface_longitude`, `pcp_pump_rpm` from decimal to double.
2. Dedupe on `(uwi, production_date)`: 29 duplicate rows. Inspect them first; keep the row with more non-null fields.
3. Pressures: set `tubing_pressure_kpa` = 999999 to null (7 rows). Null any tubing/casing pressure < 0 or > 20,000 kPa (tubing: 3,033 neg, 1,155 high; casing: 27,214 neg, 819 high).
4. Volumes: keep the raw columns; add `oil_clean`, `water_clean` with negatives set to null (oil 3,720 rows, water 10,284). Negatives look like accounting corrections; Discord Q3 confirms.
5. Pump sensors: null `pcp_pump_rpm` > 1,000 (p99 is 453, max 44,180). Null current and torque above their p99.9. Null `pump_efficiency_pct` < 0 or > 2 (115,149 rows are > 1; max 91.4).
6. Normalize `pcp_pump_make` (~32 spellings): Weatherford ← WEATHERFORD, WEATHWEFORD, Weeatherford, WFD, WTF, 'Weatherford '; Mantl ← MANTL, MAntl, Mantl, Mantle New; Europump ← EUROPUMP, Euro Pump, Eurppump, 'Europump (PCM)'; Kudu ← KUDU; PCM ← pcm, PCm; BMW ← 'BMW Pump'; LSI ← 'Lifting Solutions'. Mark 'PCP', 'PCP Pump' as unknown.
7. Add `water_cut` = water / (oil + water) when the sum > 0, and `runtime_frac` = `hours_on_prod` / 24.
8. Write every rule and its row count to a `cleaning_log` table.

**Output:** `silver_daily` + `cleaning_log`.

**Done when:** Zero duplicate well-days; no pressure > 20,000; row count = 5,075,590 − duplicates removed.

> **Evidence:** All counts above are from our profiling run. `hours_on_prod + hours_down` = 24 on every row, so either column can be used.

#### `f5` Clean jobs and failures (silver_jobs, silver_failures) · Jacob · `notebooks/01_clean_jobs_failures.py`

**Inputs:** `bronze_jobs`, `bronze_failures`

1. Jobs: dedupe on `(uwi, start_date, job_type, job_subtype)` (23 rows). Recompute `duration_days` = end − start; null it where end < start (17 rows) or the stored value is negative (65 rows).
2. Jobs: map `job_category` (29 values) to Rod Truck, Workover, Drilling, Completion, Abandon, Other. Equipment names like '2-kudu Drivehead Oryx Seals' go to Other.
3. Jobs: drop empty columns `job_status_1`, `job_status_2`, `actual_final_job_cost`. Exclude `field_est_total` ≤ 0 from cost sums (1 negative).
4. Failures: dedupe on `(uwi, failure_start_date, failure_type)` (6 rows).
5. Failures: map `failure_type` (29 values) to Rods (incl. 'Rod', '|Rod'), Pump (incl. 'PCP'), Tubing (incl. 'Hole in tbg colar'), Surface, Casing, Packer, Other.
6. Failures: add `rod_kind` from `failure_detail`: 'Corod' → COROD, 'Sucker Rod' → Sucker rod, else unknown.
7. Failures: drop empty columns (`failure_end_date` 2,872/2,873 null, `cause_category`, `action_taken`, `failure_system_category`, `failure_type_category`, `cause_comments`).
8. Link each failure to its repair job: the first Rod Truck/Workover job on the same `uwi` from −3 to +14 days. Store `job_cost` and `job_start` on the failure row.

**Output:** `silver_jobs`, `silver_failures` (with linked job).

**Done when:** About 2,812 failures have a linked job (our profiling count); the rest are flagged unlinked.

> **Evidence:** 2,812 of 2,873 failures matched a Rod Truck/Workover job in that window.

#### `f6` Downtime buckets + daily sensor features · Ananya · `notebooks/01_downtime_features.py`

**Inputs:** `silver_daily` (columns `downtime_code_1/2/3`, sensor columns)

1. List distinct `downtime_code_1` with total `lost_oil_m3` and row count. Map the codes covering ~95% of lost oil into buckets: Downhole, Surface equipment, Facility, Power/grid, Planned (Scheduled, Turn Around, Development), Economic (Uneconomic, LTSI), Other.
2. Downhole includes at least: 'Downhole Equipment - Rods/Pump/Tubing Failure/Unspecified/Well Torqued', 'ALL - Downhole - Rodstring Failure', 'ALL - Downhole - Bottomhole Pump Failure', code_2 'RODSPART', 'Downhole'.
3. Save the map as a table `downtime_bucket_map` (code → bucket) so it is reviewable.
4. Sensor features per producing well-day (days with `runtime_frac` ≥ 0.5 only): 7-day rolling mean of torque, current, efficiency, RPM; 30-day baseline mean over days −37 to −8; percent change 7d vs baseline; `torque_per_rpm` = torque / RPM.

**Output:** `downtime_bucket_map`, `silver_daily` with `downtime_bucket`, `feat_daily_sensor`.

**Done when:** Mapped codes cover ≥ 95% of non-null lost oil; the unmapped list is in the notebook.

> **Evidence:** 72 values in `downtime_code_2` from at least two coding schemes. Lost oil by code_2: Uneconomic 162k, Downhole Equipment 144k, Facility 80k, Downhole 61k, Development 30k, Surface Equipment 29k, Scheduled 24k, Power 12k m³.

### ◆ Fri 11 PM

**Checkpoint 1: clean tables locked. No changes after this.**

#### `c1` Clean tables frozen · Everyone

1. Each owner posts their table names and row counts.
2. Anyone finding a new issue after this adds it to `docs/known_issues.md` instead of changing silver tables.

**Done when:** silver_daily, silver_jobs, silver_failures, downtime_bucket_map, feat_daily_sensor exist.

### Sat 9–11 AM

#### `n1` Run the daily-file checks in Databricks · Ananya · `notebooks/01_daily_checks.py`

**Inputs:** `silver_downtime`, `silver_failures`

1. Run after `01_downtime_features`. The sketch returns two small tables, safe to paste into chat.
2. Result 1 tells us whether the loss waterfall is worth a dashboard page and which buckets dominate.
3. Result 2 gives downhole down days and lost oil per failure, counted from the stop day (the start of the downtime streak before the logged date) to 14 days after the logged date. Use the median as the response-time number in the pitch.
4. Sensor coverage on failed wells is already answered by s3 (efficiency ~90%, torque 61%, Cactus Lake torque 6%); no need to re-run it.

```python
# UNTESTED sketch. Run after 01_downtime_features, then paste the two small results into chat.
from pyspark.sql import functions as F
S = "workspace.dsmlc"
dt = spark.table(f"{S}.silver_downtime"); f = spark.table(f"{S}.silver_failures")
# 1) lost oil by month and bucket
(dt.groupBy(F.trunc("production_date", "month").alias("month"), "bucket")
  .agg(F.round(F.sum("lost_oil_m3")).alias("lost_m3")).orderBy("month", "bucket").show(400, False))
# 2) downhole down days and lost oil per failure: up to 30 days before the logged date through 14 days after
j = f.filter(F.col("failure_start_date") >= "2025-06-01").select("failure_id", "uwi", F.col("failure_start_date").alias("fd")).join(dt.filter("is_downhole"), "uwi")
per = (j.where(F.datediff("production_date", "fd").between(-30, 14))
  .groupBy("failure_id").agg(F.count("*").alias("down_days"), F.sum("lost_oil_m3").alias("lost_m3")))
per.agg(F.count("*").alias("failures_with_downtime"), F.expr("percentile_approx(down_days, array(0.25,0.5,0.75))").alias("down_days_q"), F.expr("percentile_approx(lost_m3, array(0.25,0.5,0.75))").alias("lost_m3_q")).show(truncate=False)
```

**Output:** Two small result tables pasted in chat.

**Done when:** We know the dominant loss buckets and median downhole days and lost oil per failure.

> **Evidence:** The event logs show failure and repair start the same day in 76% of cases, and wells stop a median 2 days before the logged date, so downtime must come from the daily file.

> **Watch out:** Done on Nov 21 on the raw files (Downhole 219,627 m³ = 51% of non-economic lost oil; power downtime not seasonal, so weather is no-go; down days median rods 1, pump 4, tubing 5; stop-day window adds 51% lost oil). The notebook `01_daily_checks` repeats it in Databricks and saves `gold_failure_downtime`; the sketch below is a quick alternative.

#### `n2` Repeat-failure analysis and recency tiers · Jacob · `notebooks/03_repeat_failures.py`

**Inputs:** `silver_failures`, `silver_jobs`

1. Sort failures by well and date; compute days to the next failure on the same well. Keep failures from 2025-06-01.
2. Re-failure@90 and @30, dropping failures with less than 90 (or 30) days of follow-up. Report overall, by failure type, by area.
3. Repeat the same measure on Rod Truck jobs for each of 2023, 2024, 2025, 2026 to show the rate is stable and not a logging artefact.
4. Recency tiers: at 14-day snapshots from 2025-09-01 to 2026-07-26, count failures in the prior 90 days (0, 1, 2, 3+) for wells that already have a failure; measure the share with a failure in the next 90 days. Report precision and recall for the 2+ flag.
5. Save `gold_failure_gap` and `gold_recency_tier` for the dashboard.

**Output:** Re-failure table, tier table, two gold tables.

**Done when:** Numbers land within 1–2 points of ours: 57% overall, 66% rods, about 30% pump and tubing; tiers 26 / 40 / 61 / 70%; flag 2+ precision 64%, recall 34%.

> **Evidence:** Computed from the two event logs here: 2,684 failures with 30-day follow-up, 2,338 with 90-day follow-up.

> **Watch out:** Only wells with a logged failure are in the tier universe. Say so on the slide. Reproduced on Nov 21 on the raw files: re@90 57.1% (rods 65.4%), Rod Truck repeat 58–65% every year 2021–26, tiers 26/40/61/70%, flag 2+ precision 64.0% recall 33.5%. By area: Winter 81%, Cactus Lake 41%.

#### `s1` Monthly well table + cost score v1 · Angie · `notebooks/02_ranking.py`

**Inputs:** silver tables

1. Build `gold_well_month`: one row per `uwi` × month with oil, water, producing days, downhole lost oil, Rod Truck/Workover job count and $, failures (2025+), area, facility, pump make/model (mode for the month).
2. Build `gold_well_period`: sum each well over train and test windows separately.
3. Apply the eligible-producer filter (definitions above). Record how many wells are eligible and how many are 'new, unranked'.
4. Compute `cost_score` for the train window. Netback is a notebook widget, prefilled with C$32.50 per boe and its source (see the sourced inputs table).

**Output:** `gold_well_month`, `gold_well_period`, train-window ranking.

**Done when:** Sum of repair $ over all wells and years ≈ $212.4M (cross-check with profiling).

> **Evidence:** Repair $ concentration since Jun 2025: top 10% of wells = 33%, top 20% = 50% (975 wells). The 2021–26 figures (31%, 49%) are biased by missing Rod Truck cost before 2023.

#### `s2` Failure rates by equipment, within area · Jacob · `notebooks/03_runlife_payback.py`

**Inputs:** `silver_failures`, `silver_daily` (pump make/model, producing days)

1. Exposure: producing well-years per well in 2025–26 = sum of `runtime_frac` / 365.
2. Rate = failures / producing well-years, grouped by area × pump make, and area × pump model (top models only).
3. Only report groups with ≥ 20 wells and ≥ 10 failures. Add a Poisson 95% interval on each rate.
4. Compare within the 6 largest failure areas (Cactus Lake, Winter, Cosine, Druid Main, Court, Bodo East).
5. Rod kind (COROD vs sucker rod): only known on failure rows, so compare **failure modes and causes** by rod kind, not rates. Check whether `job_subtype` or `objective` names rod type for non-failed wells (verify).
6. Optional: Kaplan–Meier time-to-next-failure after a repair job, by make (adds `lifelines`).

**Output:** Table of rates with intervals; 1 chart per top area.

**Done when:** At least one make/model difference with non-overlapping intervals in a large area, or a clear 'no difference' result.

> **Evidence:** Failures concentrate in Cactus Lake (957) and Winter (829), so those two areas carry most of the statistical power.

> **Watch out:** Equipment choice is not random: harder wells may get a particular pump. Present differences as association.

#### `s3` Look at sensors before 50 failures · Ananya · `notebooks/04_early_warning.py`

**Inputs:** `silver_daily`, `silver_failures` (2025 rod/pump). Notebook: `04_early_warning`

1. First, coverage: share of 2025 failures whose well has torque, current, efficiency on ≥ 20 of the 30 days before failure. If below ~30%, flag risk 1 now.
2. Sample 50 covered Rods/Pump failures from 2025 (spread across areas).
3. Plot days −45 to +5 for torque, current, efficiency, RPM, with the failure date marked.
4. Note the patterns: gradual torque rise, efficiency decline, current spike, or nothing. Mark failures where torque only drops on day 0 (parted rods: detection, not warning).

**Output:** Grid of 50 plots + a written list of the patterns seen.

**Done when:** We can say which signal (if any) moves in days −30 to −2.

> **Evidence:** Done on Nov 21 (local pandas run; Databricks notebook ready). Torque/current/RPM repeat the previous day 90–96% of the time; no drift before failures. Wells stop a median 2 days before the logged date. Partial-run and down days in days −8 to −2 before the stop are 2–4× more common than on normal days. Charts in `docs/figures/`.

### Sat 11 AM–1 PM

#### `s4` Out-of-time validation of the ranking · Angie · `notebooks/02_ranking.py` (section to add)

**Inputs:** `gold_well_period`

1. Rank eligible wells by train `cost_score`. Take the top 5%, 10% and 20%.
2. Compute capture@k on test cost_score, and separately on test repair $ only and test lost oil only.
3. Baselines: random (k), and size (rank by train oil volume).
4. Bootstrap: resample eligible wells 1,000 times; report the 95% interval for capture@10%.
5. Persistence: share of train top-10% wells still top 10% in test (job-count version gave 31–35%). Repair-$ preview from the job file alone: capture@10% = 16.2% (13.3–19.2%), 1.6 times random. Add a recency score and compare.
6. Lift chart: x = share of wells targeted, y = share of test cost captured, with the random diagonal and the size baseline.

```python
top = train.orderBy(F.desc('cost_score')).limit(int(n*0.10))
capture = test.join(top.select('uwi'), 'uwi').agg(F.sum('cost_score')) / test.agg(F.sum('cost_score'))
```

**Output:** Validation table + lift chart, ready for a slide.

**Done when:** capture@10% with interval, compared with both baselines.

> **Evidence:** Early concentration and persistence numbers suggest the ranking should beat random; the size baseline is the real test.

#### `s5` Pick 2 actions and compute break-even payback · Jacob · `notebooks/03_runlife_payback.py` (section to add)

**Inputs:** `silver_failures` (cause, linked job cost), downhole lost oil around each failure, s2 results

1. Cost per failure = linked repair job cost + downhole lost oil on that well from the **stop day** (start of the downtime streak before the logged date; median 2 days earlier) to +14 days × 6.2898 × netback. Report the median and mean by failure type.
2. Pick 2 actions by failure type and dollars first (rods: 76% of failures at about $5k each; pump and tubing: 22% of failures, about 55% of failure-linked $), then match to cause and s2 results. Candidates: fatigue-driven rod failures (1,225) → rod string upgrade or RPM reduction; corrosion (789) → corrosion-inhibitor program or coated rods; Burnt/Low Inflow (127) → slow RPM / pump-off control; a worse pump make in s2 → switch make at next pull.
3. Action cost: no regional price for a rod string upgrade or inhibitor program turned up. Use the 2023-onward Workover median ($34,152) as an install-job proxy and label it an assumption. Ask a mentor for a real range.
4. Break-even failures avoided per well-year: `N* = annualized action cost / cost per failure`.
5. Compare N* with the observed failure rate of bad-actor wells (e.g. wells with ≥ 3 failures in ~16.8 months ≈ ≥ 2.1 per year) and with the rate gap from s2.
6. Sensitivity grid: netback ±25%, action cost ×0.5 / ×1 / ×2, avoided-failure share 25% / 50%. A vendor case study (Colombia, 6 wells) reports rod run life up 51%, about a 34% lower failure rate, so 25% and 50% bracket it.
7. Netback: C$32.50 per boe (Strathcona Q3 2026 operating earnings, company-wide). The release gives no heavy-oil segment netback; open the MD&A to confirm the definition and write down the page.
8. Use `gold_failure_downtime` (n1) for lost oil per failure from the stop day: median 7.0 m³ rods, 9.5 pump, 16.2 tubing.

**Output:** Payback table per action, sensitivity grid, list of assumptions with sources.

**Done when:** Each action has N*, a payback statement, and every input traced to data or a cited source.

> **Evidence:** Median repair since 2023: Rod Truck $4,374, Workover $34,152. Rods are 2,186 of 2,873 failures.

> **Watch out:** Action effects come from observational comparisons. Say 'associated with', not 'causes'.

#### `s6` Draft threshold rules, tune on Jun–Dec 2025 · Ananya · `notebooks/04_early_warning.py` (section to add)

**Inputs:** `silver_daily` (runtime), `feat_daily_sensor` (efficiency, fluid rate), Jun–Dec 2025 failures aligned to the stop day

1. Define 2–3 candidate rules from what s3 showed: ≥ X partial-run days (0 < hours on < 23) in the last 7; ≥ Y fully down days in the last 7; efficiency or fluid-rate 7d mean ≤ Z × baseline. No torque or current rules: those readings repeat 90–95% of the time.
2. An alert is a 'hit' if the well stops for a Rods/Pump/Tubing failure within the next 2–30 days. Use the stop day, not `failure_start_date`, and ignore alerts from the day before the stop onward (that day is usually the partial-run failure day).
3. Suppress repeat alerts on the same well for 14 days, so one problem counts once.
4. Grid-search X, Y, Z on Jun–Dec 2025 only (s3 suggests X = 2, Y = 1 as a start). Pick the setting with the best precision while recall ≥ ~20% and alerts stay manageable (alerts per area per week).
5. Record the chosen thresholds before looking at 2026.
6. Baseline to beat: flag wells with 2+ failures in the last 90 days (precision 64%, recall 34% on our snapshots). A sensor rule counts only if it beats this or adds recall at similar precision.

**Output:** Frozen rule set with thresholds and 2025 metrics.

**Done when:** Thresholds written down and committed before s9 runs.

> **Evidence:** About 1,200 failures in Jun–Dec 2025 for tuning, about 1,640 in Jan–Oct 2026 for testing.

### ◆ Sat 1 PM

**Checkpoint 2: ranking validated, actions chosen. Optional mentor check (1–5 PM).**

#### `c2` Ranking and actions signed off · Everyone

1. Review s4 lift chart and s5 payback table together.
2. If the ranking does not beat the size baseline, switch to a per-producing-day score (risk 3) now.
3. Optional: show the story to a mentor and ask what a field engineer would challenge.

**Done when:** Team agrees on the score definition and the 2 actions.

### Sat 1–3 PM

#### `n4` Budget scenarios (precomputed) · Jacob

**Inputs:** s5 results, `gold_failure_gap`

1. Expected avoided cost per well = shrunken failure rate × cost per failure × avoided share − action cost. Shrink each well's rate toward its area rate with a Gamma-Poisson (empirical Bayes) prior.
2. Rank wells by expected avoided cost per dollar and treat the best ones until the budget is spent. A greedy pick is enough; say so.
3. Build 3–4 scenarios (for example 25, 50, 100 and 200 wells) and save `gold_budget_scenarios` with wells, cost, expected savings range and break-even.
4. Use avoided shares of 25% and 50%. One vendor case supports about 34% (see the sourced inputs table), so label them assumptions with that bracket.

**Output:** `gold_budget_scenarios` and a one-line explanation of the method.

**Done when:** All three of us can explain the selection in 30 seconds.

> **Watch out:** The dashboard filters precomputed scenarios. Do not promise live optimization.

#### `s7` Dashboard page 2: weekly work list and repeat failures · Angie

1. Databricks AI/BI dashboard on gold tables.
2. Widgets: ranked table (uwi, area, facility, recency tier, cost score, repair $, lost oil, failures in last 90 days, pump make, suggested action); filters for area, facility and failure type; point map of wells coloured by tier; KPI tiles for 90-day re-failure rate, capture@10% and lift.
3. Add a 'how to read this' text box with the score definition.

**Output:** Page 2 live.

**Done when:** Filters work; numbers match the s4 notebook.

#### `s8` Dashboard page 3: payback and budget scenarios · Jacob

1. Cost per failure by type; the 2 actions with N*, payback statement, and the sensitivity grid.
2. Bad-actor wells eligible for each action (cause match), with expected savings range.
3. Budget scenarios from n4: a selector for 3–4 precomputed scenarios showing which wells are treated, cost, expected savings and break-even.

**Output:** Page 3 live.

**Done when:** Every number on the page traces to the s5 notebook.

#### `s9` Test thresholds on 2026 + dashboard page 4: watchlist · Ananya · `notebooks/04_early_warning.py` (section to add)

1. Run the frozen rules on 2026 data. Report precision, recall, median lead time (days from first alert to failure), alerts per area per week.
2. Compare with a random-alert baseline at the same alert rate.
3. Page 4: current alerts list (well, rule fired, days since, recency tier, rank on page 2), the 2026 test metrics, and the same metrics for the recency watchlist.

**Output:** 2026 metrics + page 4 live.

**Done when:** Metrics reported as measured, whether good or weak.

> **Evidence:** If precision is near the random baseline, switch to the monitoring-view fallback (risk 1).

### ◆ Sat 3 PM

**Go/no-go on weather: only if all three dashboard pages work.**

#### `c3` Weather decision made · Everyone

1. Free seasonality check first: monthly Power/grid downtime hours and monthly median torque on instrumented wells. Already done on the event logs: failures are flat month to month and Apr–May Rod Truck jobs run 8% below expected, so no weather story there.
2. If none of the three is seasonal, skip weather and say so on one slide.
3. If seasonal and f3 found a source, go to s12.

**Done when:** Decision posted in chat.

### Sat 3–5 PM

#### `n3` Strategy case and implementation plan (30/60/90) · Ananya

1. Value at stake: Rod Truck plus Workover spend is about $46M a year since Jun 2025 (field estimates). Use n2 to state how much of it follows a failure on a well that failed in the last 90 days.
2. Pilot: Cactus Lake and Winter first (957 and 829 failures, most of the statistical power). Cactus Lake is where the money is (workovers are 43% of its jobs); Winter is where failures repeat most (81% re-fail within 90 days) and is mostly cheap rod truck jobs.
3. Days 0–30: weekly reliability review driven by the work list. Days 31–60: budget scenario selection and a root-cause review on every well with a second failure within 90 days. Days 61–90: add sensor rules only if they beat the recency baseline; then expand by area.
4. Cover the four resources the rubric names. Money: pilot cost and break-even, each input labelled sourced or assumed. Manpower: who runs the weekly review, who acts, who owns the data. Logistics: crew and rig scheduling; Apr–May job volume runs 8% lower. Time: days to first savings.
5. KPIs: 90-day re-failure rate, failures per well-year, repair $ per well-year, downhole lost oil (m³), alerts per area per week.
6. Data fixes needed to run it for real: actual job cost (empty today), failure log before Jun 2025, consistent failure type spellings, sensor coverage.

**Output:** One implementation slide plus a README section.

**Done when:** Every cost input is labelled sourced or assumed, and a judge could read the plan without the dashboard open.

#### `n5` Dashboard page 1: where barrels and dollars go · Angie

1. Lost oil by downtime bucket and month as bars or lines, using the f6 bucket map. Add failures per month and repair $ by failure type and area.
2. Add a data-trust panel: rows changed per rule from `cleaning_log`, and how the top of the ranking changes with and without cleaning (999999 pressures, negative volumes).
3. Use bars, lines, tables and point maps only.

**Output:** Page 1 live.

**Done when:** A reader sees where the losses are and why they can trust the numbers in under a minute.

#### `s10` Dashboard polish + optional Genie space · Angie

1. Consistent titles, units (m³, $, kPa), and definitions on every page.
2. Judges will open the workspace, so every page needs a plain-language title, a one-line how-to-read note and no unexplained widget. Save a screenshot of each page into dashboard/ in the repo, because the video and repo are judged first.
3. Optional Genie space on gold tables with 5 tested example questions (e.g. 'top 10 bad actors in Cactus Lake', 'payback for corrosion program'). Check each answer against the notebooks.

**Output:** Final dashboard; Genie link if done.

**Done when:** A teammate can use the dashboard without explanation.

#### `s11` Sensitivity table + limitations list · Jacob

1. Finalize the sensitivity grid from s5 for the slide.
2. Write the limitations list: failures only 2025–26; field estimates not actual cost; netback assumption; observational comparisons; lost oil is an operator estimate (67.5% of rows null); downtime codes partly mapped; sensor coverage.

**Output:** Limitations section for README and slides.

**Done when:** Each limitation states its effect on the result (over- or under-estimate).

#### `s12` Weather join + 3 checks (2 h cap), or threshold polish · Ananya

1. Pull daily min/max temperature for each of the 26 area centroids (mean lat/lon of wells), 2021–2026.
2. H1: Power/grid downtime hours per well-day on days with min temp ≤ −30 °C vs other winter days.
3. H2: within-well correlation of torque with daily min temp, holding RPM bands fixed.
4. H3: Apr–May Rod Truck job volume vs expected, by area (event logs show about −8% overall; check whether it sits in a few areas).
5. Stop at 2 hours, whatever the state.

**Output:** 3 results, each 'effect / no effect' with the number.

**Done when:** One slide's worth of evidence, or a documented null.

### Sat 5–8 PM

#### `s13` README findings and methods · Angie

1. Fill every required section. Findings: capture@10%, lift, the 2 actions' break-even, early-warning test metrics.
2. Methods: data cleaning rules (link cleaning_log), definitions, windows, baselines.
3. Validation: out-of-time design, bootstrap intervals, 2026 hold-out for warnings.
4. External resources: Strathcona investor report used for netback, weather source, PCP references. AI disclosure from AI_LOG.md.

**Output:** README complete.

**Done when:** Someone outside the team could rerun the notebooks from it.

#### `s14` Slides: problem, data, method, results · Jacob

1. ~10 slides for 10 minutes, one storyline: 1 Situation: where barrels and dollars go. 2 Complication: failures repeat, 57% within 90 days. 3 What we built: the Reliability Desk demo. 4 Data we cleaned and why we trust it. 5 Who to act on: ranking, lift, size baseline. 6 What to do: 2 actions, payback, sensitivity grid. 7 Early warning vs the recency baseline. 8 Implementation plan from n3. 9 Limitations. 10 Recommendation and ask.
2. Each slide has one message in its title. Mark the 5-minute cut for the video: slides 1, 2, 3, 6, 8 and 10.

**Output:** Slide draft.

**Done when:** Every number on a slide links to a notebook cell.

#### `s15` Video script + slide visuals · Ananya

1. 650–750 words: problem, research question, data, approach, result, business value, limitations, next steps. Mark where each visual or demo appears.
2. Export clean charts (lift chart, payback grid, warning example) for the slides.

**Output:** Script + chart images.

**Done when:** Script reads aloud in under 5:00.

### ◆ Sat 8 PM

**Checkpoint 3: full slide draft and script done.**

#### `c4` Draft slides and script reviewed · Everyone

1. Read the script aloud with slides; time it.
2. Each person challenges one number and one claim.

**Done when:** Changes listed for Sunday morning.

### Sun 8–9 AM

#### `u1` Check every slide number against the code · Angie

1. Walk each slide; rerun the cell that produced each number.
2. Fix mismatches in the slides, not the data.

**Done when:** No unchecked number left.

#### `u2` Final slide pass · Jacob

1. Apply Checkpoint 3 changes.
2. Export PDF for the repo.

**Done when:** PDF in `docs/`.

#### `u3` Record the video · Ananya

1. Screen-record slides + dashboard demo with voice-over, under 5:00.
2. Upload (unlisted YouTube or Drive with link access).

**Done when:** Link works in a private window.

### ◆ Sun 9 AM

**Slides and video locked.**

#### `c5` Slides and video final · Everyone

**Done when:** No more content changes.

### Sun 9–11 AM

#### `u4` Repo cleanup; notebooks run end to end · Angie

1. Run notebooks 01→05 on a fresh cluster.
2. Remove scratch files, secrets, and outputs with raw data.
3. Check the repo is public.

**Done when:** Clean run with no errors.

#### `u5` Upload video and slides; add links to README · Jacob

1. Add video link and slides PDF/link to README.
2. Open the links from a logged-out browser.

**Done when:** Both links work for a stranger.

#### `u6` Prep 10 hard judge questions · Ananya

1. 2 per rubric criterion, e.g. 'Why not ML?', 'Is the size baseline fair?', 'Where does the netback come from?', 'How would Strathcona run this monthly?', 'What if the failure log change explains the 2025 jump?' (answer: Rod Truck jobs show the same 58–60% repeat rate in every year 2023–26), 'Why no repair-time analysis?', 'Does your sensor rule beat the recency list?'.
2. Write a 2-sentence answer for each and assign who answers.

**Done when:** Answers shared with the team.

### ◆ Sun 11 AM

**Post the repo link in #submissions.**

#### `c6` Repo link posted · Everyone

**Done when:** Link posted before 12:00 PM; screenshot saved.

## Top risks

### Early-warning rules show no usable lead time

- **Status:** **Partly happened (s3).** Torque and current are repeated entries with no drift before failures. Runtime instability does separate failures from normal days (2–4×), but precision is not yet measured.
- **Early sign:** s9: runtime rules do not beat the recency watchlist on 2026.
- **Fallback:** Ship the recency watchlist (2+ failures in 90 days: precision 64%, recall 34%) as the early warning, and show sensors as a monitoring view with measured precision.

### Payback numbers can't be defended

- **Why it could happen:** Field estimates aren't actual costs; no price in the data; actions weren't randomized.
- **Early sign:** We can't name the source of a cost or price input.
- **Fallback:** Break-even plus sensitivity grid, cited netback, same-area comparisons only, stated as association.

### Ranking doesn't beat the size baseline

- **Why it could happen:** High-rate wells lose more oil when down, so volume alone may rank as well as cost.
- **Early sign:** Sat 1 PM: capture@10% for cost score ≤ size baseline.
- **Fallback:** Normalize: cost per producing day, or failures per well-year. Report both honestly.

### The daily file disagrees with the event logs

- **Why it could happen:** Event-log numbers were recomputed in v2. Barrels, sensors, buckets and pump make still come from the Nov 20 profiling.
- **Early sign:** n1 results differ from the facts table.
- **Fallback:** Re-run the facts in Databricks before they go on a slide (u1 already checks every number).

### A judge says repeat failures are a logging artefact

- **Why it could happen:** The failure log only starts in Jun 2025.
- **Early sign:** The question comes up in Q&A.
- **Fallback:** Rod Truck jobs show a 58–60% 90-day repeat rate in each of 2023, 2024, 2025 and 2026. Keep that chart in the appendix.

### Weather eats time or shows nothing

- **Early sign:** No data by Sat 3:30 PM, or no seasonality in the free check.
- **Fallback:** Drop it, or one slide: "weather is not a main driver." Never delays must-haves.

### Cleaning drags on

- **Early sign:** Silver tables still changing after Fri 11 PM.
- **Fallback:** Hard freeze at Checkpoint 1. Later fixes go on a known-issues list in the README.

## Where each part earns points

| MVP part | Research 25% | Technical 25% | Impact 20% | Present 15% | Creative 15% |
|---|---|---|---|---|---|
| Clean tables + cleaning log |  | ●● |  |  |  |
| Repeat-failure analysis + 90-day recency watchlist (no ML) | ●● | ● | ●● | ●● | ●● |
| Bad-actor ranking + out-of-time test vs random and size baselines | ● | ●● | ●● | ● | ● |
| Failure rates by equipment (within area) | ●● | ● | ● |  |  |
| Payback / break-even for 2 actions + sensitivity | ● | ● | ●● | ●● |  |
| Early-warning thresholds vs the recency baseline (tune Jun–Dec 2025, test 2026) | ● | ● | ● | ● | ● |
| AI/BI dashboard (+ Genie) |  | ● | ● | ●● | ●● |
| Weather link (if done) | ●● |  | ● |  | ●● |
| Limitations list | ● | ●● | ● |  |  |
| Strategy case + 30/60/90 implementation plan | ● |  | ●● | ●● | ● |
| README, video, slides |  |  |  | ●● |  |

●● main source of points · ● supporting

---

_Plan drafted Nov 20, 2026; revised Nov 21 (v2) after re-profiling the jobs and failures files. Daily-file facts come from the Nov 20 profiling and are not re-checked. Scores, thresholds to try, and timings are team judgment; anything marked "verify" has not been checked yet._
_Never put Databricks tokens or connection strings in notebooks or the repo. Use Databricks secrets or environment variables._
