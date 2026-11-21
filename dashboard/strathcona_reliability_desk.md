# Strathcona Reliability Desk - AI/BI dashboard specification

This repository specification implements the pasted dashboard brief and the
four-page MVP in `docs/build_plan.md`.

## Reconciliation

The detailed attachment specifies six pages. The build plan names four MVP
pages. The six-page order below is used for the pitch; the build-plan grouping
is retained by combining:

- Page 6 (data trust) into the data-trust content of page 1.
- Page 5 (ranking validation) into the work-list/payback storyline.

For the MVP dashboard, expose these six pages only if all required gold tables
exist. Otherwise stop and correct the data contract rather than creating
sample data.

## Global configuration

- Dashboard name: `Strathcona Reliability Desk`
- Catalog and schema: `main.strathcona`
- Parameter: `netback_per_bbl`, default `32.50`
- Global filters: `field_operating_area`, `failure_type`
- Data end date: `2026-10-26`
- Reliable failure history: `2025-06-01` onward
- Currency: CAD
- Volume: m3
- Netback formula: lost oil m3 x 6.2898 x `:netback_per_bbl`

## Required tables

```text
gold_loss_bucket_month
gold_repair_cost_month
gold_failure_gap
gold_recency_tier
gold_work_list
gold_validation_curve
gold_validation_summary
gold_warning_eval
gold_payback_grid
gold_budget_scenarios
cleaning_log
gold_cleaning_impact
gold_alerts
```

All missing tables or columns must be reported. Do not invent sample rows.

## Palette

| Use | Colour |
|---|---|
| Text/headings | `#1B2B34` |
| Primary/money | `#1F4E79` |
| Repeat/downhole/watchlist | `#C2410C` |
| Positive benefit | `#2F7A4F` |
| Neutral/baseline | `#94A3B8` |
| Rods | `#1F4E79` |
| Pump | `#6B4D9C` |
| Tubing | `#0F766E` |
| Other | `#94A3B8` |

Recency tiers use `0=#E5E7EB`, `1=#FDBA74`, `2=#EA580C`, `3+=#9A3412`.

## Page 1 - Where the money goes

Header:

- Heading: `Where do the money and barrels go?`
- Sentence: `Rods fail most often, but pump and tubing failures cost the most per repair. Spend also concentrates in a few areas.`
- Next: `Failures repeat`

Counters:

1. Repair spend in the last 12 months.
2. Downhole lost oil in the last 12 months (m3).
3. Value of lost oil using the netback parameter.

Charts:

- Stacked column of lost oil by month and downtime bucket.
- Failure-share versus repair-cost-share for Rods, Pump and Tubing.
- Stacked horizontal bars of Rod Truck and Workover spend by area.

Every chart and counter description must state units and the time window.

## Page 2 - Failures repeat

Header:

- Heading: `Do failures come back?`
- Sentence: `More than half of failures are followed by another on the same well within 90 days. Rod failures come back two times in three.`
- Next: `This week's work list`

Counters:

1. Overall 90-day re-failure rate.
2. Rod 90-day re-failure rate.
3. Pump and tubing 90-day re-failure rate.

Charts:

- 30-day and 90-day repeat rates by failure type.
- Failure gap distribution.
- Monthly failure counts by type.
- Re-failure rate by recency tier.
- Chronic-well distribution.

Reference values to validate:

- Overall about 57%
- Rods about 66%
- Pump and tubing about 30%
- Tiers 0/1/2/3+: about 26%, 40%, 61%, 70%

## Page 3 - This week's work list

Header:

- Heading: `Which wells do we act on this week?`
- Sentence: `Wells with two or more failures in the last 90 days are the watchlist. They are sorted by recent failures, then by cost.`
- Next: `What acting pays back`

Counters:

1. Wells on the watchlist.
2. Wells with three or more recent failures.
3. Repair cost on watchlist wells.

Widgets:

- Point map of watchlist wells, coloured by recency tier.
- Horizontal bar of watchlist wells by area and tier.
- Ranked table with 25 rows: rank, well, area, facility, pump make, tier,
  recent failures, last failure, last failure date, days since, 12-month
  repair cost, 12-month lost oil, cost score, and suggested action.

Filters: `field_operating_area`, `facility_name`, `failure_type`,
`recency_tier`, `pump_make`.

## Page 4 - What acting pays back

Header:

- Heading: `What does acting pay back?`
- Sentence: `Two actions are tested on the wells they fit, with a range because we cannot prove how many failures they avoid.`
- Next: `Does the ranking hold up`

Widgets:

- Net benefit at 25% avoided share.
- Net benefit at 50% avoided share.
- Fastest 50%-case payback.
- 25%/50% net-benefit chart by action.
- Break-even table by action.
- Sensitivity grid by action cost and netback.
- Precomputed budget scenarios.

Every assumption must be visible:

- Netback: C$32.50 per boe, company-wide
- Action costs are assumptions
- 25% and 50% avoided-failure shares are assumptions
- Observed effects are associations, not proven causes

## Page 5 - Does the ranking hold up

Header:

- Heading: `Does the ranking hold up on data it has not seen?`
- Sentence: `We rank wells on 2023-24 history and test on Jun 2025 to Oct 2026. A sensor rule only counts if it beats the simple recency watchlist.`
- Next: `Can we trust the data`

Widgets:

- Capture@10% counter.
- Lift versus random.
- 95% interval.
- Capture curve for our ranking, size baseline and random.
- Sensor precision/recall versus the recency baseline.

Reference capture@10% is approximately 16% in the repair-cost-only preview.
Do not relabel or smooth the result if the final notebook differs.

## Page 6 - Can we trust the data

Header:

- Heading: `Can we trust the data?`
- Sentence: `We cleaned the raw files, logged every rule, and list what the data cannot tell us.`
- Next: `End of dashboard`

Widgets:

- Rows changed by cleaning.
- Top-50 wells that change after cleaning.
- Top cleaning rules by rows affected.
- Data-limitations text.
- Full cleaning log table.

Limitations:

- Failure history starts June 2025.
- Rod Truck cost is incomplete before 2023.
- Costs are field estimates, not invoices.
- Lost oil is an operator estimate.
- Netback is company-wide, not heavy-oil-specific.
- Action effects are associations, not proven causation.

## Required Databricks datasets

Create the datasets against `main.strathcona` with these semantic rules:

```sql
-- Page 1 repair spend, last 12 months
SELECT field_operating_area,
       SUM(repair_cost) AS repair_cost_12m
FROM main.strathcona.gold_repair_cost_month
WHERE month > add_months((SELECT MAX(month)
                          FROM main.strathcona.gold_repair_cost_month), -12)
GROUP BY field_operating_area;

-- Page 1 downhole loss and value
SELECT field_operating_area,
       SUM(lost_oil_m3) AS downhole_lost_oil_m3_12m,
       SUM(lost_oil_m3) * 6.2898 * :netback_per_bbl AS downhole_lost_oil_value
FROM main.strathcona.gold_loss_bucket_month
WHERE downtime_bucket = 'Downhole'
  AND month > add_months((SELECT MAX(month)
                          FROM main.strathcona.gold_loss_bucket_month), -12)
GROUP BY field_operating_area;

-- Page 2 90-day re-failure rate
SELECT failure_type,
       AVG(CASE WHEN days_to_next_failure <= 90 THEN 1.0 ELSE 0.0 END) AS refail_90d
FROM main.strathcona.gold_failure_gap
WHERE failure_date BETWEEN DATE '2025-06-01' AND DATE '2026-07-28'
GROUP BY failure_type;

-- Page 3 watchlist
SELECT w.*,
       CASE WHEN failures_last_90d >= 3 THEN '3+' ELSE CAST(failures_last_90d AS STRING) END AS recency_tier,
       repair_cost_12m + downhole_lost_oil_m3_12m * 6.2898 * :netback_per_bbl AS cost_score
FROM main.strathcona.gold_work_list w
WHERE failures_last_90d >= 1;

-- Page 4 scenario net benefit
SELECT scenario, wells_treated, total_action_cost,
       savings_low - total_action_cost AS net_low,
       savings_high - total_action_cost AS net_high
FROM main.strathcona.gold_budget_scenarios
ORDER BY wells_treated;

-- Page 5 warning comparison
SELECT rule_name, is_baseline, precision, recall,
       alerts_per_area_week, median_lead_days
FROM main.strathcona.gold_warning_eval;
```

## Acceptance checks

1. Every global filter responds on every page or is explicitly hidden by data contract.
2. Changing `netback_per_bbl` changes lost-oil value and work-list cost score.
3. Page headings read as one pitch storyline.
4. All KPI values are within the documented reference ranges or are visibly
   labelled as changed by newer table results.
5. Export screenshots to `dashboard/screenshots/`.
