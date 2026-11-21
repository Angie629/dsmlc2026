# Databricks notebook source
# MAGIC %md
# MAGIC # 03c · Budget scenarios · task n4
# MAGIC
# MAGIC Build plan task n4. Needs `01a_clean_daily`, `01b_clean_jobs_failures`, `01d_daily_checks` and the s5 section of `03b_runlife_payback` (`gold_payback`). Writes:
# MAGIC - `workspace.dsmlc.gold_budget_wells`: one row per producing well with its expected failure rate, best action and rank
# MAGIC - `workspace.dsmlc.gold_budget_scenarios`: one row per budget (25, 50, 100, 200 wells)
# MAGIC
# MAGIC **Same rules as s5 (`gold_payback`), so page 4 tells one story**
# MAGIC - Actions: **Action 1** = rod failures caused by fatigue or corrosion; **Action 2** = pump or tubing failures.
# MAGIC - Cost per failure and action cost come from `gold_payback`'s base case (mean cost per failure; C$34,152 assumed action cost).
# MAGIC
# MAGIC **Pick on one period, measure on the next (backtest)**
# MAGIC - Wells are ranked as of 2026-01-01 using Jun–Dec 2025 failures only.
# MAGIC - Each well's failure rate is shrunk toward its area's rate (Gamma-Poisson empirical Bayes), so a well with one lucky or unlucky month doesn't jump to the top.
# MAGIC - Each well gets the action with the higher expected savings; wells are ranked by expected yearly savings (every action costs the same).
# MAGIC - Savings are then **measured on Jan–Oct 2026 failures**, which the ranking never saw. That is the number to quote.
# MAGIC
# MAGIC Avoided-failure shares of 25% and 50% are assumptions (one vendor case: about 34%). Savings are yearly; the action cost is one-off.

# COMMAND ----------

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

CATALOG_SCHEMA = "workspace.dsmlc"
DAILY = f"{CATALOG_SCHEMA}.silver_daily"
FAILURES = f"{CATALOG_SCHEMA}.silver_failures"
PAYBACK = f"{CATALOG_SCHEMA}.gold_payback"
WELLS_TARGET = f"{CATALOG_SCHEMA}.gold_budget_wells"
SCENARIOS_TARGET = f"{CATALOG_SCHEMA}.gold_budget_scenarios"

PICK = ("2025-06-01", "2025-12-31")
MEASURE = ("2026-01-01", "2026-10-27")
BUDGET_WELLS = [25, 50, 100, 200]
SHARES = [0.25, 0.50]
MIN_EXPOSURE_YEARS = 0.1
PRIOR_BETA_BOUNDS = (0.1, 5.0)
ACTION_NAMES = {
    "A1": "Action 1 · Rod string upgrade or corrosion inhibition",
    "A2": "Action 2 · Pump and tubing failure prevention",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Failures per well and action group, before and after the ranking date

# COMMAND ----------

WELLS_SQL = f"""
WITH ep AS (
  SELECT uwi, MAX(field_operating_area) AS area, SUM(runtime_frac) / 365.0 AS exposure_pick
  FROM {DAILY}
  WHERE flow_direction = 'production' AND production_date BETWEEN DATE'{PICK[0]}' AND DATE'{PICK[1]}'
  GROUP BY uwi),
em AS (
  SELECT uwi, SUM(runtime_frac) / 365.0 AS exposure_measure
  FROM {DAILY}
  WHERE flow_direction = 'production' AND production_date BETWEEN DATE'{MEASURE[0]}' AND DATE'{MEASURE[1]}'
  GROUP BY uwi),
f AS (
  SELECT uwi, failure_start_date AS d,
         CASE WHEN failure_type_norm = 'Rods' AND cause IN ('Fatigue', 'Corrosion') THEN 'A1'
              WHEN failure_type_norm IN ('Pump', 'Tubing') THEN 'A2' END AS action_group
  FROM {FAILURES}
  WHERE failure_start_date BETWEEN DATE'{PICK[0]}' AND DATE'{MEASURE[1]}'),
c AS (
  SELECT uwi, action_group,
         SUM(CASE WHEN d <= DATE'{PICK[1]}' THEN 1 ELSE 0 END) AS failures_pick,
         SUM(CASE WHEN d >= DATE'{MEASURE[0]}' THEN 1 ELSE 0 END) AS failures_measure
  FROM f WHERE action_group IS NOT NULL GROUP BY uwi, action_group)
SELECT e.uwi, COALESCE(e.area, 'Unknown') AS area, e.exposure_pick, COALESCE(m.exposure_measure, 0) AS exposure_measure,
       g.action_group, COALESCE(c.failures_pick, 0) AS failures_pick, COALESCE(c.failures_measure, 0) AS failures_measure
FROM ep e
LEFT JOIN em m USING (uwi)
CROSS JOIN (SELECT explode(array('A1', 'A2')) AS action_group) g
LEFT JOIN c ON c.uwi = e.uwi AND c.action_group = g.action_group
WHERE e.exposure_pick >= {MIN_EXPOSURE_YEARS}"""

COSTS_SQL = f"""
SELECT action, MAX(mean_cost_per_failure) AS cost_per_failure, MAX(action_cost) AS action_cost
FROM {PAYBACK} WHERE is_base_case GROUP BY action"""


def load():
    return spark.sql(WELLS_SQL).toPandas(), spark.sql(COSTS_SQL).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Shrunken failure rates, best action per well, ranking

# COMMAND ----------

def shrink(df):
    """Gamma-Poisson posterior mean rate per well, with a method-of-moments prior per area and action group."""
    out = []
    for (_, _), g in df.groupby(["area", "action_group"]):
        t, y = g.exposure_pick.to_numpy(float), g.failures_pick.to_numpy(float)
        mean = y.sum() / t.sum()
        between = np.var(y / t) - mean * np.mean(1 / t)
        beta = mean / between if between > 0 and mean > 0 else PRIOR_BETA_BOUNDS[1]
        beta = float(np.clip(beta, *PRIOR_BETA_BOUNDS))
        out.append(g.assign(area_rate=mean, prior_beta_years=beta, expected_rate=(y + mean * beta) / (t + beta)))
    return pd.concat(out, ignore_index=True)


def score(wells, costs):
    costs = costs.assign(action_group=costs.action.map({v: k for k, v in ACTION_NAMES.items()}))
    assert costs.action_group.notna().all(), "gold_payback action names changed"
    w = shrink(wells).merge(costs[["action_group", "cost_per_failure", "action_cost"]], on="action_group")
    measure_years = (pd.Timestamp(MEASURE[1]) - pd.Timestamp(MEASURE[0])).days / 365 + 1 / 365
    w["expected_value_per_year"] = w.expected_rate * w.cost_per_failure
    w["measured_rate"] = w.failures_measure / measure_years
    best = w.loc[w.groupby("uwi").expected_value_per_year.idxmax()]
    best = best.sort_values(["expected_value_per_year", "failures_pick", "uwi"], ascending=[False, False, True]).reset_index(drop=True)
    best["rank"] = np.arange(1, len(best) + 1)
    best["action"] = best.action_group.map(ACTION_NAMES)
    best["smallest_budget"] = pd.cut(best["rank"], [0] + BUDGET_WELLS, labels=BUDGET_WELLS).astype("float")
    return best, measure_years


def scenarios(best):
    rows = []
    for n in BUDGET_WELLS:
        b = best[best["rank"] <= n]
        cost = float(b.action_cost.sum())
        row = {
            "budget_wells": n,
            "scenario": f"Treat top {n} wells",
            "action_cost_total": cost,
            "wells_action_1": int((b.action_group == "A1").sum()),
            "wells_action_2": int((b.action_group == "A2").sum()),
            "failures_per_well_year_expected": round(float(b.expected_rate.mean()), 2),
            "failures_per_well_year_measured": round(float(b.measured_rate.mean()), 2),
        }
        for s in SHARES:
            exp_sav = float((b.expected_rate * b.cost_per_failure).sum() * s)
            meas_sav = float((b.measured_rate * b.cost_per_failure).sum() * s)
            pct = int(s * 100)
            row[f"expected_savings_per_year_{pct}"] = round(exp_sav)
            row[f"measured_savings_per_year_{pct}"] = round(meas_sav)
            row[f"payback_years_measured_{pct}"] = round(cost / meas_sav, 2) if meas_sav > 0 else None
        rows.append(row)
    return pd.DataFrame(rows)


wells_raw, costs = load()
best, measure_years = score(wells_raw, costs)
summary = scenarios(best)
print(f"Wells ranked: {len(best):,}; measurement window {measure_years:.2f} years")
display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write and check

# COMMAND ----------

keep = ["rank", "uwi", "area", "action", "action_group", "failures_pick", "exposure_pick", "area_rate", "prior_beta_years",
        "expected_rate", "failures_measure", "measured_rate", "cost_per_failure", "action_cost", "expected_value_per_year", "smallest_budget"]
spark.createDataFrame(best[keep]).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(WELLS_TARGET)
summary["pick_window"] = f"{PICK[0]} to {PICK[1]}"
summary["measure_window"] = f"{MEASURE[0]} to {MEASURE[1]}"
spark.createDataFrame(summary).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(SCENARIOS_TARGET)

n4_checks = {
    "one row per well": not best.uwi.duplicated().any(),
    "one action per well, from the two s5 actions": set(best.action_group) <= {"A1", "A2"},
    "bigger budget treats more wells": summary.budget_wells.is_monotonic_increasing and (summary.action_cost_total.diff().dropna() > 0).all(),
    "only producing wells with exposure": bool((best.exposure_pick >= MIN_EXPOSURE_YEARS).all()),
}
for name, ok in n4_checks.items():
    print(("PASS " if ok else "FAIL ") + name)
print(summary[["scenario", "action_cost_total", "wells_action_1", "wells_action_2", "failures_per_well_year_expected",
               "failures_per_well_year_measured", "measured_savings_per_year_50", "payback_years_measured_50"]].to_string(index=False))
assert all(n4_checks.values()), "One or more checks failed"
