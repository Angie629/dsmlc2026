# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03b · Failure rates by equipment, within area · task s2 (s5 next)
# MAGIC
# MAGIC Build plan task s2. Needs `01a_clean_daily` and `01b_clean_jobs_failures`. Writes `workspace.dsmlc.gold_failure_rates`.
# MAGIC Payback (task s5) is added to this notebook next.
# MAGIC
# MAGIC **Question:** which pump makes, models and operating speeds are associated with more downhole failures per year of running, comparing wells in the same operating area?
# MAGIC
# MAGIC **Method**
# MAGIC - Window: 2025-06-01 to 2026-10-27. The failure log is live only from Jun 2025 (Jan–Apr 2025: 3 rows, May: 27, then ~160–180 a month). Starting earlier would add running time with almost no logged failures and understate every rate.
# MAGIC - Exposure: producing well-years = Σ `runtime_frac` / 365 over producing well-days, so shut-in time doesn't count.
# MAGIC - Equipment is taken **per day**, so a well that changed pump make contributes exposure and failures to each make for the days it ran it.
# MAGIC - Rate = failures ÷ well-years, with a Poisson 95% interval (Byar's approximation, no extra packages).
# MAGIC - Reported only for groups with ≥ 20 wells and ≥ 10 failures.
# MAGIC - Compared within the 6 areas with the most failures, against the area's overall rate.
# MAGIC
# MAGIC Equipment is not assigned at random: harder wells may get a particular pump. Differences are associations, not proof that the equipment causes failures.

# COMMAND ----------

import math

import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG_SCHEMA = "workspace.dsmlc"
DAILY = f"{CATALOG_SCHEMA}.silver_daily"
FAILURES = f"{CATALOG_SCHEMA}.silver_failures"
JOBS = f"{CATALOG_SCHEMA}.silver_jobs"
RATES_TARGET = f"{CATALOG_SCHEMA}.gold_failure_rates"

WINDOW = ("2025-06-01", "2026-10-27")
DOWNHOLE_TYPES = ["Rods", "Pump", "Tubing"]
MIN_WELLS = 20
MIN_FAILURES = 10
TOP_AREAS = 6
TOP_MODELS_PER_AREA = 8
Z = 1.96
RPM_BANDS = [(0, 150, "<150"), (150, 250, "150–249"), (250, 350, "250–349"), (350, 10_000, "350+")]

# COMMAND ----------

def mode_by(df, keys, col):
    counts = df.filter(F.col(col).isNotNull()).groupBy(*keys, col).count()
    w = Window.partitionBy(*keys).orderBy(F.desc("count"), col)
    return counts.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn", "count")


def poisson_ci(k, z=Z):
    """Byar's approximation to the exact Poisson interval for a count k."""
    lo = 0.0 if k == 0 else k * (1 - 1 / (9 * k) - z / (3 * math.sqrt(k))) ** 3
    k1 = k + 1
    hi = k1 * (1 - 1 / (9 * k1) + z / (3 * math.sqrt(k1))) ** 3
    return lo, hi

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Producing well-days with equipment and speed

# COMMAND ----------

d = spark.table(DAILY).filter(
    (F.col("flow_direction") == "production") & F.col("production_date").between(*WINDOW)
)

well_make = mode_by(d, ["uwi"], "pcp_pump_make_norm").withColumnRenamed("pcp_pump_make_norm", "well_make")
well_model = mode_by(d, ["uwi"], "pcp_pump_model").withColumnRenamed("pcp_pump_model", "well_model")
well_area = mode_by(d, ["uwi"], "field_operating_area").withColumnRenamed("field_operating_area", "area")
well_rpm = (
    d.filter(F.col("is_rate_day") & (F.col("pcp_pump_rpm") > 0))
    .groupBy("uwi").agg(F.expr("percentile_approx(pcp_pump_rpm, 0.5)").alias("median_rpm"))
)

rpm_band = F.lit(None).cast("string")
for lo, hi, label in reversed(RPM_BANDS):
    rpm_band = F.when((F.col("median_rpm") >= lo) & (F.col("median_rpm") < hi), label).otherwise(rpm_band)

days = (
    d.select("uwi", "production_date", "runtime_frac", "pcp_pump_make_norm", "pcp_pump_model")
    .join(well_make, "uwi", "left")
    .join(well_model, "uwi", "left")
    .join(well_area, "uwi", "left")
    .join(well_rpm, "uwi", "left")
    .withColumn("make", F.coalesce("pcp_pump_make_norm", "well_make", F.lit("Unknown")))
    .withColumn("model", F.coalesce("pcp_pump_model", "well_model", F.lit("Unknown")))
    .withColumn("rpm_band", F.coalesce(rpm_band, F.lit("No RPM data")))
    .select("uwi", "production_date", "runtime_frac", "area", "make", "model", "rpm_band")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Failures tagged with the equipment running that day
# MAGIC
# MAGIC A failure takes the make and model from the same well-day. If that day is missing from the producing record, it falls back to the well's most common make and model in the window.

# COMMAND ----------

fail = (
    spark.table(FAILURES)
    .filter(F.col("failure_start_date").between(*WINDOW) & F.col("failure_type_norm").isin(DOWNHOLE_TYPES))
    .select("failure_id", "uwi", F.col("failure_start_date").alias("production_date"), "failure_type_norm", "cause", "rod_kind", "failure_detail")
)
tagged = (
    fail.join(days.drop("runtime_frac"), ["uwi", "production_date"], "left")
    .join(well_make, "uwi", "left")
    .join(well_model, "uwi", "left")
    .join(well_area.withColumnRenamed("area", "well_area"), "uwi", "left")
    .join(well_rpm, "uwi", "left")
    .withColumn("area", F.coalesce("area", "well_area"))
    .withColumn("make", F.coalesce("make", "well_make", F.lit("Unknown")))
    .withColumn("model", F.coalesce("model", "well_model", F.lit("Unknown")))
    .withColumn("rpm_band", F.coalesce("rpm_band", rpm_band, F.lit("No RPM data")))
    .select("failure_id", "uwi", "production_date", "failure_type_norm", "cause", "rod_kind", "failure_detail", "area", "make", "model", "rpm_band")
)

n_fail = fail.count()
n_tagged = tagged.count()
n_on_day = fail.join(days, ["uwi", "production_date"], "inner").count()
print(f"Downhole failures in window: {n_fail:,}; tagged: {n_tagged:,}; equipment from the same day: {n_on_day:,}")
print(f"Failures with no area (well not producing in window): {tagged.filter(F.col('area').isNull()).count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Rate table

# COMMAND ----------

def rate_table(dims, failure_filter=None, label=""):
    exp = days.groupBy(*dims).agg(
        F.countDistinct("uwi").alias("wells"),
        (F.sum("runtime_frac") / 365).alias("well_years"),
    )
    f = tagged if failure_filter is None else tagged.filter(failure_filter)
    cnt = f.groupBy(*dims).agg(F.count("*").alias("failures"), F.countDistinct("uwi").alias("failed_wells"))
    pdf = exp.join(cnt, dims, "left").fillna(0, subset=["failures", "failed_wells"]).toPandas()
    pdf = pdf[pdf["well_years"] > 0].copy()
    ci = pdf["failures"].apply(poisson_ci)
    pdf["rate_per_well_year"] = pdf["failures"] / pdf["well_years"]
    pdf["rate_lo"] = pd.Series([lo for lo, _ in ci], index=pdf.index) / pdf["well_years"]
    pdf["rate_hi"] = pd.Series([hi for _, hi in ci], index=pdf.index) / pdf["well_years"]
    pdf["reportable"] = (pdf["wells"] >= MIN_WELLS) & (pdf["failures"] >= MIN_FAILURES)
    pdf["failure_scope"] = label
    return pdf


area_all = rate_table(["area"], label="Rods+Pump+Tubing")
top_areas = (
    area_all.sort_values("failures", ascending=False).head(TOP_AREAS)["area"].tolist()
)
print("Top areas by failures:", top_areas)
display(spark.createDataFrame(area_all.sort_values("failures", ascending=False)))

# COMMAND ----------

def compare_within_area(dim, failure_filter=None, label="Rods+Pump+Tubing"):
    g = rate_table(["area", dim], failure_filter, label)
    a = rate_table(["area"], failure_filter, label)[["area", "rate_per_well_year", "rate_lo", "rate_hi"]]
    a.columns = ["area", "area_rate", "area_lo", "area_hi"]
    g = g.merge(a, on="area", how="left")
    g = g[g["area"].isin(top_areas)].copy()
    g["dimension"] = dim
    g = g.rename(columns={dim: "group_value"})
    g["vs_area"] = "overlaps"
    g.loc[g["reportable"] & (g["rate_lo"] > g["area_hi"]), "vs_area"] = "higher"
    g.loc[g["reportable"] & (g["rate_hi"] < g["area_lo"]), "vs_area"] = "lower"
    g.loc[~g["reportable"], "vs_area"] = "too small"
    return g.sort_values(["area", "rate_per_well_year"], ascending=[True, False])


rods_only = F.col("failure_type_norm") == "Rods"
by_make = compare_within_area("make")
by_make_rods = compare_within_area("make", rods_only, "Rods")
by_rpm = compare_within_area("rpm_band")
by_rpm_rods = compare_within_area("rpm_band", rods_only, "Rods")

top_models = (
    days.filter(F.col("area").isin(top_areas) & (F.col("model") != "Unknown"))
    .groupBy("area", "model").agg(F.countDistinct("uwi").alias("wells"))
    .withColumn("_rn", F.row_number().over(Window.partitionBy("area").orderBy(F.desc("wells"))))
    .filter(f"_rn <= {TOP_MODELS_PER_AREA}").select("area", "model")
    .toPandas()
)
by_model = compare_within_area("model")
by_model = by_model.merge(top_models.rename(columns={"model": "group_value"}), on=["area", "group_value"], how="inner")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pump make, all downhole failures

# COMMAND ----------

COLS = ["area", "group_value", "wells", "well_years", "failures", "rate_per_well_year", "rate_lo", "rate_hi", "area_rate", "vs_area"]
display(spark.createDataFrame(by_make[COLS]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pump make, rod failures only

# COMMAND ----------

display(spark.createDataFrame(by_make_rods[COLS]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pump model (top models per area by well count)

# COMMAND ----------

display(spark.createDataFrame(by_model[COLS]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Operating speed (well's median RPM on rate days)
# MAGIC
# MAGIC Faster rotation moves more fluid but adds rod and stator wear. A higher rate in fast bands within an area supports an RPM-reduction action in payback.

# COMMAND ----------

display(spark.createDataFrame(by_rpm[COLS]))
display(spark.createDataFrame(by_rpm_rods[COLS]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Charts: rate by make within each top area

# COMMAND ----------

import matplotlib.pyplot as plt

rep = by_make[by_make["reportable"]]
fig, axes = plt.subplots(2, 3, figsize=(15, 8), sharex=False)
for ax, area in zip(axes.flat, top_areas):
    g = rep[rep["area"] == area].sort_values("rate_per_well_year")
    if g.empty:
        ax.set_title(f"{area}: no reportable makes")
        ax.axis("off")
        continue
    y = range(len(g))
    ax.errorbar(
        g["rate_per_well_year"], list(y),
        xerr=[g["rate_per_well_year"] - g["rate_lo"], g["rate_hi"] - g["rate_per_well_year"]],
        fmt="o", capsize=3,
    )
    ax.axvspan(g["area_lo"].iloc[0], g["area_hi"].iloc[0], alpha=0.15, label="area 95% CI")
    ax.axvline(g["area_rate"].iloc[0], linestyle="--", linewidth=1)
    ax.set_yticks(list(y))
    ax.set_yticklabels([f"{m} ({w} wells)" for m, w in zip(g["group_value"], g["wells"])])
    ax.set_title(area)
    ax.set_xlabel("Downhole failures per producing well-year")
fig.suptitle("Failure rate by pump make, Jun 2025 to Oct 2026 (95% Poisson intervals)")
fig.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Rod kind: failure modes, not rates
# MAGIC
# MAGIC `rod_kind` (COROD vs sucker rod) comes from `failure_detail`, so it is only known for wells that failed. Comparing *rates* would need rod kind for every well. Below: how the two fail differently, and whether job text names rod kind for other wells.

# COMMAND ----------

rods = tagged.filter(F.col("failure_type_norm") == "Rods")
display(
    rods.groupBy("rod_kind").agg(F.count("*").alias("failures"), F.countDistinct("uwi").alias("wells"))
    .orderBy(F.desc("failures"))
)
display(
    rods.filter(F.col("rod_kind").isNotNull())
    .groupBy("rod_kind", "cause").count()
    .withColumn("share", F.round(F.col("count") / F.sum("count").over(Window.partitionBy("rod_kind")), 3))
    .orderBy("rod_kind", F.desc("count"))
)
display(
    rods.filter(F.col("rod_kind").isNotNull())
    .groupBy("rod_kind", "failure_detail").count()
    .withColumn("_rn", F.row_number().over(Window.partitionBy("rod_kind").orderBy(F.desc("count"))))
    .filter("_rn <= 8").drop("_rn")
    .orderBy("rod_kind", F.desc("count"))
)

# COMMAND ----------

jobs = spark.table(JOBS)
text = F.lower(F.concat_ws(" ", "job_subtype", "objective", "summary"))
mentions = jobs.select(
    "uwi",
    text.contains("corod").alias("mentions_corod"),
    (text.contains("sucker rod") | text.contains("sucker-rod")).alias("mentions_sucker"),
)
well_mentions = mentions.groupBy("uwi").agg(
    F.max(F.col("mentions_corod").cast("int")).alias("corod"),
    F.max(F.col("mentions_sucker").cast("int")).alias("sucker"),
)
producing_wells = days.select("uwi").distinct()
display(
    producing_wells.join(well_mentions, "uwi", "left").fillna(0)
    .agg(
        F.count("*").alias("producing_wells_in_window"),
        F.sum("corod").alias("wells_with_corod_in_job_text"),
        F.sum("sucker").alias("wells_with_sucker_rod_in_job_text"),
        F.sum(((F.col("corod") + F.col("sucker")) > 0).cast("int")).alias("wells_with_either"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC If most producing wells have rod kind in job text, a COROD vs sucker-rod **rate** comparison is possible as a follow-up. If not, keep it to failure modes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Cause mix by area (input to payback action choice)

# COMMAND ----------

display(
    tagged.filter(F.col("area").isin(top_areas))
    .groupBy("area", "cause").count()
    .withColumn("share", F.round(F.col("count") / F.sum("count").over(Window.partitionBy("area")), 3))
    .orderBy("area", F.desc("count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write `gold_failure_rates` and check

# COMMAND ----------

out = pd.concat(
    [
        by_make.assign(failure_scope="Rods+Pump+Tubing"),
        by_make_rods.assign(failure_scope="Rods"),
        by_model.assign(failure_scope="Rods+Pump+Tubing"),
        by_rpm.assign(failure_scope="Rods+Pump+Tubing"),
        by_rpm_rods.assign(failure_scope="Rods"),
    ],
    ignore_index=True,
)[[
    "dimension", "failure_scope", "area", "group_value", "wells", "failed_wells", "well_years", "failures",
    "rate_per_well_year", "rate_lo", "rate_hi", "area_rate", "area_lo", "area_hi", "reportable", "vs_area",
]]
out["window_start"], out["window_end"] = WINDOW
spark.createDataFrame(out).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(RATES_TARGET)

total_years = days.agg(F.sum("runtime_frac")).first()[0] / 365
total_failures = tagged.filter(F.col("area").isNotNull()).count()
checks = {
    "every downhole failure tagged once": n_tagged == n_fail,
    "make rows sum to area failures": (
        by_make.groupby("area")["failures"].sum().reindex(top_areas).fillna(0).astype(int).tolist()
        == area_all.set_index("area").loc[top_areas, "failures"].astype(int).tolist()
    ),
}
for name, ok in checks.items():
    print(("PASS " if ok else "FAIL ") + name)
print(f"Producing well-years in window: {total_years:,.0f}; downhole failures on producing wells: {total_failures:,}; "
      f"overall rate: {total_failures / total_years:.3f} per well-year")
print("Higher than area (non-overlapping 95% CI):")
display(spark.createDataFrame(out[out["vs_area"] == "higher"]))
print("Lower than area:")
display(spark.createDataFrame(out[out["vs_area"] == "lower"]))
assert all(checks.values()), "One or more checks failed"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task s5: Pick 2 actions and payback

# COMMAND ----------

# DBTITLE 1,Calculate Cost per Failure
# MAGIC %sql
# MAGIC -- Task s5: Cost per failure by failure_type (Mean & Median)
# MAGIC -- Cost per failure = linked repair job_cost + (downhole lost_oil_m3 from stop day to +14d * 6.2898 * 32.50)
# MAGIC
# MAGIC WITH failure_costs AS (
# MAGIC     SELECT
# MAGIC         f.failure_id,
# MAGIC         f.uwi,
# MAGIC         f.failure_start_date,
# MAGIC         f.failure_type,
# MAGIC         f.cause,
# MAGIC         f.linked_job_cost AS linked_repair_job_cost,
# MAGIC         COALESCE(dt.downhole_lost_oil_m3, 0) AS downhole_lost_oil_m3,
# MAGIC         COALESCE(dt.downhole_lost_oil_m3, 0) * 6.2898 * 32.50 AS lost_oil_cost_cad,
# MAGIC         f.linked_job_cost + (COALESCE(dt.downhole_lost_oil_m3, 0) * 6.2898 * 32.50) AS cost_per_failure
# MAGIC     FROM workspace.dsmlc.silver_failures f
# MAGIC     LEFT JOIN workspace.dsmlc.gold_failure_downtime dt
# MAGIC         ON f.failure_id = dt.failure_id
# MAGIC     WHERE f.failure_start_date >= DATE '2025-06-01'
# MAGIC       AND f.linked_job_cost IS NOT NULL
# MAGIC       AND f.linked_job_cost > 0
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     failure_type,
# MAGIC     COUNT(*) AS linked_failure_count,
# MAGIC     
# MAGIC     -- Linked Repair Cost ($)
# MAGIC     ROUND(MEDIAN(linked_repair_job_cost), 0) AS median_repair_cost,
# MAGIC     ROUND(AVG(linked_repair_job_cost), 0) AS mean_repair_cost,
# MAGIC     
# MAGIC     -- Downhole Lost Oil Volume (m3) & Value ($) at C$32.50/boe
# MAGIC     ROUND(MEDIAN(downhole_lost_oil_m3), 1) AS median_lost_oil_m3,
# MAGIC     ROUND(MEDIAN(lost_oil_cost_cad), 0) AS median_lost_oil_cost,
# MAGIC     ROUND(AVG(lost_oil_cost_cad), 0) AS mean_lost_oil_cost,
# MAGIC     
# MAGIC     -- Total Cost Per Failure (Repair $ + Lost Oil $)
# MAGIC     ROUND(MEDIAN(cost_per_failure), 0) AS median_cost_per_failure,
# MAGIC     ROUND(AVG(cost_per_failure), 0) AS mean_cost_per_failure,
# MAGIC     ROUND(SUM(cost_per_failure), 0) AS total_failure_cost
# MAGIC FROM failure_costs
# MAGIC GROUP BY failure_type
# MAGIC ORDER BY total_failure_cost DESC;

# COMMAND ----------

# DBTITLE 1,Candidate Action Ranking
# MAGIC %sql
# MAGIC -- Task s5 (Part 2A): Rank failure_type x failure_cause by count and total dollars
# MAGIC -- Helps defend why the 2 actions were chosen by failure type and dollars first, then cause
# MAGIC
# MAGIC WITH failure_costs AS (
# MAGIC     SELECT
# MAGIC         f.failure_id,
# MAGIC         f.uwi,
# MAGIC         f.failure_type,
# MAGIC         COALESCE(f.cause, 'Unspecified') AS failure_cause,
# MAGIC         f.linked_job_cost AS repair_cost,
# MAGIC         COALESCE(dt.downhole_lost_oil_m3, 0) AS lost_oil_m3,
# MAGIC         COALESCE(dt.downhole_lost_oil_m3, 0) * 6.2898 * 32.50 AS lost_oil_cost
# MAGIC     FROM workspace.dsmlc.silver_failures f
# MAGIC     LEFT JOIN workspace.dsmlc.gold_failure_downtime dt
# MAGIC         ON f.failure_id = dt.failure_id
# MAGIC     WHERE f.failure_start_date >= DATE '2025-06-01'
# MAGIC       AND f.linked_job_cost > 0
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     failure_type,
# MAGIC     failure_cause,
# MAGIC     COUNT(*) AS failure_count,
# MAGIC     COUNT(DISTINCT uwi) AS distinct_wells,
# MAGIC     ROUND(MEDIAN(repair_cost), 0) AS median_repair_cost,
# MAGIC     ROUND(MEDIAN(lost_oil_m3), 1) AS median_lost_m3,
# MAGIC     ROUND(MEDIAN(repair_cost + lost_oil_cost), 0) AS median_cost_per_failure,
# MAGIC     ROUND(AVG(repair_cost + lost_oil_cost), 0) AS mean_cost_per_failure,
# MAGIC     ROUND(SUM(repair_cost + lost_oil_cost), 0) AS total_pool_dollars
# MAGIC FROM failure_costs
# MAGIC GROUP BY failure_type, failure_cause
# MAGIC ORDER BY total_pool_dollars DESC;

# COMMAND ----------

# DBTITLE 1,Break-Even Payback and Sensitivity Grid
# MAGIC %sql
# MAGIC -- Task s5 (Part 2B): Break-Even (N*), Payback & Sensitivity Grid for 2 Actions
# MAGIC -- Action 1: Rods (Fatigue / Corrosion — Rod string upgrade or corrosion inhibition program)
# MAGIC -- Action 2: Pump & Tubing (High-dollar workover — Pump/tubing upgrade or RPM/pump-off control)
# MAGIC
# MAGIC WITH base_failures AS (
# MAGIC     SELECT
# MAGIC         f.failure_id,
# MAGIC         f.uwi,
# MAGIC         f.failure_type,
# MAGIC         f.cause,
# MAGIC         f.linked_job_cost,
# MAGIC         COALESCE(dt.downhole_lost_oil_m3, 0) AS lost_m3,
# MAGIC         CASE
# MAGIC             WHEN f.failure_type = 'Rods' AND f.cause IN ('Fatigue', 'Corrosion')
# MAGIC                 THEN 'Action 1: Rods (Fatigue & Corrosion Upgrade/Inhibition)'
# MAGIC             WHEN f.failure_type IN ('Pump', 'Tubing')
# MAGIC                 THEN 'Action 2: Pump & Tubing (High-Cost Workover Prevention)'
# MAGIC             ELSE NULL
# MAGIC         END AS action_group
# MAGIC     FROM workspace.dsmlc.silver_failures f
# MAGIC     LEFT JOIN workspace.dsmlc.gold_failure_downtime dt
# MAGIC         ON f.failure_id = dt.failure_id
# MAGIC     WHERE f.failure_start_date >= DATE '2025-06-01'
# MAGIC       AND f.linked_job_cost > 0
# MAGIC ),
# MAGIC
# MAGIC -- Bad-actor failure frequency on eligible wells (>= 3 failures in 16.8 months = 1.40 years)
# MAGIC bad_actor_rates AS (
# MAGIC     SELECT
# MAGIC         action_group,
# MAGIC         COUNT(DISTINCT uwi) AS bad_actor_well_count,
# MAGIC         AVG(well_failures / 1.40) AS mean_failures_per_well_year,
# MAGIC         MEDIAN(well_failures / 1.40) AS median_failures_per_well_year
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             action_group,
# MAGIC             uwi,
# MAGIC             COUNT(*) AS well_failures
# MAGIC         FROM base_failures
# MAGIC         WHERE action_group IS NOT NULL
# MAGIC         GROUP BY action_group, uwi
# MAGIC         HAVING COUNT(*) >= 3
# MAGIC     )
# MAGIC     GROUP BY action_group
# MAGIC ),
# MAGIC
# MAGIC action_metrics AS (
# MAGIC     SELECT
# MAGIC         bf.action_group,
# MAGIC         COUNT(*) AS total_failures,
# MAGIC         MEDIAN(bf.linked_job_cost) AS median_repair_cost,
# MAGIC         AVG(bf.linked_job_cost) AS mean_repair_cost,
# MAGIC         MEDIAN(bf.lost_m3) AS median_lost_m3,
# MAGIC         AVG(bf.lost_m3) AS mean_lost_m3,
# MAGIC         ba.bad_actor_well_count,
# MAGIC         ba.mean_failures_per_well_year,
# MAGIC         ba.median_failures_per_well_year
# MAGIC     FROM base_failures bf
# MAGIC     LEFT JOIN bad_actor_rates ba
# MAGIC         ON bf.action_group = ba.action_group
# MAGIC     WHERE bf.action_group IS NOT NULL
# MAGIC     GROUP BY
# MAGIC         bf.action_group,
# MAGIC         ba.bad_actor_well_count,
# MAGIC         ba.mean_failures_per_well_year,
# MAGIC         ba.median_failures_per_well_year
# MAGIC ),
# MAGIC
# MAGIC sensitivity_params AS (
# MAGIC     SELECT * FROM VALUES
# MAGIC         -- (netback_label ($), netback_bbl, cost_mult_label, cost_mult, avoided_share)
# MAGIC         ('Netback -25% (24.38)', 24.375, '0.5x Cost (17.1k)', 0.5, 0.25),
# MAGIC         ('Netback -25% (24.38)', 24.375, '0.5x Cost (17.1k)', 0.5, 0.50),
# MAGIC         ('Netback -25% (24.38)', 24.375, '1.0x Base (34.2k)', 1.0, 0.25),
# MAGIC         ('Netback -25% (24.38)', 24.375, '1.0x Base (34.2k)', 1.0, 0.50),
# MAGIC         ('Netback -25% (24.38)', 24.375, '2.0x Cost (68.3k)', 2.0, 0.25),
# MAGIC         ('Netback -25% (24.38)', 24.375, '2.0x Cost (68.3k)', 2.0, 0.50),
# MAGIC         ('Netback Base (32.50)', 32.500, '0.5x Cost (17.1k)', 0.5, 0.25),
# MAGIC         ('Netback Base (32.50)', 32.500, '0.5x Cost (17.1k)', 0.5, 0.50),
# MAGIC         ('Netback Base (32.50)', 32.500, '1.0x Base (34.2k)', 1.0, 0.25),
# MAGIC         ('Netback Base (32.50)', 32.500, '1.0x Base (34.2k)', 1.0, 0.50),
# MAGIC         ('Netback Base (32.50)', 32.500, '2.0x Cost (68.3k)', 2.0, 0.25),
# MAGIC         ('Netback Base (32.50)', 32.500, '2.0x Cost (68.3k)', 2.0, 0.50),
# MAGIC         ('Netback +25% (40.63)', 40.625, '0.5x Cost (17.1k)', 0.5, 0.25),
# MAGIC         ('Netback +25% (40.63)', 40.625, '0.5x Cost (17.1k)', 0.5, 0.50),
# MAGIC         ('Netback +25% (40.63)', 40.625, '1.0x Base (34.2k)', 1.0, 0.25),
# MAGIC         ('Netback +25% (40.63)', 40.625, '1.0x Base (34.2k)', 1.0, 0.50),
# MAGIC         ('Netback +25% (40.63)', 40.625, '2.0x Cost (68.3k)', 2.0, 0.25),
# MAGIC         ('Netback +25% (40.63)', 40.625, '2.0x Cost (68.3k)', 2.0, 0.50)
# MAGIC     AS t(netback_label, netback_bbl, cost_mult_label, cost_mult, avoided_share)
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     m.action_group,
# MAGIC     p.netback_label,
# MAGIC     p.cost_mult_label,
# MAGIC     CONCAT(CAST(ROUND(p.avoided_share * 100, 0) AS INT), '%') AS avoided_failure_share,
# MAGIC     
# MAGIC     -- Action cost proxy ($34,152 Workover median * multiplier)
# MAGIC     ROUND(34152.0 * p.cost_mult, 0) AS assumed_action_cost,
# MAGIC     
# MAGIC     -- Cost per failure at this netback (using median repair + median lost oil)
# MAGIC     ROUND(m.median_repair_cost + (m.median_lost_m3 * 6.2898 * p.netback_bbl), 0) AS median_cost_per_failure,
# MAGIC     
# MAGIC     -- N* = Break-even failures avoided per well-year
# MAGIC     ROUND(
# MAGIC         (34152.0 * p.cost_mult) / (m.median_repair_cost + (m.median_lost_m3 * 6.2898 * p.netback_bbl)),
# MAGIC         2
# MAGIC     ) AS n_star_breakeven_failures,
# MAGIC     
# MAGIC     -- Expected failures avoided per year on a bad-actor well (>=3 failures in 16.8m)
# MAGIC     ROUND(m.mean_failures_per_well_year * p.avoided_share, 2) AS expected_failures_avoided_yr,
# MAGIC     
# MAGIC     -- Payback period in years on a bad-actor well: Action Cost / Annual Expected Savings
# MAGIC     ROUND(
# MAGIC         (34152.0 * p.cost_mult) /
# MAGIC         ((m.mean_failures_per_well_year * p.avoided_share) * (m.median_repair_cost + (m.median_lost_m3 * 6.2898 * p.netback_bbl))),
# MAGIC         2
# MAGIC     ) AS payback_years_on_bad_actor
# MAGIC FROM action_metrics m
# MAGIC CROSS JOIN sensitivity_params p
# MAGIC ORDER BY
# MAGIC     m.action_group,
# MAGIC     p.cost_mult,
# MAGIC     p.netback_bbl,
# MAGIC     p.avoided_share;

# COMMAND ----------

# DBTITLE 1,Generate Gold Table for cost per failure type
# MAGIC %sql
# MAGIC -- 1. Save Cost Per Failure by Type (for Dashboard Page 3 & Task n4)
# MAGIC CREATE OR REPLACE TABLE workspace.dsmlc.gold_cost_per_failure AS
# MAGIC WITH failure_costs AS (
# MAGIC     SELECT
# MAGIC         f.failure_id,
# MAGIC         f.uwi,
# MAGIC         f.failure_start_date,
# MAGIC         f.failure_type,
# MAGIC         f.cause,
# MAGIC         f.linked_job_cost AS linked_repair_job_cost,
# MAGIC         COALESCE(dt.downhole_lost_oil_m3, 0) AS downhole_lost_oil_m3,
# MAGIC         COALESCE(dt.downhole_lost_oil_m3, 0) * 6.2898 * 32.50 AS lost_oil_cost_cad,
# MAGIC         f.linked_job_cost + (COALESCE(dt.downhole_lost_oil_m3, 0) * 6.2898 * 32.50) AS cost_per_failure
# MAGIC     FROM workspace.dsmlc.silver_failures f
# MAGIC     LEFT JOIN workspace.dsmlc.gold_failure_downtime dt
# MAGIC         ON f.failure_id = dt.failure_id
# MAGIC     WHERE f.failure_start_date >= DATE '2025-06-01'
# MAGIC       AND f.linked_job_cost IS NOT NULL
# MAGIC       AND f.linked_job_cost > 0
# MAGIC )
# MAGIC SELECT
# MAGIC     failure_type,
# MAGIC     COUNT(*) AS linked_failure_count,
# MAGIC     ROUND(MEDIAN(linked_repair_job_cost), 0) AS median_repair_cost,
# MAGIC     ROUND(AVG(linked_repair_job_cost), 0) AS mean_repair_cost,
# MAGIC     ROUND(MEDIAN(downhole_lost_oil_m3), 1) AS median_lost_oil_m3,
# MAGIC     ROUND(MEDIAN(lost_oil_cost_cad), 0) AS median_lost_oil_cost,
# MAGIC     ROUND(AVG(lost_oil_cost_cad), 0) AS mean_lost_oil_cost,
# MAGIC     ROUND(MEDIAN(cost_per_failure), 0) AS median_cost_per_failure,
# MAGIC     ROUND(AVG(cost_per_failure), 0) AS mean_cost_per_failure,
# MAGIC     ROUND(SUM(cost_per_failure), 0) AS total_failure_cost
# MAGIC FROM failure_costs
# MAGIC GROUP BY failure_type;

# COMMAND ----------

# DBTITLE 1,Generate Gold Table for payback grid
# MAGIC %sql
# MAGIC -- 2. Save Payback & Sensitivity Grid for the 2 Actions (for Dashboard Page 3 & Task s11)
# MAGIC CREATE OR REPLACE TABLE workspace.dsmlc.gold_action_payback_grid AS
# MAGIC WITH base_failures AS (
# MAGIC     SELECT
# MAGIC         f.failure_id,
# MAGIC         f.uwi,
# MAGIC         f.failure_type,
# MAGIC         f.cause,
# MAGIC         f.linked_job_cost,
# MAGIC         COALESCE(dt.downhole_lost_oil_m3, 0) AS lost_m3,
# MAGIC         CASE
# MAGIC             WHEN f.failure_type = 'Rods' AND f.cause IN ('Fatigue', 'Corrosion')
# MAGIC                 THEN 'Action 1: Rods (Fatigue & Corrosion Upgrade/Inhibition)'
# MAGIC             WHEN f.failure_type IN ('Pump', 'Tubing')
# MAGIC                 THEN 'Action 2: Pump & Tubing (High-Cost Workover Prevention)'
# MAGIC             ELSE NULL
# MAGIC         END AS action_group
# MAGIC     FROM workspace.dsmlc.silver_failures f
# MAGIC     LEFT JOIN workspace.dsmlc.gold_failure_downtime dt
# MAGIC         ON f.failure_id = dt.failure_id
# MAGIC     WHERE f.failure_start_date >= DATE '2025-06-01'
# MAGIC       AND f.linked_job_cost > 0
# MAGIC ),
# MAGIC bad_actor_rates AS (
# MAGIC     SELECT
# MAGIC         action_group,
# MAGIC         COUNT(DISTINCT uwi) AS bad_actor_well_count,
# MAGIC         AVG(well_failures / 1.40) AS mean_failures_per_well_year,
# MAGIC         MEDIAN(well_failures / 1.40) AS median_failures_per_well_year
# MAGIC     FROM (
# MAGIC         SELECT action_group, uwi, COUNT(*) AS well_failures
# MAGIC         FROM base_failures
# MAGIC         WHERE action_group IS NOT NULL
# MAGIC         GROUP BY action_group, uwi
# MAGIC         HAVING COUNT(*) >= 3
# MAGIC     )
# MAGIC     GROUP BY action_group
# MAGIC ),
# MAGIC action_metrics AS (
# MAGIC     SELECT
# MAGIC         bf.action_group,
# MAGIC         COUNT(*) AS total_failures,
# MAGIC         MEDIAN(bf.linked_job_cost) AS median_repair_cost,
# MAGIC         MEDIAN(bf.lost_m3) AS median_lost_m3,
# MAGIC         ba.bad_actor_well_count,
# MAGIC         ba.mean_failures_per_well_year
# MAGIC     FROM base_failures bf
# MAGIC     LEFT JOIN bad_actor_rates ba ON bf.action_group = ba.action_group
# MAGIC     WHERE bf.action_group IS NOT NULL
# MAGIC     GROUP BY bf.action_group, ba.bad_actor_well_count, ba.mean_failures_per_well_year
# MAGIC ),
# MAGIC sensitivity_params AS (
# MAGIC     SELECT * FROM VALUES
# MAGIC         ('Netback -25% (24.38)', 24.375, '0.5x Cost (17.1k)', 0.5, 0.25),
# MAGIC         ('Netback -25% (24.38)', 24.375, '0.5x Cost (17.1k)', 0.5, 0.50),
# MAGIC         ('Netback -25% (24.38)', 24.375, '1.0x Base (34.2k)', 1.0, 0.25),
# MAGIC         ('Netback -25% (24.38)', 24.375, '1.0x Base (34.2k)', 1.0, 0.50),
# MAGIC         ('Netback -25% (24.38)', 24.375, '2.0x Cost (68.3k)', 2.0, 0.25),
# MAGIC         ('Netback -25% (24.38)', 24.375, '2.0x Cost (68.3k)', 2.0, 0.50),
# MAGIC         ('Netback Base (32.50)', 32.500, '0.5x Cost (17.1k)', 0.5, 0.25),
# MAGIC         ('Netback Base (32.50)', 32.500, '0.5x Cost (17.1k)', 0.5, 0.50),
# MAGIC         ('Netback Base (32.50)', 32.500, '1.0x Base (34.2k)', 1.0, 0.25),
# MAGIC         ('Netback Base (32.50)', 32.500, '1.0x Base (34.2k)', 1.0, 0.50),
# MAGIC         ('Netback Base (32.50)', 32.500, '2.0x Cost (68.3k)', 2.0, 0.25),
# MAGIC         ('Netback Base (32.50)', 32.500, '2.0x Cost (68.3k)', 2.0, 0.50),
# MAGIC         ('Netback +25% (40.63)', 40.625, '0.5x Cost (17.1k)', 0.5, 0.25),
# MAGIC         ('Netback +25% (40.63)', 40.625, '0.5x Cost (17.1k)', 0.5, 0.50),
# MAGIC         ('Netback +25% (40.63)', 40.625, '1.0x Base (34.2k)', 1.0, 0.25),
# MAGIC         ('Netback +25% (40.63)', 40.625, '1.0x Base (34.2k)', 1.0, 0.50),
# MAGIC         ('Netback +25% (40.63)', 40.625, '2.0x Cost (68.3k)', 2.0, 0.25),
# MAGIC         ('Netback +25% (40.63)', 40.625, '2.0x Cost (68.3k)', 2.0, 0.50)
# MAGIC     AS t(netback_label, netback_bbl, cost_mult_label, cost_mult, avoided_share)
# MAGIC )
# MAGIC SELECT
# MAGIC     m.action_group,
# MAGIC     p.netback_label,
# MAGIC     p.cost_mult_label,
# MAGIC     CONCAT(CAST(ROUND(p.avoided_share * 100, 0) AS INT), '%') AS avoided_failure_share,
# MAGIC     ROUND(34152.0 * p.cost_mult, 0) AS assumed_action_cost,
# MAGIC     ROUND(m.median_repair_cost + (m.median_lost_m3 * 6.2898 * p.netback_bbl), 0) AS median_cost_per_failure,
# MAGIC     ROUND((34152.0 * p.cost_mult) / (m.median_repair_cost + (m.median_lost_m3 * 6.2898 * p.netback_bbl)), 2) AS n_star_breakeven_failures,
# MAGIC     ROUND(m.mean_failures_per_well_year * p.avoided_share, 2) AS expected_failures_avoided_yr,
# MAGIC     ROUND(
# MAGIC         (34152.0 * p.cost_mult) /
# MAGIC         ((m.mean_failures_per_well_year * p.avoided_share) * (m.median_repair_cost + (m.median_lost_m3 * 6.2898 * p.netback_bbl))),
# MAGIC         2
# MAGIC     ) AS payback_years_on_bad_actor
# MAGIC FROM action_metrics m
# MAGIC CROSS JOIN sensitivity_params p;

# COMMAND ----------

# DBTITLE 1,Generate Gold Table for action eligible wells
# MAGIC %sql
# MAGIC -- 3. Save Bad-Actor Wells Eligible for Each Action (for Dashboard Page 3 widget 2)
# MAGIC CREATE OR REPLACE TABLE workspace.dsmlc.gold_action_eligible_wells AS
# MAGIC WITH well_action_failures AS (
# MAGIC     SELECT
# MAGIC         f.uwi,
# MAGIC         CASE
# MAGIC             WHEN f.failure_type = 'Rods' AND f.cause IN ('Fatigue', 'Corrosion')
# MAGIC                 THEN 'Action 1: Rods (Fatigue & Corrosion Upgrade/Inhibition)'
# MAGIC             WHEN f.failure_type IN ('Pump', 'Tubing')
# MAGIC                 THEN 'Action 2: Pump & Tubing (High-Cost Workover Prevention)'
# MAGIC             ELSE NULL
# MAGIC         END AS action_group,
# MAGIC         COUNT(*) AS failure_count_16_8m,
# MAGIC         COUNT(*) / 1.40 AS failures_per_year,
# MAGIC         SUM(f.linked_job_cost + (COALESCE(dt.downhole_lost_oil_m3, 0) * 6.2898 * 32.50)) AS historical_failure_cost,
# MAGIC         AVG(f.linked_job_cost + (COALESCE(dt.downhole_lost_oil_m3, 0) * 6.2898 * 32.50)) AS avg_cost_per_failure
# MAGIC     FROM workspace.dsmlc.silver_failures f
# MAGIC     LEFT JOIN workspace.dsmlc.gold_failure_downtime dt
# MAGIC         ON f.failure_id = dt.failure_id
# MAGIC     WHERE f.failure_start_date >= DATE '2025-06-01'
# MAGIC       AND f.linked_job_cost > 0
# MAGIC     GROUP BY
# MAGIC         f.uwi,
# MAGIC         CASE
# MAGIC             WHEN f.failure_type = 'Rods' AND f.cause IN ('Fatigue', 'Corrosion')
# MAGIC                 THEN 'Action 1: Rods (Fatigue & Corrosion Upgrade/Inhibition)'
# MAGIC             WHEN f.failure_type IN ('Pump', 'Tubing')
# MAGIC                 THEN 'Action 2: Pump & Tubing (High-Cost Workover Prevention)'
# MAGIC             ELSE NULL
# MAGIC         END
# MAGIC )
# MAGIC SELECT
# MAGIC     uwi,
# MAGIC     action_group,
# MAGIC     failure_count_16_8m,
# MAGIC     ROUND(failures_per_year, 2) AS failures_per_year,
# MAGIC     ROUND(historical_failure_cost, 0) AS historical_failure_cost,
# MAGIC     -- Expected annual gross savings range at 25% and 50% avoided failure share
# MAGIC     ROUND((historical_failure_cost / 1.40) * 0.25, 0) AS expected_annual_savings_25pct,
# MAGIC     ROUND((historical_failure_cost / 1.40) * 0.50, 0) AS expected_annual_savings_50pct
# MAGIC FROM well_action_failures
# MAGIC WHERE action_group IS NOT NULL
# MAGIC   AND failure_count_16_8m >= 3
# MAGIC ORDER BY expected_annual_savings_50pct DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task n4: Budget Scenarios

# COMMAND ----------

# DBTITLE 1,Create Gold Budget Table
# MAGIC %sql
# MAGIC -- Task n4: Precomputed Budget Scenarios (25, 50, 100, and 200 wells)
# MAGIC -- Uses Gamma-Poisson (Empirical Bayes) shrinkage of each well's failure rate toward its area rate,
# MAGIC -- joined with s5 cost_per_failure by failure_type and n2 recency/failure counts.
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workspace.dsmlc.gold_budget_scenarios AS
# MAGIC WITH well_exposure AS (
# MAGIC     -- Producing exposure (well-years) and area in the failure window (Jun 2025 - Oct 2026)
# MAGIC     SELECT
# MAGIC         uwi,
# MAGIC         MAX(field_operating_area) AS area,
# MAGIC         MAX(facility_name) AS facility,
# MAGIC         -- Exposure in well-years from runtime_frac (or producing days / 365.25), floored at 0.25 yr
# MAGIC         GREATEST(SUM(COALESCE(runtime_frac, hours_on_prod / 24.0, 0)) / 365.25, 0.25) AS exposure_yr
# MAGIC     FROM workspace.dsmlc.silver_daily
# MAGIC     WHERE production_date >= DATE '2025-06-01'
# MAGIC     GROUP BY uwi
# MAGIC ),
# MAGIC
# MAGIC cost_by_type AS (
# MAGIC     -- Pull median cost per failure by failure_type from s5 gold_cost_per_failure
# MAGIC     SELECT
# MAGIC         failure_type,
# MAGIC         median_cost_per_failure
# MAGIC     FROM workspace.dsmlc.gold_cost_per_failure
# MAGIC ),
# MAGIC
# MAGIC well_failures AS (
# MAGIC     -- Count failures and compute weighted cost per failure for each well (Jun 2025+)
# MAGIC     SELECT
# MAGIC         f.uwi,
# MAGIC         COUNT(*) AS failure_count,
# MAGIC         -- Dominant failure type on this well
# MAGIC         MODE(f.failure_type) AS primary_failure_type,
# MAGIC         MODE(COALESCE(f.cause, 'Unspecified')) AS primary_failure_cause,
# MAGIC         -- Average of type-level median cost per failure across this well's failure mix
# MAGIC         AVG(COALESCE(c.median_cost_per_failure, f.linked_job_cost + COALESCE(dt.downhole_lost_oil_m3, 0) * 6.2898 * 32.50)) AS well_cost_per_failure
# MAGIC     FROM workspace.dsmlc.silver_failures f
# MAGIC     LEFT JOIN cost_by_type c
# MAGIC         ON f.failure_type = c.failure_type
# MAGIC     LEFT JOIN workspace.dsmlc.gold_failure_downtime dt
# MAGIC         ON f.failure_id = dt.failure_id
# MAGIC     WHERE f.failure_start_date >= DATE '2025-06-01'
# MAGIC     GROUP BY f.uwi
# MAGIC ),
# MAGIC
# MAGIC well_base AS (
# MAGIC     SELECT
# MAGIC         e.uwi,
# MAGIC         COALESCE(e.area, 'Unknown') AS area,
# MAGIC         e.facility,
# MAGIC         e.exposure_yr,
# MAGIC         COALESCE(wf.failure_count, 0) AS failure_count,
# MAGIC         COALESCE(wf.failure_count, 0) / e.exposure_yr AS raw_rate_yr,
# MAGIC         wf.primary_failure_type,
# MAGIC         wf.primary_failure_cause,
# MAGIC         wf.well_cost_per_failure
# MAGIC     FROM well_exposure e
# MAGIC     LEFT JOIN well_failures wf
# MAGIC         ON e.uwi = wf.uwi
# MAGIC ),
# MAGIC
# MAGIC area_priors AS (
# MAGIC     -- Empirical Bayes (Gamma-Poisson) prior parameters per area
# MAGIC     -- Shrinks each well toward its area mean rate with prior weight beta_prior = 1.0 well-year
# MAGIC     -- (or method-of-moments overdispersion weight when area variance > area mean)
# MAGIC     SELECT
# MAGIC         area,
# MAGIC         SUM(failure_count) / SUM(exposure_yr) AS area_mean_rate_yr,
# MAGIC         VAR_POP(raw_rate_yr) AS area_rate_var,
# MAGIC         -- Prior pseudo-exposure beta (clamped between 0.5 and 2.0 well-years for numerical stability)
# MAGIC         LEAST(
# MAGIC             GREATEST(
# MAGIC                 CASE
# MAGIC                     WHEN VAR_POP(raw_rate_yr) > (SUM(failure_count) / SUM(exposure_yr))
# MAGIC                     THEN (SUM(failure_count) / SUM(exposure_yr)) / (VAR_POP(raw_rate_yr) - (SUM(failure_count) / SUM(exposure_yr)))
# MAGIC                     ELSE 1.0
# MAGIC                 END,
# MAGIC                 0.5
# MAGIC             ),
# MAGIC             2.0
# MAGIC         ) AS prior_beta_yr
# MAGIC     FROM well_base
# MAGIC     GROUP BY area
# MAGIC ),
# MAGIC
# MAGIC shrunken_wells AS (
# MAGIC     SELECT
# MAGIC         wb.uwi,
# MAGIC         wb.area,
# MAGIC         wb.facility,
# MAGIC         wb.exposure_yr,
# MAGIC         wb.failure_count,
# MAGIC         wb.raw_rate_yr,
# MAGIC         ap.area_mean_rate_yr,
# MAGIC         -- Gamma-Poisson posterior mean failure rate: (y_i + alpha) / (t_i + beta), where alpha = area_mean * beta
# MAGIC         (wb.failure_count + (ap.area_mean_rate_yr * ap.prior_beta_yr))
# MAGIC             / (wb.exposure_yr + ap.prior_beta_yr) AS shrunken_rate_yr,
# MAGIC         wb.primary_failure_type,
# MAGIC         wb.primary_failure_cause,
# MAGIC         wb.well_cost_per_failure,
# MAGIC         CASE
# MAGIC             WHEN wb.primary_failure_type = 'Rods'
# MAGIC                 THEN 'Action 1: Rods (Fatigue & Corrosion Upgrade/Inhibition)'
# MAGIC             WHEN wb.primary_failure_type IN ('Pump', 'Tubing')
# MAGIC                 THEN 'Action 2: Pump & Tubing (High-Cost Workover Prevention)'
# MAGIC             ELSE 'Action 1: Rods (General Reliability Intervention)'
# MAGIC         END AS recommended_action,
# MAGIC         -- Assumed install action cost proxy ($34,152 Workover median from s5)
# MAGIC         34152.0 AS action_cost_cad
# MAGIC     FROM well_base wb
# MAGIC     INNER JOIN area_priors ap
# MAGIC         ON wb.area = ap.area
# MAGIC     WHERE wb.failure_count > 0
# MAGIC       AND wb.well_cost_per_failure IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC scored_wells AS (
# MAGIC     SELECT
# MAGIC         uwi,
# MAGIC         area,
# MAGIC         facility,
# MAGIC         failure_count,
# MAGIC         ROUND(exposure_yr, 2) AS exposure_yr,
# MAGIC         ROUND(raw_rate_yr, 2) AS raw_failure_rate_yr,
# MAGIC         ROUND(area_mean_rate_yr, 2) AS area_prior_rate_yr,
# MAGIC         ROUND(shrunken_rate_yr, 2) AS shrunken_failure_rate_yr,
# MAGIC         primary_failure_type,
# MAGIC         primary_failure_cause,
# MAGIC         recommended_action,
# MAGIC         ROUND(well_cost_per_failure, 0) AS cost_per_failure_cad,
# MAGIC         ROUND(action_cost_cad, 0) AS action_cost_cad,
# MAGIC         
# MAGIC         -- Annual expected gross savings at 25%, 34% (vendor case), and 50% avoided share
# MAGIC         ROUND(shrunken_rate_yr * well_cost_per_failure * 0.25, 0) AS gross_savings_25pct_cad,
# MAGIC         ROUND(shrunken_rate_yr * well_cost_per_failure * 0.34, 0) AS gross_savings_34pct_cad,
# MAGIC         ROUND(shrunken_rate_yr * well_cost_per_failure * 0.50, 0) AS gross_savings_50pct_cad,
# MAGIC         
# MAGIC         -- Annual expected net avoided cost (Gross Savings - Action Cost) at 25% and 50%
# MAGIC         ROUND((shrunken_rate_yr * well_cost_per_failure * 0.25) - action_cost_cad, 0) AS net_avoided_cost_25pct_cad,
# MAGIC         ROUND((shrunken_rate_yr * well_cost_per_failure * 0.50) - action_cost_cad, 0) AS net_avoided_cost_50pct_cad,
# MAGIC         
# MAGIC         -- Expected avoided cost per dollar spent (at 34% midpoint / 50% share) for greedy ranking
# MAGIC         ROUND((shrunken_rate_yr * well_cost_per_failure * 0.50) / action_cost_cad, 3) AS savings_per_dollar_50pct,
# MAGIC         
# MAGIC         -- Break-even payback in years for this well (at 25% and 50% avoided share)
# MAGIC         ROUND(action_cost_cad / NULLIF(shrunken_rate_yr * well_cost_per_failure * 0.25, 0), 2) AS breakeven_years_25pct,
# MAGIC         ROUND(action_cost_cad / NULLIF(shrunken_rate_yr * well_cost_per_failure * 0.50, 0), 2) AS breakeven_years_50pct,
# MAGIC         
# MAGIC         -- Greedy rank by expected avoided cost per dollar
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             ORDER BY ((shrunken_rate_yr * well_cost_per_failure * 0.50) / action_cost_cad) DESC,
# MAGIC                      failure_count DESC
# MAGIC         ) AS greedy_rank
# MAGIC     FROM shrunken_wells
# MAGIC ),
# MAGIC
# MAGIC scenarios AS (
# MAGIC     SELECT * FROM VALUES
# MAGIC         ('Scenario 1: Top 25 Wells (~0.85M Budget)', 25),
# MAGIC         ('Scenario 2: Top 50 Wells (~1.71M Budget)', 50),
# MAGIC         ('Scenario 3: Top 100 Wells (~3.42M Budget)', 100),
# MAGIC         ('Scenario 4: Top 200 Wells (~6.83M Budget)', 200)
# MAGIC     AS t(scenario_name, max_wells)
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     s.scenario_name,
# MAGIC     s.max_wells AS scenario_well_limit,
# MAGIC     w.*
# MAGIC FROM scored_wells w
# MAGIC INNER JOIN scenarios s
# MAGIC     ON w.greedy_rank <= s.max_wells
# MAGIC ORDER BY s.max_wells, w.greedy_rank;