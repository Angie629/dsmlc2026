# Databricks notebook source
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
