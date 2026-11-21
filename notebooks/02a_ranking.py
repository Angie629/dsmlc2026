# Databricks notebook source
# MAGIC %md
# MAGIC # 02a · Monthly well table and cost score v1 · task s1 (s4 next)
# MAGIC
# MAGIC Build plan task s1. Needs all `01_*` notebooks. Writes:
# MAGIC - `workspace.dsmlc.gold_well_month`: one row per `uwi` × month
# MAGIC - `workspace.dsmlc.gold_well_period`: one row per `uwi` × period (train 2023–24, test Jun 2025 – Oct 2026), with the cost score and ranks
# MAGIC
# MAGIC Out-of-time validation of the ranking (task s4) builds on `gold_well_period`.
# MAGIC
# MAGIC **Cost score**
# MAGIC ```
# MAGIC cost_score = repair_cost + downhole_lost_oil_m3 × 6.2898 × netback_cad_per_bbl
# MAGIC ```
# MAGIC `netback_cad_per_bbl` comes from the widget at the top, prefilled with C$32.50 per boe: Strathcona's Q3 2026 operating earnings (release dated Nov 8, 2026). It is company-wide, not heavy oil only; confirm the definition in the MD&A before quoting it. At this value lost oil is worth about C$9M a year against about C$46M a year of repair spend, so report the two parts separately. Clear the widget to rank on the parts only.

# COMMAND ----------

dbutils.widgets.text("netback_cad_per_bbl", "32.50", "Netback (CAD/bbl)")
dbutils.widgets.text("netback_source", "Strathcona Q3 2026 results (Nov 8, 2026): operating earnings C$32.50/boe, company-wide; confirm definition in MD&A", "Netback source (report, quarter, page)")

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG_SCHEMA = "workspace.dsmlc"
DAILY = f"{CATALOG_SCHEMA}.silver_daily"
DOWNTIME = f"{CATALOG_SCHEMA}.silver_downtime"
JOBS = f"{CATALOG_SCHEMA}.silver_jobs"
FAILURES = f"{CATALOG_SCHEMA}.silver_failures"
MONTH_TARGET = f"{CATALOG_SCHEMA}.gold_well_month"
PERIOD_TARGET = f"{CATALOG_SCHEMA}.gold_well_period"

BBL_PER_M3 = 6.2898
TRAIN = ("2023-01-01", "2024-12-31")
TEST = ("2025-06-01", "2026-10-27")
MIN_PRODUCING_DAYS = 30

_nb = dbutils.widgets.get("netback_cad_per_bbl").strip()
NETBACK = float(_nb) if _nb else None
NETBACK_SOURCE = dbutils.widgets.get("netback_source").strip() or None
if NETBACK is None:
    print("Netback not set: cost_score will be null. Enter it in the widget with its source, then rerun.")
elif not NETBACK_SOURCE:
    raise ValueError("Netback entered without a source. Fill in 'Netback source' (report, quarter, page).")

# COMMAND ----------

def mode_by(df, keys, col):
    counts = df.filter(F.col(col).isNotNull()).groupBy(*keys, col).count()
    w = Window.partitionBy(*keys).orderBy(F.desc("count"), col)
    return counts.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn", "count")


def month_of(col):
    return F.trunc(F.col(col), "month")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Daily → well-month

# COMMAND ----------

daily = spark.table(DAILY).withColumn("month", month_of("production_date"))

prod_month = daily.groupBy("uwi", "month").agg(
    F.count("*").alias("days_in_record"),
    F.sum((F.col("oil_clean_m3") > 0).cast("int")).alias("producing_days"),
    F.sum("runtime_frac").alias("runtime_days"),
    F.sum("oil_clean_m3").alias("oil_m3"),
    F.sum("water_clean_m3").alias("water_m3"),
    F.sum("gas_clean_e3m3").alias("gas_e3m3"),
    F.max((F.col("flow_direction") == "production").cast("int")).cast("boolean").alias("is_producer_month"),
    F.max((F.col("flow_direction") == "injection").cast("int")).cast("boolean").alias("is_injector_month"),
)

well_attrs = (
    mode_by(daily, ["uwi"], "field_operating_area")
    .join(mode_by(daily, ["uwi"], "province"), "uwi", "left")
    .join(
        daily.groupBy("uwi").agg(
            F.avg("surface_latitude").alias("latitude"),
            F.avg("surface_longitude").alias("longitude"),
        ),
        "uwi",
        "left",
    )
)
month_attrs = (
    mode_by(daily, ["uwi", "month"], "facility_name")
    .join(mode_by(daily, ["uwi", "month"], "pcp_pump_make_norm"), ["uwi", "month"], "full")
    .join(mode_by(daily, ["uwi", "month"], "pcp_pump_model"), ["uwi", "month"], "full")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Downtime, repair jobs, failures → well-month
# MAGIC
# MAGIC - Downtime by bucket from `silver_downtime`. Downhole lost oil feeds the score; the other buckets are kept for the dashboard.
# MAGIC - Repair jobs are Rod Truck and Workover (`is_repair`), costed with `field_est_clean`, counted in the month the job starts.
# MAGIC - The failure log is live only from Jun 2025, so failure columns are for the dashboard and the test-window analyses, not the train score. Train starts in 2023 because Rod Truck cost is missing before then (0.1% of jobs costed in 2021, 40% in 2022, 99.5%+ from 2023).

# COMMAND ----------

dt = spark.table(DOWNTIME).withColumn("month", month_of("production_date"))
downtime_month = dt.groupBy("uwi", "month").agg(
    F.sum(F.when(F.col("is_downhole"), F.col("lost_oil_m3"))).alias("downhole_lost_oil_m3"),
    F.sum(F.when(F.col("is_downhole"), F.col("hours_down"))).alias("downhole_hours"),
    F.sum(F.when(F.col("bucket").isin("Surface equipment", "Power", "Facility / pipeline", "Access / weather"), F.col("lost_oil_m3"))).alias("other_unplanned_lost_oil_m3"),
    F.sum(F.when(F.col("bucket") == "Planned", F.col("lost_oil_m3"))).alias("planned_lost_oil_m3"),
    F.sum(F.when(F.col("bucket") == "Economic", F.col("hours_down"))).alias("economic_shutin_hours"),
    F.sum(F.when(F.col("bucket") == "Power", F.col("hours_down"))).alias("power_hours"),
)

jobs = spark.table(JOBS).filter("is_repair").withColumn("month", month_of("start_date"))
jobs_month = jobs.groupBy("uwi", "month").agg(
    F.count("*").alias("repair_jobs"),
    F.sum((F.col("job_category_norm") == "Rod Truck").cast("int")).alias("rod_truck_jobs"),
    F.sum((F.col("job_category_norm") == "Workover").cast("int")).alias("workover_jobs"),
    F.sum("field_est_clean").alias("repair_cost"),
)

fail = spark.table(FAILURES).withColumn("month", month_of("failure_start_date"))
fail_month = fail.groupBy("uwi", "month").agg(
    F.count("*").alias("failures"),
    F.sum((F.col("failure_type_norm") == "Rods").cast("int")).alias("rod_failures"),
    F.sum((F.col("failure_type_norm") == "Pump").cast("int")).alias("pump_failures"),
    F.sum((F.col("failure_type_norm") == "Tubing").cast("int")).alias("tubing_failures"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write `gold_well_month`
# MAGIC
# MAGIC The grid is every well-month present in the daily data. Jobs and failures on wells missing from daily (69 job wells, 1 failure well in profiling) are counted and dropped.

# COMMAND ----------

counts_cols = [
    "downhole_lost_oil_m3", "downhole_hours", "other_unplanned_lost_oil_m3", "planned_lost_oil_m3",
    "economic_shutin_hours", "power_hours", "repair_jobs", "rod_truck_jobs", "workover_jobs",
    "repair_cost", "failures", "rod_failures", "pump_failures", "tubing_failures",
]
gwm = (
    prod_month.join(well_attrs, "uwi", "left")
    .join(month_attrs, ["uwi", "month"], "left")
    .join(downtime_month, ["uwi", "month"], "left")
    .join(jobs_month, ["uwi", "month"], "left")
    .join(fail_month, ["uwi", "month"], "left")
    .fillna(0, subset=counts_cols)
    .withColumn("water_cut", F.when(F.col("oil_m3") + F.col("water_m3") > 0, F.col("water_m3") / (F.col("oil_m3") + F.col("water_m3"))))
    .withColumn(
        "period",
        F.when(F.col("month").between(*TRAIN), "train").when(F.col("month").between(*TEST), "test"),
    )
)
gwm.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(MONTH_TARGET)
gwm = spark.table(MONTH_TARGET)

dropped_jobs = jobs_month.join(prod_month, ["uwi", "month"], "left_anti").agg(F.sum("repair_jobs"), F.sum("repair_cost")).first()
dropped_fail = fail_month.join(prod_month, ["uwi", "month"], "left_anti").agg(F.sum("failures")).first()[0]
print(f"Repair jobs outside the daily grid: {dropped_jobs[0] or 0:,} jobs, ${dropped_jobs[1] or 0:,.0f}")
print(f"Failures outside the daily grid: {dropped_fail or 0:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Well-period totals and eligibility
# MAGIC
# MAGIC Eligible producer: at least 30 days with oil in **both** train and test. Wells producing only in test are "new, unranked" and reported separately.

# COMMAND ----------

period = (
    gwm.filter(F.col("period").isNotNull())
    .groupBy("uwi", "period")
    .agg(
        F.first("field_operating_area").alias("field_operating_area"),
        F.first("latitude").alias("latitude"),
        F.first("longitude").alias("longitude"),
        F.countDistinct("month").alias("months"),
        F.sum("producing_days").alias("producing_days"),
        F.sum("runtime_days").alias("runtime_days"),
        F.sum("oil_m3").alias("oil_m3"),
        F.sum("water_m3").alias("water_m3"),
        F.sum("downhole_lost_oil_m3").alias("downhole_lost_oil_m3"),
        F.sum("downhole_hours").alias("downhole_hours"),
        F.sum("other_unplanned_lost_oil_m3").alias("other_unplanned_lost_oil_m3"),
        F.sum("repair_jobs").alias("repair_jobs"),
        F.sum("rod_truck_jobs").alias("rod_truck_jobs"),
        F.sum("workover_jobs").alias("workover_jobs"),
        F.sum("repair_cost").alias("repair_cost"),
        F.sum("failures").alias("failures"),
    )
)

producing = period.filter(F.col("producing_days") >= MIN_PRODUCING_DAYS)
both = producing.groupBy("uwi").agg(F.countDistinct("period").alias("n")).filter("n = 2").select("uwi", F.lit(True).alias("eligible"))
test_only = (
    producing.groupBy("uwi").agg(F.collect_set("period").alias("ps"))
    .filter(F.array_contains("ps", "test") & ~F.array_contains("ps", "train"))
    .select("uwi", F.lit(True).alias("new_unranked"))
)
period = (
    period.join(both, "uwi", "left").join(test_only, "uwi", "left")
    .fillna(False, subset=["eligible", "new_unranked"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cost score and ranks
# MAGIC
# MAGIC Ranks are within each period, over eligible wells only (1 = most costly).
# MAGIC - `rank_cost`: cost score (null until netback is set)
# MAGIC - `rank_repair`, `rank_lost_oil`: each part alone
# MAGIC - `rank_size`: oil volume, the size baseline the score must beat in s4
# MAGIC - `cost_per_producing_day`: the fallback score if the size baseline wins (risk 3)

# COMMAND ----------

lost_value = F.col("downhole_lost_oil_m3") * BBL_PER_M3 * F.lit(NETBACK)
period = (
    period.withColumn("netback_cad_per_bbl", F.lit(NETBACK).cast("double"))
    .withColumn("netback_source", F.lit(NETBACK_SOURCE).cast("string"))
    .withColumn("lost_oil_value", lost_value if NETBACK is not None else F.lit(None).cast("double"))
    .withColumn("cost_score", F.col("repair_cost") + F.col("lost_oil_value"))
    .withColumn("cost_per_producing_day", F.when(F.col("producing_days") > 0, F.col("cost_score") / F.col("producing_days")))
)


def ranked(df, col, name):
    w = Window.partitionBy("period").orderBy(F.desc_nulls_last(col), "uwi")
    pct = Window.partitionBy("period").orderBy(F.desc_nulls_last(col), "uwi")
    return (
        df.withColumn(name, F.when(F.col("eligible") & F.col(col).isNotNull(), F.row_number().over(w)))
        .withColumn(f"{name}_pct", F.when(F.col(name).isNotNull(), F.percent_rank().over(pct)))
    )


elig = period.filter("eligible")
rest = period.filter("NOT eligible")
for col, name in [
    ("cost_score", "rank_cost"),
    ("repair_cost", "rank_repair"),
    ("downhole_lost_oil_m3", "rank_lost_oil"),
    ("oil_m3", "rank_size"),
    ("cost_per_producing_day", "rank_cost_per_day"),
]:
    elig = ranked(elig, col, name)
    rest = rest.withColumn(name, F.lit(None).cast("int")).withColumn(f"{name}_pct", F.lit(None).cast("double"))

gwp = elig.unionByName(rest)
gwp.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(PERIOD_TARGET)
gwp = spark.table(PERIOD_TARGET)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Train-window ranking v1

# COMMAND ----------

rank_col = "rank_cost" if NETBACK is not None else "rank_repair"
print(f"Ranking by {rank_col}")
display(
    gwp.filter("period = 'train' AND eligible")
    .orderBy(rank_col)
    .select(
        rank_col, "uwi", "field_operating_area", "cost_score", "repair_cost", "repair_jobs",
        "downhole_lost_oil_m3", "oil_m3", "producing_days", "rank_size",
    )
    .limit(50)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Checks

# COMMAND ----------

all_repair = spark.table(JOBS).filter("is_repair").agg(F.sum("field_est_clean")).first()[0]
gold_repair = gwm.agg(F.sum("repair_cost")).first()[0]
all_downhole = spark.table(DOWNTIME).filter("is_downhole").agg(F.sum("lost_oil_m3")).first()[0]
gold_downhole = gwm.agg(F.sum("downhole_lost_oil_m3")).first()[0]

summary = gwp.groupBy("period").agg(
    F.sum(F.col("eligible").cast("int")).alias("eligible_wells"),
    F.sum(F.col("new_unranked").cast("int")).alias("new_unranked_wells"),
    F.sum(F.when(F.col("eligible"), F.col("repair_cost"))).alias("eligible_repair_cost"),
    F.sum(F.when(F.col("eligible"), F.col("downhole_lost_oil_m3"))).alias("eligible_downhole_lost_oil_m3"),
)
display(summary)

checks = {
    "one row per well-month": gwm.groupBy("uwi", "month").count().filter("count > 1").count() == 0,
    "one row per well-period": gwp.groupBy("uwi", "period").count().filter("count > 1").count() == 0,
    "repair $ in gold = silver − jobs outside daily grid": abs(gold_repair + (dropped_jobs[1] or 0) - all_repair) < 1,
    "downhole lost oil in gold = silver_downtime": abs(gold_downhole - all_downhole) < 1,
    "ranks only on eligible wells": gwp.filter("NOT eligible AND rank_repair IS NOT NULL").count() == 0,
}
for name, ok in checks.items():
    print(("PASS " if ok else "FAIL ") + name)
print(f"Repair $ all silver jobs: {all_repair:,.0f} (profiling: ~212.4M) · in gold: {gold_repair:,.0f}")
print(f"Downhole lost oil: {gold_downhole:,.0f} m³")
assert all(checks.values()), "One or more checks failed"
