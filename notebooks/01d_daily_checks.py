# Databricks notebook source
# MAGIC %md
# MAGIC # 01d · Daily-file checks · task n1
# MAGIC
# MAGIC Build plan task n1. Run after `01c_downtime_features`. Writes `workspace.dsmlc.gold_failure_downtime` (one row per failure) for payback (s5); everything else is read-only.
# MAGIC
# MAGIC 1. Where lost oil goes, by downtime bucket and year (dashboard page 1)
# MAGIC 2. Is power/grid downtime seasonal? (weather go/no-go, c3)
# MAGIC 3. Per failure: when the well stopped, how long it was down, and downhole lost oil from the stop day
# MAGIC
# MAGIC First run on the raw files (Nov 21, pandas): Downhole is the largest bucket (219,627 m³, 51% of non-economic lost oil); power downtime has spikes but no steady winter pattern; rods are down a median 1 day, pump 4, tubing 5; counting from the stop day adds 51% more lost oil than counting from day −1.

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import functions as F

CATALOG_SCHEMA = "workspace.dsmlc"
DAILY = f"{CATALOG_SCHEMA}.silver_daily"
DOWNTIME = f"{CATALOG_SCHEMA}.silver_downtime"
FAILURES = f"{CATALOG_SCHEMA}.silver_failures"
TARGET = f"{CATALOG_SCHEMA}.gold_failure_downtime"

FAIL_START = "2025-06-01"
DOWNHOLE_TYPES = ["Rods", "Pump", "Tubing"]
LOOK_BACK = 60
LOOK_AHEAD = 60
LOST_OIL_DAYS_AFTER = 14

dt = spark.table(DOWNTIME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Lost oil by bucket

# COMMAND ----------

by_bucket = (
    dt.groupBy("bucket")
    .agg(F.sum("lost_oil_m3").alias("lost_oil_m3"), F.sum("hours_down").alias("hours_down"), F.count("*").alias("well_days"))
    .toPandas().sort_values("lost_oil_m3", ascending=False)
)
by_bucket["lost_share"] = by_bucket.lost_oil_m3 / by_bucket.lost_oil_m3.sum()
reliability = by_bucket[~by_bucket.bucket.isin(["Economic", "Injection"])]
by_bucket["share_of_non_economic"] = by_bucket.lost_oil_m3 / reliability.lost_oil_m3.sum()
display(spark.createDataFrame(by_bucket.round(4)))

display(
    dt.groupBy(F.year("production_date").alias("year"))
    .pivot("bucket").agg(F.round(F.sum("lost_oil_m3")))
    .orderBy("year")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Power/grid downtime by calendar month
# MAGIC
# MAGIC Hours per 1,000 producing well-days, so months with more wells don't look worse. Look for a repeatable winter peak (same months high every year), not one-off spikes.

# COMMAND ----------

prod_days = (
    spark.table(DAILY).filter(F.col("flow_direction") == "production")
    .groupBy(F.trunc("production_date", "month").alias("month")).count().withColumnRenamed("count", "well_days")
)
power = (
    dt.filter((F.col("bucket") == "Power") & (F.col("flow_direction") == "production"))
    .groupBy(F.trunc("production_date", "month").alias("month")).agg(F.sum("hours_down").alias("power_hours"))
)
mon = prod_days.join(power, "month", "left").fillna(0).toPandas().sort_values("month")
mon["per_1000"] = mon.power_hours / mon.well_days * 1000
mon["moy"] = pd.to_datetime(mon.month).dt.month
mon["year"] = pd.to_datetime(mon.month).dt.year
display(spark.createDataFrame(mon.groupby("moy").per_1000.agg(["mean", "median", "min", "max"]).round(1).reset_index()))
display(spark.createDataFrame(mon.pivot(index="moy", columns="year", values="per_1000").round(1).reset_index().rename(columns=str)))

fig, ax = plt.subplots(figsize=(12, 3.5))
ax.plot(pd.to_datetime(mon.month), mon.per_1000)
ax.set_title("Power/grid downtime hours per 1,000 producing well-days")
fig.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC **Weather go/no-go (c3):** go only if the same months are high in most years. Spikes in single months point to individual outage events, not a weather pattern.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Downtime per failure, from the stop day
# MAGIC
# MAGIC `failure_start_date` is logged after the well stops (median 2 days). For each rod/pump/tubing failure from Jun 2025:
# MAGIC - **stop day**: first day of the downtime streak that covers the logged day or the day before; otherwise the logged day
# MAGIC - **restart day**: first day after the stop with ≥ 1 h on production
# MAGIC - **down_days** = restart − stop
# MAGIC - **downhole_lost_oil_m3**: downhole-bucket lost oil from the stop day to 14 days after the logged date (the payback window)

# COMMAND ----------

fail = (
    spark.table(FAILURES)
    .filter((F.col("failure_start_date") >= FAIL_START) & F.col("failure_type_norm").isin(DOWNHOLE_TYPES))
    .select("failure_id", "uwi", F.col("failure_start_date").alias("fd"), "failure_type_norm", "field_operating_area", "job_id", "linked_job_cost")
)
days = (
    spark.table(DAILY).filter(F.col("flow_direction") == "production")
    .select("uwi", "production_date", "hours_on_prod")
    .join(dt.select("uwi", "production_date", "bucket", "lost_oil_m3"), ["uwi", "production_date"], "left")
)
win = (
    fail.select("failure_id", "uwi", "fd").join(days, "uwi")
    .withColumn("offset", F.datediff("production_date", "fd"))
    .filter(F.col("offset").between(-LOOK_BACK, LOOK_AHEAD))
    .select("failure_id", "offset", "hours_on_prod", "bucket", "lost_oil_m3")
    .toPandas()
)


print(f"Window rows: {len(win):,} for {win.failure_id.nunique():,} failures")


def downtime_per_failure(win):
    """Stop day, restart day and lost oil for every failure at once (no per-failure loop)."""
    ids = win.failure_id.unique()
    grid = pd.MultiIndex.from_product([ids, range(-LOOK_BACK, LOOK_AHEAD + 1)], names=["failure_id", "offset"])
    g = win.set_index(["failure_id", "offset"]).reindex(grid).reset_index()
    g["down"] = g.hours_on_prod.fillna(0) < 1
    g["streak"] = (~g.down).groupby(g.failure_id).cumsum()
    g["streak_start"] = g[g.down].groupby(["failure_id", "streak"]).offset.transform("min")

    at = g[g.offset.isin([0, -1])].pivot(index="failure_id", columns="offset", values=["down", "streak_start"])
    down0, down1 = at[("down", 0)].astype(bool), at[("down", -1)].astype(bool)
    stop = np.where(down0, at[("streak_start", 0)], np.where(down1, at[("streak_start", -1)], 0))
    out = pd.DataFrame({"failure_id": at.index, "stop_offset": stop.astype(int)})

    g = g.merge(out, on="failure_id")
    running = g[(g.offset > np.maximum(g.stop_offset, -1)) & (g.hours_on_prod.fillna(0) >= 1)]
    out = out.merge(running.groupby("failure_id").offset.min().rename("restart_offset"), on="failure_id", how="left")
    out["down_days"] = out.restart_offset - out.stop_offset

    new_w = g[(g.offset >= g.stop_offset) & (g.offset <= LOST_OIL_DAYS_AFTER)]
    old_w = g[g.offset.between(-1, LOST_OIL_DAYS_AFTER) & (g.bucket == "Downhole")]
    out = out.merge(new_w[new_w.bucket == "Downhole"].groupby("failure_id").lost_oil_m3.sum().rename("downhole_lost_oil_m3"), on="failure_id", how="left")
    out = out.merge(new_w.groupby("failure_id").lost_oil_m3.sum().rename("all_lost_oil_m3"), on="failure_id", how="left")
    out = out.merge(old_w.groupby("failure_id").lost_oil_m3.sum().rename("downhole_lost_oil_old_window_m3"), on="failure_id", how="left")
    return out.fillna({"downhole_lost_oil_m3": 0.0, "all_lost_oil_m3": 0.0, "downhole_lost_oil_old_window_m3": 0.0})


pf = downtime_per_failure(win)
fd = fail.toPandas().merge(pf, on="failure_id", how="left")
fd["fd"] = pd.to_datetime(fd.fd)
fd["stop_date"] = fd.fd + pd.to_timedelta(fd.stop_offset.fillna(0), unit="D")
fd["restart_date"] = fd.fd + pd.to_timedelta(fd.restart_offset, unit="D")
fd["stopped_before_log"] = fd.stop_offset < 0
fd = fd.rename(columns={"fd": "failure_start_date"}).astype({
    "stop_offset": "float", "restart_offset": "float", "down_days": "float",
    "linked_job_cost": "float", "stopped_before_log": "bool",
})
print(f"Computed {len(fd):,} failures")

# COMMAND ----------

spark.createDataFrame(fd).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET)
print(f"Wrote {TARGET}")

# COMMAND ----------

print(f"Failures: {len(fd):,}; with daily data: {fd.stop_offset.notna().sum():,}; stopped before the logged date: {fd.stopped_before_log.mean():.1%}")
display(spark.createDataFrame(
    fd.groupby("failure_type_norm")[["down_days", "downhole_lost_oil_m3"]]
    .describe(percentiles=[.25, .5, .75, .9]).round(2).reset_index()
    .pipe(lambda d: d.set_axis(["_".join(c).strip("_") for c in d.columns], axis=1))
))
print(f"Downhole lost oil, stop-day window: {fd.downhole_lost_oil_m3.sum():,.0f} m³; old day −1 window: {fd.downhole_lost_oil_old_window_m3.sum():,.0f} m³")

checks = {
    "one row per failure": spark.table(TARGET).groupBy("failure_id").count().filter("count > 1").count() == 0,
    "stop day never after logged day": (fd.stop_offset.fillna(0) <= 0).all(),
    "stop-day window >= old window": fd.downhole_lost_oil_m3.sum() >= fd.downhole_lost_oil_old_window_m3.sum(),
}
for name, ok in checks.items():
    print(("PASS " if ok else "FAIL ") + name)
assert all(checks.values()), "One or more checks failed"
