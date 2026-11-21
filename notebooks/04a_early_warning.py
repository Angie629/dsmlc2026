# Databricks notebook source
# MAGIC %md
# MAGIC # 04a · Early warning, part 1: what sensors show before failures · task s3 (s6, s9 next)
# MAGIC
# MAGIC Build plan task s3. Needs `01a_clean_daily` and `01b_clean_jobs_failures`. Read-only: writes no tables.
# MAGIC
# MAGIC Questions:
# MAGIC 1. How many Jun–Dec 2025 rod/pump failures (the tuning window; the log is live from Jun 2025) have enough sensor data before them?
# MAGIC 2. Are torque, current and RPM real daily readings, or repeated values?
# MAGIC 3. Is `failure_start_date` the day the well stopped?
# MAGIC 4. Which signals differ before failures compared with normal well-days (controls)?
# MAGIC
# MAGIC First run on the raw files (Nov 21, pandas) found: torque/current/RPM repeat the previous day's value 90–96% of the time; 66% of failed wells were already down on day 0 or −1; no torque, current or efficiency drift before failures; but partial-run and down days in the week before the stop are 2–4× more common than in controls. Charts from that run: `docs/figures/prefailure_*.png`.

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG_SCHEMA = "workspace.dsmlc"
DAILY = f"{CATALOG_SCHEMA}.silver_daily"
FAILURES = f"{CATALOG_SCHEMA}.silver_failures"

TUNE = ("2025-06-01", "2025-12-31")
FAIL_TYPES = ["Rods", "Pump"]
N_CONTROLS = 1500
N_SAMPLE = 50
SEED = 42
PRE = (-37, -8)
SIGNALS = {
    "pcp_pump_rod_torque_ftlb": "torque",
    "pcp_pump_electric_current_a": "current",
    "pump_efficiency_pct": "efficiency",
    "pcp_pump_rpm": "rpm",
}

# COMMAND ----------

d = (
    spark.table(DAILY)
    .filter((F.col("flow_direction") == "production") & (F.col("production_date") >= "2025-02-01"))
    .select(
        "uwi", "production_date", "field_operating_area", "hours_on_prod", "is_rate_day",
        *[F.when(F.col("is_rate_day"), F.col(c)).alias(a) for c, a in SIGNALS.items()],
        F.when(
            F.col("is_rate_day"),
            (F.coalesce("oil_clean_m3", F.lit(0)) + F.coalesce("water_clean_m3", F.lit(0))) / (F.col("hours_on_prod") / 24),
        ).alias("fluid_rate"),
    )
)
fail = (
    spark.table(FAILURES)
    .filter(F.col("failure_start_date").between(*TUNE) & F.col("failure_type_norm").isin(FAIL_TYPES))
    .select("failure_id", "uwi", F.col("failure_start_date").alias("anchor"), "failure_type_norm", "failure_detail", "cause")
)
print(f"Rod/pump failures {TUNE[0]} to {TUNE[1]}: {fail.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Are the pump readings daily measurements?
# MAGIC
# MAGIC Share of rate-day readings identical to the same well's previous reading, and distinct values per well since Feb 2025. A real daily sensor rarely repeats exactly.

# COMMAND ----------

w = Window.partitionBy("uwi").orderBy("production_date")
rows = []
for a in list(SIGNALS.values()) + ["fluid_rate"]:
    x = d.filter(F.col(a).isNotNull()).withColumn("prev", F.lag(a).over(w))
    r = x.agg(F.avg((F.col(a) == F.col("prev")).cast("int")).alias("repeat_share")).first()["repeat_share"]
    n = x.groupBy("uwi").agg(F.countDistinct(a).alias("n")).agg(F.expr("percentile_approx(n, 0.5)")).first()[0]
    rows.append((a, r, n))
display(spark.createDataFrame(pd.DataFrame(rows, columns=["signal", "repeat_share", "median_distinct_values_per_well"])))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Windows around each failure and around control well-days
# MAGIC
# MAGIC Controls: random Jun–Dec 2025 well-days where the well ran ≥ 23 h and had no failure from 10 days before to 60 days after.

# COMMAND ----------

def windows(anchors, before, after):
    a = anchors.select("event_id", "uwi", "anchor")
    return (
        a.join(d, "uwi")
        .withColumn("offset", F.datediff("production_date", "anchor"))
        .filter(F.col("offset").between(before, after))
        .drop("production_date")
        .toPandas()
    )


fw = windows(fail.withColumnRenamed("failure_id", "event_id"), -100, 10)

all_fail = spark.table(FAILURES).select("uwi", F.col("failure_start_date").alias("fd"))
cand = (
    d.filter(F.col("production_date").between(*TUNE) & (F.col("hours_on_prod") >= 23))
    .select("uwi", F.col("production_date").alias("anchor"))
)
near = cand.join(all_fail, "uwi").filter(F.datediff("fd", "anchor").between(-10, 60)).select("uwi", "anchor").distinct()
ctrl = (
    cand.join(near, ["uwi", "anchor"], "left_anti")
    .orderBy(F.rand(SEED)).limit(N_CONTROLS)
    .withColumn("event_id", F.concat_ws("|", "uwi", F.col("anchor").cast("string")))
)
cw = windows(ctrl, -45, 0)
print(f"Failure window rows: {len(fw):,}; control window rows: {len(cw):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Coverage before failures
# MAGIC
# MAGIC A failure is "covered" for a signal if it has ≥ 20 rate-day readings in days −30 to −1.

# COMMAND ----------

pre = fw[fw.offset.between(-30, -1)]
cov = pre.groupby("event_id")[list(SIGNALS.values())].count()
cov = cov.reindex(fail.select("failure_id").toPandas().failure_id).fillna(0)
area = fw.groupby("event_id").field_operating_area.agg(lambda s: s.mode().iloc[0] if s.notna().any() else None)
display(spark.createDataFrame((cov >= 20).mean().rename("covered_share").reset_index().rename(columns={"index": "signal"})))
display(spark.createDataFrame(
    (cov >= 20).join(area).groupby("field_operating_area")[list(SIGNALS.values())].mean().round(2)
    .join(cov.join(area).groupby("field_operating_area").size().rename("failures")).sort_values("failures", ascending=False).reset_index()
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. When did the well actually stop?
# MAGIC
# MAGIC If the well was down (< 1 h) on the failure day or the day before, the stop day is the first day of that downtime streak. Otherwise it is the failure day.

# COMMAND ----------

def stop_offset(g):
    s = g.set_index("offset").hours_on_prod.reindex(range(-60, 1)).fillna(0)
    down = s < 1
    if not (down.loc[0] or down.loc[-1]):
        return 0
    k = 0 if down.loc[0] else -1
    while k - 1 >= -60 and down.loc[k - 1]:
        k -= 1
    return k


stops = fw[fw.offset <= 0].groupby("event_id").apply(stop_offset).rename("stop_offset")
print(f"Stopped before the logged failure date: {(stops < 0).mean():.1%}")
print("Days between stop and logged failure date:")
display(spark.createDataFrame((-stops).describe(percentiles=[.25, .5, .75, .9]).round(1).reset_index()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Signals before the stop: failures vs controls
# MAGIC
# MAGIC Re-aligned so day 0 = stop day. Sensor values are divided by the well's own baseline (days −37 to −8); efficiency is shown as a ratio too.
# MAGIC Window tested: days −8 to −2. Day −1 is left out because it is usually the partial day on which the failure happened.

# COMMAND ----------

fr = fw.merge(stops, left_on="event_id", right_index=True)
fr["offset"] = fr["offset"] - fr["stop_offset"]
fr = fr[fr.offset.between(-45, 0)]


def prep(df):
    df = df.copy()
    df["partial"] = ((df.hours_on_prod > 0) & (df.hours_on_prod < 23)).astype(float)
    df["down"] = (df.hours_on_prod < 1).astype(float)
    return df


fr, cr = prep(fr), prep(cw)
REL = ["torque", "current", "efficiency", "fluid_rate"]


def relative(df):
    base = df[df.offset.between(*PRE)].groupby("event_id")[REL].agg(["mean", "count"])
    out = df.set_index("event_id")
    for s in REL:
        ok = (base[(s, "count")] >= 15) & (base[(s, "mean")] > 0)
        b = base[(s, "mean")].where(ok)
        out[f"{s}_rel"] = out[s] / out.index.map(b)
    return out.reset_index()


fr, cr = relative(fr), relative(cr)
win = lambda df: df[df.offset.between(-8, -2)]
rows = []
for s in REL:
    fa, ca = win(fr).groupby("event_id")[f"{s}_rel"].mean(), win(cr).groupby("event_id")[f"{s}_rel"].mean()
    for t in [1.2, 0.9, 0.8, 0.7]:
        f_share = (fa >= t).mean() if t > 1 else (fa <= t).mean()
        c_share = (ca >= t).mean() if t > 1 else (ca <= t).mean()
        rows.append((s, (">= " if t > 1 else "<= ") + f"{t:.0%} of baseline", f_share, c_share))
for s in ["partial", "down"]:
    fa, ca = win(fr).groupby("event_id")[s].sum(), win(cr).groupby("event_id")[s].sum()
    for t in [1, 2, 3]:
        rows.append((s, f">= {t} day(s) in days -8..-2", (fa >= t).mean(), (ca >= t).mean()))
cmp = pd.DataFrame(rows, columns=["signal", "condition", "failures_share", "controls_share"])
cmp["ratio"] = cmp.failures_share / cmp.controls_share
display(spark.createDataFrame(cmp.round(3)))

# COMMAND ----------

fig, axes = plt.subplots(1, 4, figsize=(18, 4))
for ax, (s, title, agg) in zip(axes, [
    ("efficiency_rel", "Pump efficiency / baseline (median)", "median"),
    ("torque_rel", "Rod torque / baseline (median)", "median"),
    ("partial", "Share with a partial-run day", "mean"),
    ("down", "Share down that day", "mean"),
]):
    for lab, df, col in [("Failures", fr, "C3"), ("Controls", cr, "C0")]:
        y = df[df.offset < 0].groupby("offset")[s].agg(agg)
        ax.plot(y.index, y.values, color=col, label=f"{lab} (n={df.event_id.nunique()})")
    ax.set_title(title)
    ax.set_xlabel("Days before the well stopped")
    ax.legend(fontsize=8)
fig.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 50 sampled failures
# MAGIC
# MAGIC Torque-covered failures, sampled in proportion to each area's covered failures. Solid: torque and current ÷ baseline. Dotted (right axis): efficiency − baseline. Dashed line: logged failure date.
# MAGIC Look for: steady torque climb, efficiency slide, current spike, or nothing. Flat steps mean a reading was entered once and repeated.

# COMMAND ----------

covered = cov[cov.torque >= 20].join(area)
weights = covered.groupby("field_operating_area").size()
take = (weights / weights.sum() * N_SAMPLE).round().clip(lower=1).astype(int)
sample_ids = pd.concat([
    covered[covered.field_operating_area == a].sample(min(n, (covered.field_operating_area == a).sum()), random_state=SEED)
    for a, n in take.items()
]).index[:N_SAMPLE]
meta = fail.toPandas().set_index("failure_id")

fig, axes = plt.subplots(10, 5, figsize=(20, 26), sharex=True)
for ax, eid in zip(axes.flat, sample_ids):
    g = fw[(fw.event_id == eid) & fw.offset.between(-45, 5)].set_index("offset").sort_index()
    base = g.loc[PRE[0]:PRE[1]]
    for s, col in [("torque", "C3"), ("current", "C1")]:
        if base[s].count() >= 15 and base[s].mean() > 0:
            ax.plot(g.index, g[s] / base[s].mean(), color=col, lw=1)
    ax2 = ax.twinx()
    if base.efficiency.count() >= 15:
        ax2.plot(g.index, g.efficiency - base.efficiency.mean(), color="C0", lw=1, ls=":")
    ax2.set_ylim(-0.6, 0.6)
    ax.set_ylim(0, 2)
    ax.axvline(0, color="k", lw=.7, ls="--")
    ax.axhline(1, color="grey", lw=.5)
    m = meta.loc[eid]
    ax.set_title(f"{covered.loc[eid, 'field_operating_area']} | {m.failure_type_norm} | {str(m.failure_detail)[:28]}", fontsize=7)
fig.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Notes for threshold rules (task s6)
# MAGIC
# MAGIC Fill in after running:
# MAGIC - Signals with a clear failure/control ratio in section 5:
# MAGIC - Signals to drop (repeated values, no difference):
# MAGIC - Coverage limits (areas where rules cannot run):
# MAGIC - Alignment: tune and test rules against the **stop day**, not `failure_start_date`.
