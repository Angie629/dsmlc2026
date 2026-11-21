# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Repeat failures and recency tiers
# MAGIC
# MAGIC Build plan task n2. Needs `01_clean_jobs_failures`. Writes:
# MAGIC - `workspace.dsmlc.gold_failure_gap`: one row per failure with days to the next failure on the same well
# MAGIC - `workspace.dsmlc.gold_recency_tier`: one row per well × snapshot with failures in the prior 90 days, tier, and whether it failed in the next 90 days. The last snapshot is the data end date (outcome unknown) and feeds the weekly work list.
# MAGIC
# MAGIC First run on the raw files (Nov 21, pandas): re-failure@90 57.1% (rods 65.4%, pump 30.6%, tubing 29.4%); Rod Truck jobs repeat within 90 days 57–65% in every year 2021–2026; tiers 0/1/2/3+ fail again within 90 days 26% / 40% / 61% / 70%; flag 2+: precision 64.0%, recall 33.5%.

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import functions as F

CATALOG_SCHEMA = "workspace.dsmlc"
FAILURES = f"{CATALOG_SCHEMA}.silver_failures"
JOBS = f"{CATALOG_SCHEMA}.silver_jobs"
GAP_TARGET = f"{CATALOG_SCHEMA}.gold_failure_gap"
TIER_TARGET = f"{CATALOG_SCHEMA}.gold_recency_tier"

LOG_START = pd.Timestamp("2025-06-01")
DATA_END = pd.Timestamp("2026-10-27")
HORIZONS = (30, 90)
TIER_DAYS = 90
SNAP_START, SNAP_END, SNAP_STEP = "2025-09-01", "2026-07-26", "14D"
MIN_AREA_FAILURES = 50

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Days to the next failure
# MAGIC
# MAGIC The next failure can be any type. A failure counts toward re-failure@h only if it has h days of follow-up before the data ends.

# COMMAND ----------

f = (
    spark.table(FAILURES)
    .select("failure_id", "uwi", "failure_start_date", "failure_type_norm", "field_operating_area", "cause")
    .toPandas()
)
f["failure_start_date"] = pd.to_datetime(f.failure_start_date)
f = f.sort_values(["uwi", "failure_start_date", "failure_id"]).reset_index(drop=True)
f["next_failure_date"] = f.groupby("uwi").failure_start_date.shift(-1)
f["days_to_next"] = (f.next_failure_date - f.failure_start_date).dt.days
f["in_log_window"] = f.failure_start_date >= LOG_START
for h in HORIZONS:
    f[f"has_{h}d_followup"] = f.failure_start_date <= DATA_END - pd.Timedelta(days=h)
    f[f"refail_{h}"] = (f.days_to_next <= h).where(f[f"has_{h}d_followup"])

spark.createDataFrame(f.astype({f"refail_{h}": "float" for h in HORIZONS})).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GAP_TARGET)

# COMMAND ----------

w = f[f.in_log_window]


def refail(df, h):
    x = df[df[f"has_{h}d_followup"]]
    return pd.Series({f"failures_{h}d_followup": len(x), f"refail_{h}": x[f"refail_{h}"].mean()})


overall = pd.concat([refail(w, h) for h in HORIZONS])
print(overall.round(3).to_string())
by_type = w.groupby("failure_type_norm").apply(lambda g: pd.concat([refail(g, h) for h in HORIZONS])).round(3)
by_type["median_days_to_next"] = w.groupby("failure_type_norm").days_to_next.median()
display(spark.createDataFrame(by_type.reset_index()))
by_area = w.groupby("field_operating_area").apply(lambda g: refail(g, 90))
display(spark.createDataFrame(by_area[by_area.failures_90d_followup >= MIN_AREA_FAILURES].sort_values("failures_90d_followup", ascending=False).round(3).reset_index()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Is it a logging artefact? Rod Truck jobs by year
# MAGIC
# MAGIC The failure log only starts in Jun 2025. Rod Truck jobs exist for every year, so the same 90-day repeat measure on them shows whether wells have always re-failed this often.

# COMMAND ----------

rt = (
    spark.table(JOBS).filter(F.col("job_category_norm") == "Rod Truck")
    .select("uwi", "start_date").toPandas()
)
rt["start_date"] = pd.to_datetime(rt.start_date)
rt = rt.sort_values(["uwi", "start_date"])
rt["days_to_next"] = (rt.groupby("uwi").start_date.shift(-1) - rt.start_date).dt.days
rt = rt[rt.start_date <= DATA_END - pd.Timedelta(days=90)]
by_year = rt.groupby(rt.start_date.dt.year).agg(jobs=("uwi", "size"), repeat_90=("days_to_next", lambda s: (s <= 90).mean()))
display(spark.createDataFrame(by_year.round(3).reset_index().rename(columns={"start_date": "year"})))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Recency tiers (the watchlist baseline)
# MAGIC
# MAGIC Every 14 days from 2025-09-01 to 2026-07-26 (so each snapshot has 90 days of history in the log and 90 days of follow-up), for every well with at least one logged failure before the snapshot:
# MAGIC - `failures_prior_90`: failures in the 90 days before the snapshot → tier 0, 1, 2, 3+
# MAGIC - `failed_next_90`: any failure in the 90 days from the snapshot
# MAGIC
# MAGIC Only wells with a logged failure are in the universe. Say so wherever the precision is quoted.

# COMMAND ----------

fl = f[f.in_log_window]
snaps = list(pd.date_range(SNAP_START, SNAP_END, freq=SNAP_STEP)) + [DATA_END]
rows = []
for sd in snaps:
    hist = fl[fl.failure_start_date < sd]
    if hist.empty:
        continue
    prior = hist[hist.failure_start_date >= sd - pd.Timedelta(days=TIER_DAYS)].groupby("uwi").size()
    last = hist.groupby("uwi").failure_start_date.max()
    area = hist.groupby("uwi").field_operating_area.agg(lambda s: s.mode().iloc[0])
    t = pd.DataFrame({"uwi": hist.uwi.unique()})
    t["snapshot_date"] = sd
    t["field_operating_area"] = t.uwi.map(area)
    t["failures_prior_90"] = t.uwi.map(prior).fillna(0).astype(int)
    t["last_failure_date"] = t.uwi.map(last)
    if sd + pd.Timedelta(days=TIER_DAYS) <= DATA_END:
        nxt = fl[(fl.failure_start_date >= sd) & (fl.failure_start_date < sd + pd.Timedelta(days=TIER_DAYS))].groupby("uwi").size()
        t["failed_next_90"] = t.uwi.map(nxt).fillna(0).gt(0).astype(float)
    else:
        t["failed_next_90"] = float("nan")
    rows.append(t)
tiers = pd.concat(rows, ignore_index=True)
tiers["tier"] = tiers.failures_prior_90.clip(upper=3).map({0: "0", 1: "1", 2: "2", 3: "3+"})
tiers["is_current"] = tiers.snapshot_date == DATA_END
spark.createDataFrame(tiers).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TIER_TARGET)

# COMMAND ----------

ev = tiers[~tiers.is_current]
print(f"Evaluated snapshots: {ev.snapshot_date.nunique()}, wells: {ev.uwi.nunique():,}, well-snapshots: {len(ev):,}; base rate {ev.failed_next_90.mean():.1%}")
display(spark.createDataFrame(ev.groupby("tier").failed_next_90.agg(["mean", "size"]).round(3).reset_index()))

rows = []
for k in (1, 2, 3):
    flag = ev.failures_prior_90 >= k
    rows.append({
        "flag": f"{k}+ failures in prior 90 days",
        "share_flagged": flag.mean(),
        "precision": ev[flag].failed_next_90.mean(),
        "recall": flag[ev.failed_next_90 == 1].mean(),
    })
display(spark.createDataFrame(pd.DataFrame(rows).round(3)))

cur = tiers[tiers.is_current]
print(f"Current watchlist ({DATA_END.date()}): {(cur.failures_prior_90 >= 2).sum():,} wells with 2+ failures in the last 90 days")

fig, ax = plt.subplots(figsize=(6, 3.5))
r = ev.groupby("tier").failed_next_90.mean()
ax.bar(r.index, r.values)
ax.axhline(ev.failed_next_90.mean(), ls="--", c="grey", label="all wells with a logged failure")
ax.set_xlabel("Failures in the prior 90 days")
ax.set_ylabel("Share failing again in the next 90 days")
ax.legend(fontsize=8)
fig.tight_layout()
display(fig)

# COMMAND ----------

checks = {
    "one row per failure in gap table": spark.table(GAP_TARGET).groupBy("failure_id").count().filter("count > 1").count() == 0,
    "one row per well-snapshot": spark.table(TIER_TARGET).groupBy("uwi", "snapshot_date").count().filter("count > 1").count() == 0,
    "re-failure@90 between 50% and 65% (raw-file run: 57.1%)": 0.50 <= overall["refail_90"] <= 0.65,
}
for name, ok in checks.items():
    print(("PASS " if ok else "FAIL ") + name)
assert all(checks.values()), "One or more checks failed"
