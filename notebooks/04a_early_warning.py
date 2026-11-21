# Databricks notebook source
# MAGIC %md
# MAGIC # 04a · Early warning: sensors before failures and threshold rules · tasks s3, s6, s9
# MAGIC
# MAGIC Build plan tasks s3, s6, s9. Needs `01a_clean_daily`, `01b_clean_jobs_failures`, `01c_downtime_features` and `01d_daily_checks`. Sections 1–7 are read-only; section 8 writes `gold_warning_eval` and `gold_alerts`.
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Threshold rules: tune on 2025, freeze, test on 2026 (tasks s6, s9)
# MAGIC
# MAGIC Candidate rules use runtime instability and pump efficiency, not torque or current (those repeat day to day):
# MAGIC - partial-run days (0 < hours on < 23) in the last 7 days
# MAGIC - fully down days in the last 7 days, **not counting** economic, planned or injection downtime
# MAGIC - efficiency 7-day mean below its baseline by a set number of points (`efficiency_chg` from `feat_daily_sensor`)
# MAGIC
# MAGIC Rules run only on days a well is **producing** (`flow_direction = 'production'`, `well_daily_status = 'Producing'`), so suspended, abandoned and injection wells never alert.
# MAGIC
# MAGIC **A hit** is an alert 2 to 30 days before the well stops for a rod, pump or tubing failure. The stop day comes from `gold_failure_downtime.stop_date`, the same definition used for payback. The day just before a stop is excluded because it is usually the partial-run day of the failure itself (68% of cases).
# MAGIC
# MAGIC One alert per well per 14 days. Thresholds are chosen on Jun–Dec 2025 only (recall ≥ 20%, 90th-percentile alerts per area-week ≤ 15, best precision) and frozen before the 2026 test. The recency watchlist (2+ failures in the last 90 days) is scored the same way as the baseline to beat.
# MAGIC
# MAGIC The daily features are built in Spark; pandas only handles small per-well arrays.

# COMMAND ----------

from itertools import product

DOWNTIME = f"{CATALOG_SCHEMA}.silver_downtime"
SENSOR = f"{CATALOG_SCHEMA}.feat_daily_sensor"
STOPS = f"{CATALOG_SCHEMA}.gold_failure_downtime"
DATA_END = "2026-10-27"
WINDOWS = {
    "2025 tuning": dict(alert0="2025-06-01", alert1="2025-12-31", label0="2025-07-01", label1="2025-12-31"),
    "2026 holdout": dict(alert0="2026-01-01", alert1=DATA_END, label0="2026-01-31", label1=DATA_END),
}
LEAD_MIN, LEAD_MAX = 2, 30
SUPPRESS_DAYS = 14
EXCLUDED_DOWN_BUCKETS = ("Economic", "Planned", "Injection")
RECENCY_DAYS, RECENCY_MIN = 90, 2
CURRENT_DAYS = 14
MIN_RECALL = 0.20
MAX_ALERTS_AREA_WEEK_P90 = 15
PARTIAL_GRID = [1, 2, 3, None]
DOWN_GRID = [1, 2, 3, None]
EFFICIENCY_GRID = [0.03, 0.05, 0.08, 0.12, None]


def feature_sql(a0, a1):
    excluded = ", ".join(f"'{b}'" for b in EXCLUDED_DOWN_BUCKETS)
    return f"""
WITH d AS (
  SELECT s.uwi, s.production_date, s.field_operating_area, s.well_daily_status,
         DATEDIFF(s.production_date, DATE'2020-01-01') AS dn,
         CASE WHEN s.hours_on_prod > 0 AND s.hours_on_prod < 23 THEN 1 ELSE 0 END AS partial,
         CASE WHEN s.hours_on_prod < 1 AND COALESCE(dt.bucket, 'Uncoded') NOT IN ({excluded}) THEN 1 ELSE 0 END AS down
  FROM {DAILY} s
  LEFT JOIN {DOWNTIME} dt ON dt.uwi = s.uwi AND dt.production_date = s.production_date
  WHERE s.flow_direction = 'production' AND s.production_date BETWEEN date_sub(DATE'{a0}', 6) AND DATE'{a1}'),
r AS (
  SELECT *,
         SUM(partial) OVER (PARTITION BY uwi ORDER BY dn RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS partial_7d,
         SUM(down) OVER (PARTITION BY uwi ORDER BY dn RANGE BETWEEN 6 PRECEDING AND CURRENT ROW) AS down_7d
  FROM d)
SELECT r.uwi, r.production_date, r.field_operating_area, r.partial_7d, r.down_7d, f.efficiency_chg
FROM r LEFT JOIN {SENSOR} f ON f.uwi = r.uwi AND f.production_date = r.production_date
WHERE r.production_date BETWEEN DATE'{a0}' AND DATE'{a1}' AND r.well_daily_status = 'Producing'"""


STOPS_SQL = f"SELECT uwi, stop_date FROM {STOPS} WHERE stop_date IS NOT NULL"
FAILURES_SQL = f"SELECT uwi, failure_start_date FROM {FAILURES} WHERE failure_start_date >= DATE'2025-06-01'"


def load_window(w):
    return (
        spark.sql(feature_sql(w["alert0"], w["alert1"])).toPandas(),
        spark.sql(STOPS_SQL).toPandas(),
        spark.sql(FAILURES_SQL).toPandas(),
    )

# COMMAND ----------

def to_day(s):
    s = pd.to_datetime(s)
    return (s.dt.tz_localize(None) if s.dt.tz is not None else s).dt.normalize()


def prepare(feat, stops, fails, w):
    """Day x well arrays for one window. Rows are days from alert0 to alert1, columns are wells."""
    dates = pd.date_range(w["alert0"], w["alert1"], freq="D")
    d0, D = dates[0], len(dates)
    feat = feat.assign(production_date=to_day(feat.production_date))
    stops = stops.assign(stop_date=to_day(stops.stop_date))
    fails = fails.assign(failure_start_date=to_day(fails.failure_start_date))

    wells = pd.Index(sorted(set(feat.uwi) | set(stops.uwi) | set(fails.uwi)))
    W = len(wells)
    wi = wells.get_indexer(feat.uwi)
    di = (feat.production_date - d0).dt.days.to_numpy()

    active = np.zeros((D, W), dtype=bool)
    partial_7d = np.zeros((D, W))
    down_7d = np.zeros((D, W))
    eff_chg = np.full((D, W), np.nan)
    active[di, wi] = True
    partial_7d[di, wi] = feat.partial_7d.to_numpy()
    down_7d[di, wi] = feat.down_7d.to_numpy()
    eff_chg[di, wi] = pd.to_numeric(feat.efficiency_chg).to_numpy(dtype=float)

    area = (feat.dropna(subset=["field_operating_area"]).groupby("uwi").field_operating_area.agg(lambda s: s.mode().iloc[0])
            .reindex(wells).fillna("Unknown").to_numpy())

    hit = np.zeros((D, W), dtype=bool)
    labels = []
    l0, l1 = pd.Timestamp(w["label0"]), pd.Timestamp(w["label1"])
    for uwi, stop in zip(stops.uwi, stops.stop_date):
        k, fi = wells.get_loc(uwi), (stop - d0).days
        lo, hi = max(0, fi - LEAD_MAX), min(D, fi - LEAD_MIN + 1)
        if hi > lo:
            hit[lo:hi, k] = True
        if l0 <= stop <= l1:
            labels.append((k, fi))

    baseline = np.zeros((D, W), dtype=bool)
    days = np.arange(D)
    for uwi, g in fails.groupby("uwi"):
        fd = np.sort((g.failure_start_date - d0).dt.days.to_numpy())
        recent = np.searchsorted(fd, days, side="left") - np.searchsorted(fd, days - RECENCY_DAYS, side="left")
        baseline[:, wells.get_loc(uwi)] = recent >= RECENCY_MIN
    baseline &= active

    week = days // 7
    aw = pd.DataFrame({"area": np.repeat(area[None, :], D, axis=0)[active], "week": np.repeat(week[:, None], W, axis=1)[active]})
    area_weeks = aw.drop_duplicates()

    return dict(dates=dates, D=D, W=W, wells=wells, area=area, week=week, active=active,
                partial_7d=partial_7d, down_7d=down_7d, eff_chg=eff_chg,
                hit=hit, labels=labels, baseline=baseline, area_weeks=area_weeks)


def suppress(trigger, gap=SUPPRESS_DAYS):
    D, W = trigger.shape
    kept = np.zeros_like(trigger)
    last = np.full(W, -10**9)
    for d in range(D):
        fire = trigger[d] & (d - last > gap)
        kept[d] = fire
        last[fire] = d
    return kept


def rule_trigger(ctx, partial_days, down_days, efficiency_drop):
    t = np.zeros((ctx["D"], ctx["W"]), dtype=bool)
    if partial_days is not None:
        t |= ctx["partial_7d"] >= partial_days
    if down_days is not None:
        t |= ctx["down_7d"] >= down_days
    if efficiency_drop is not None:
        t |= np.isfinite(ctx["eff_chg"]) & (ctx["eff_chg"] <= -efficiency_drop)
    return t & ctx["active"]


def evaluate(selected, ctx):
    alerts = int(selected.sum())
    hits = int(ctx["hit"][selected].sum())
    caught, leads = 0, []
    for k, fi in ctx["labels"]:
        lo, hi = max(0, fi - LEAD_MAX), min(ctx["D"], fi - LEAD_MIN + 1)
        first = np.flatnonzero(selected[lo:hi, k]) if hi > lo else []
        if len(first):
            caught += 1
            leads.append(fi - (lo + first[0]))
    d_idx, w_idx = np.nonzero(selected)
    per_aw = (pd.DataFrame({"area": ctx["area"][w_idx], "week": ctx["week"][d_idx]}).value_counts()
              .rename("n").reset_index())
    per_aw = ctx["area_weeks"].merge(per_aw, on=["area", "week"], how="left").fillna({"n": 0})
    n_labels = len(ctx["labels"])
    return dict(
        alerts=alerts,
        hits=hits,
        precision=round(hits / alerts, 3) if alerts else 0.0,
        recall=round(caught / n_labels, 3) if n_labels else 0.0,
        alerts_per_area_week=round(float(per_aw.n.mean()), 2),
        alerts_per_area_week_p90=round(float(per_aw.n.quantile(0.9)), 1),
        median_lead_days=float(np.median(leads)) if leads else None,
        failures_caught=caught,
        label_failures=n_labels,
    )


PREP = {name: prepare(*load_window(w), w) for name, w in WINDOWS.items()}
for name, ctx in PREP.items():
    print(f"{name}: {ctx['W']:,} wells, {int(ctx['active'].sum()):,} producing well-days, {len(ctx['labels']):,} failures to catch")

# COMMAND ----------

# DBTITLE 1,Tune on 2025 and freeze
tune = PREP["2025 tuning"]
rows = []
for p, dn, e in product(PARTIAL_GRID, DOWN_GRID, EFFICIENCY_GRID):
    if p is None and dn is None and e is None:
        continue
    rows.append({"partial_days": p, "down_days": dn, "efficiency_drop": e, **evaluate(suppress(rule_trigger(tune, p, dn, e)), tune)})
grid = pd.DataFrame(rows)
ok = grid[(grid.recall >= MIN_RECALL) & (grid.alerts_per_area_week_p90 <= MAX_ALERTS_AREA_WEEK_P90)]
if ok.empty:
    raise ValueError(f"No 2025 setting reaches recall >= {MIN_RECALL:.0%} with p90 alerts per area-week <= {MAX_ALERTS_AREA_WEEK_P90}")
best = ok.sort_values(["precision", "alerts"], ascending=[False, True]).iloc[0]
FROZEN = {k: (None if pd.isna(best[k]) else (float(best[k]) if k == "efficiency_drop" else int(best[k])))
          for k in ["partial_days", "down_days", "efficiency_drop"]}
print("Frozen on 2025:", FROZEN)
display(spark.createDataFrame(grid.sort_values("precision", ascending=False).head(15).astype({"partial_days": "float", "down_days": "float", "efficiency_drop": "float"})))

# COMMAND ----------

# DBTITLE 1,Test the frozen rule on 2026 against the recency watchlist
eval_rows, alert_rows = [], []
for name, ctx in PREP.items():
    rule = suppress(rule_trigger(ctx, **FROZEN))
    base = suppress(ctx["baseline"])
    for rule_name, sel, is_base in (("Runtime + efficiency rule", rule, False),
                                    ("Recency watchlist (2+ in 90 days)", base, True),
                                    ("Rule or watchlist", rule | base, False)):
        eval_rows.append({"window": name, "rule_name": rule_name, "is_baseline": is_base, **evaluate(sel, ctx), **FROZEN})
    d_idx, w_idx = np.nonzero(rule)
    alert_rows.append(pd.DataFrame({
        "alert_date": ctx["dates"][d_idx].date, "uwi": ctx["wells"][w_idx], "field_operating_area": ctx["area"][w_idx],
        "rule_name": "Runtime + efficiency rule", "window": name,
        "partial_7d": ctx["partial_7d"][d_idx, w_idx], "down_7d": ctx["down_7d"][d_idx, w_idx], "efficiency_chg": ctx["eff_chg"][d_idx, w_idx],
    }))

warning_eval = pd.DataFrame(eval_rows)
alerts = pd.concat(alert_rows, ignore_index=True)
alerts["is_current"] = pd.to_datetime(alerts.alert_date) > pd.Timestamp(DATA_END) - pd.Timedelta(days=CURRENT_DAYS)
display(spark.createDataFrame(warning_eval.astype({"partial_days": "float", "down_days": "float", "efficiency_drop": "float", "median_lead_days": "float"})))

spark.createDataFrame(warning_eval.astype({"partial_days": "float", "down_days": "float", "efficiency_drop": "float", "median_lead_days": "float"})) \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG_SCHEMA}.gold_warning_eval")
spark.createDataFrame(alerts).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG_SCHEMA}.gold_alerts")

# COMMAND ----------

hold = warning_eval[warning_eval.window == "2026 holdout"].set_index("rule_name")
s6_checks = {
    "thresholds chosen on 2025 only": set(grid.columns) >= {"precision", "recall"} and tune is PREP["2025 tuning"],
    "no alerts on non-producing days": all(not (suppress(rule_trigger(c, **FROZEN)) & ~c["active"]).any() for c in PREP.values()),
    "precision and recall between 0 and 1": bool(warning_eval.precision.between(0, 1).all() and warning_eval.recall.between(0, 1).all()),
    "one row per window and rule": not warning_eval.duplicated(["window", "rule_name"]).any(),
}
for name, ok_ in s6_checks.items():
    print(("PASS " if ok_ else "FAIL ") + name)
print("2026 holdout:")
print(hold[["precision", "recall", "alerts", "alerts_per_area_week", "median_lead_days"]].to_string())
beats = hold.loc["Runtime + efficiency rule", "precision"] > hold.loc["Recency watchlist (2+ in 90 days)", "precision"]
print("Rule beats the watchlist on precision:" if beats else "Rule does NOT beat the watchlist on precision: ship the watchlist as the early warning (risk 1 fallback).")
assert all(s6_checks.values()), "One or more checks failed"
