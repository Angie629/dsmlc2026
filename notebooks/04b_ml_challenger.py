# Databricks notebook source
# MAGIC %md
# MAGIC # 04b · Alert tiers and an ML challenger · tasks n6, n7
# MAGIC
# MAGIC Build plan tasks n6 and n7. Needs `01a_clean_daily`, `01b_clean_jobs_failures`, `01d_daily_checks` (`gold_failure_downtime`) and `04a_early_warning` section 8 (`gold_warning_eval`). Writes `gold_alert_tiers`, `gold_model_eval` and `gold_model_weights`.
# MAGIC
# MAGIC 1. **Alert tiers (n6).** Split the 2026 holdout alerts by which rule fired on the alert day: both rules, the recency watchlist only, or the runtime rule only. Precision per tier tells crews which alerts to work first, a risk score without a model.
# MAGIC 2. **ML challenger (n7).** One row per producing well every 14 days. Label: a rod, pump or tubing stop 2–30 days later (the same definition as the alert test). Features use only data before the snapshot. Train on Jun–Dec 2025, test on Jan–Oct 2026 with no tuning on 2026, and compare with the rules **at the same number of flags**.
# MAGIC
# MAGIC Rules come first because the failure log only starts in Jun 2025, torque is missing or repeated on most well-days, and recent failures already carry most of the signal. A model is adopted only if it holds its edge without more alerts for crews.

# COMMAND ----------

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

CATALOG_SCHEMA = "workspace.dsmlc"
DAILY = f"{CATALOG_SCHEMA}.silver_daily"
FAILURES = f"{CATALOG_SCHEMA}.silver_failures"
JOBS = f"{CATALOG_SCHEMA}.silver_jobs"
STOPS = f"{CATALOG_SCHEMA}.gold_failure_downtime"
WARNING_EVAL = f"{CATALOG_SCHEMA}.gold_warning_eval"

LOG_START = pd.Timestamp("2025-06-01")
DATA_END = pd.Timestamp("2026-10-27")
LOAD_START = "2024-10-01"
LEAD_MIN, LEAD_MAX = 2, 30
SUPPRESS_DAYS = 14
PARTIAL_DAYS = 2
RECENCY_DAYS, RECENCY_MIN = 90, 2
HOLDOUT = dict(alert0=pd.Timestamp("2026-01-01"), alert1=DATA_END, label0=pd.Timestamp("2026-01-31"), label1=DATA_END)
TRAIN = ("2025-06-02", "2025-11-30")
TEST = ("2026-01-05", str((DATA_END - pd.Timedelta(days=LEAD_MAX)).date()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load
# MAGIC
# MAGIC Producing-direction well-days from Oct 2024 (enough history for 180-day windows on the first snapshot), every failure since Jun 2025, Rod Truck and Workover jobs with a cost, and the stop day of each rod, pump or tubing failure from `gold_failure_downtime`.

# COMMAND ----------

def to_day(s):
    s = pd.to_datetime(s)
    return (s.dt.tz_localize(None) if s.dt.tz is not None else s).dt.normalize()


def load():
    d = (spark.table(DAILY)
         .filter((F.col("flow_direction") == "production") & (F.col("production_date") >= LOAD_START))
         .select("uwi", F.col("production_date").alias("day"), "field_operating_area", "well_daily_status",
                 F.col("hours_on_prod").cast("double").alias("hours_on_prod"),
                 F.col("pump_efficiency_pct").cast("double").alias("pump_efficiency_pct"),
                 F.col("pcp_pump_make_norm").alias("pump_make"))
         .toPandas())
    f = (spark.table(FAILURES)
         .filter(F.col("failure_start_date") >= str(LOG_START.date()))
         .select("uwi", F.col("failure_start_date").alias("day"), F.col("failure_type_norm").alias("ftype"))
         .toPandas())
    j = (spark.table(JOBS)
         .filter(F.col("is_repair") & (F.col("field_est_clean") > 0))
         .select("uwi", F.col("start_date").alias("day"), F.col("field_est_clean").cast("double").alias("field_est_total"))
         .toPandas())
    stops = (spark.table(STOPS).filter(F.col("stop_date").isNotNull())
             .select("uwi", F.col("stop_date").alias("stop")).toPandas())
    d["day"], f["day"], j["day"], stops["stop"] = to_day(d.day), to_day(f.day), to_day(j.day), to_day(stops.stop)
    f["ftype"] = f.ftype.where(f.ftype.isin(["Rods", "Pump", "Tubing"]), "Other")
    return d, f, j, stops


d, f, j, stops = load()
print(f"{len(d):,} producing-direction well-days, {len(f):,} failures since Jun 2025, {len(j):,} costed repair jobs, {len(stops):,} rod/pump/tubing stops")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Alert tiers on the 2026 holdout (n6)
# MAGIC
# MAGIC Same test as `04a` section 8: rules run on producing days only; a hit is a stop 2–30 days after the alert; one alert per well per 14 days. The first three rows must match `gold_warning_eval`.

# COMMAND ----------

def daily_grid(d, a0, a1, extra_wells=()):
    """Day x well arrays: producing flag, partial-run days in the last 7 days, area."""
    dates = pd.date_range(a0, a1, freq="D")
    wells = pd.Index(sorted(set(d.uwi) | set(extra_wells)))
    x = d[(d.day >= a0 - pd.Timedelta(days=6)) & (d.day <= a1)].copy()
    x["partial"] = ((x.hours_on_prod > 0) & (x.hours_on_prod < 23)).astype(int)
    full = pd.date_range(a0 - pd.Timedelta(days=6), a1, freq="D")
    P = x.pivot_table(index="day", columns="uwi", values="partial", aggfunc="max").reindex(index=full, columns=wells).fillna(0)
    partial_7d = P.rolling(7, min_periods=1).sum().loc[dates].to_numpy()
    prod = x.assign(p=(x.well_daily_status == "Producing")).pivot_table(index="day", columns="uwi", values="p", aggfunc="max")
    active = prod.reindex(index=dates, columns=wells).fillna(False).astype(bool).to_numpy()
    area = d.groupby("uwi").field_operating_area.agg(lambda s: s.mode().iloc[0] if s.notna().any() else "Unknown")
    return dates, wells, active, partial_7d, area.reindex(wells).fillna("Unknown").to_numpy()


def suppress(trigger, gap=SUPPRESS_DAYS):
    kept = np.zeros_like(trigger)
    last = np.full(trigger.shape[1], -10**9)
    for i in range(trigger.shape[0]):
        fire = trigger[i] & (i - last > gap)
        kept[i] = fire
        last[fire] = i
    return kept


def holdout_context(d, f, stops):
    w = HOLDOUT
    # Stop and failure wells are kept even without producing rows, as in 04a, so recall uses the same denominator.
    dates, wells, active, partial_7d, area = daily_grid(d, w["alert0"], w["alert1"], set(stops.uwi) | set(f.uwi))
    D, W, d0 = len(dates), len(wells), dates[0]
    hit = np.zeros((D, W), dtype=bool)
    labels = []
    for uwi, stop in zip(stops.uwi, stops.stop):
        if uwi not in wells:
            continue
        k, fi = wells.get_loc(uwi), (stop - d0).days
        lo, hi = max(0, fi - LEAD_MAX), min(D, fi - LEAD_MIN + 1)
        if hi > lo:
            hit[lo:hi, k] = True
        if w["label0"] <= stop <= w["label1"]:
            labels.append((k, fi))
    days = np.arange(D)
    watch = np.zeros((D, W), dtype=bool)
    for uwi, g in f.groupby("uwi"):
        if uwi in wells:
            fd = np.sort((g.day - d0).dt.days.to_numpy())
            watch[:, wells.get_loc(uwi)] = (np.searchsorted(fd, days, "left") - np.searchsorted(fd, days - RECENCY_DAYS, "left")) >= RECENCY_MIN
    return dict(D=D, W=W, active=active, hit=hit, labels=labels, area=area, week=days // 7,
                watch=watch & active, runtime=(partial_7d >= PARTIAL_DAYS) & active)


def evaluate(sel, ctx):
    alerts, hits = int(sel.sum()), int(ctx["hit"][sel].sum())
    caught = 0
    for k, fi in ctx["labels"]:
        lo, hi = max(0, fi - LEAD_MAX), min(ctx["D"], fi - LEAD_MIN + 1)
        caught += bool(hi > lo and sel[lo:hi, k].any())
    di, wi = np.nonzero(sel)
    weeks = pd.DataFrame({"a": ctx["area"][wi], "w": ctx["week"][di]})
    act_d, act_w = np.nonzero(ctx["active"])
    area_weeks = pd.DataFrame({"a": ctx["area"][act_w], "w": ctx["week"][act_d]}).drop_duplicates()
    return dict(alerts=alerts, hits=hits, precision_pct=round(100 * hits / alerts, 1) if alerts else 0.0,
                failures_caught=caught, label_failures=len(ctx["labels"]),
                recall_pct=round(100 * caught / len(ctx["labels"]), 1),
                alerts_per_area_week=round(len(weeks) / len(area_weeks), 2))


def n6_tiers(ctx):
    watch, runtime = suppress(ctx["watch"]), suppress(ctx["runtime"])
    rows = [{"tier_order": 0, "rule": "Recency watchlist (2+ in 90 days)", **evaluate(watch, ctx)},
            {"tier_order": 0, "rule": "Runtime rule (2+ partial-run days in 7)", **evaluate(runtime, ctx)},
            {"tier_order": 0, "rule": "Either rule", **evaluate(watch | runtime, ctx)}]
    either = suppress(ctx["watch"] | ctx["runtime"])
    rows += [{"tier_order": 1, "rule": "1 · Both rules", **evaluate(either & ctx["watch"] & ctx["runtime"], ctx)},
             {"tier_order": 2, "rule": "2 · Watchlist only", **evaluate(either & ctx["watch"] & ~ctx["runtime"], ctx)},
             {"tier_order": 3, "rule": "3 · Runtime rule only", **evaluate(either & ctx["runtime"] & ~ctx["watch"], ctx)}]
    return pd.DataFrame(rows)


ctx = holdout_context(d, f, stops)
tiers = n6_tiers(ctx)
display(tiers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ML challenger (n7)
# MAGIC
# MAGIC Features per snapshot, all from before the snapshot day: failures in the last 30 / 90 / 180 days and so far, days since the last failure, last failure type, Rod Truck and Workover $ in the last 12 months, partial-run and down days in the last 7 and 30 days, efficiency drift (7-day mean minus the prior 60-day mean), area and pump make. Logistic regression (scaled, class-weighted) and gradient-boosted trees, both from scikit-learn, no tuning.
# MAGIC
# MAGIC Comparison at the same workload: the model flags as many test snapshots as the rule does, its top scores, and both are scored on the same snapshots.

# COMMAND ----------

def snapshots(d, f, j, stops, start, end):
    """One row per producing well every 14 days; features use data before the snapshot day only."""
    days = pd.date_range(start, end, freq="14D")
    x = d.copy()
    x["partial"] = ((x.hours_on_prod > 0) & (x.hours_on_prod < 23)).astype(int)
    x["down"] = (x.hours_on_prod.fillna(0) < 1).astype(int)
    top = x.pump_make.value_counts().head(8).index
    x["make"] = np.where(x.pump_make.isin(top), x.pump_make, "Other")
    rows = []
    for t in days:
        today = x[(x.day == t) & (x.well_daily_status == "Producing")][["uwi", "field_operating_area", "make"]]
        if today.empty:
            continue
        r = today.drop_duplicates("uwi").set_index("uwi")
        for n in (7, 30):
            win = x[(x.day >= t - pd.Timedelta(days=n)) & (x.day < t)].groupby("uwi")
            r[f"partial_{n}d"] = win.partial.sum()
            r[f"down_{n}d"] = win.down.sum()
        eff7 = x[(x.day >= t - pd.Timedelta(days=7)) & (x.day < t)].groupby("uwi").pump_efficiency_pct.mean()
        eff60 = x[(x.day >= t - pd.Timedelta(days=60)) & (x.day < t - pd.Timedelta(days=7))].groupby("uwi").pump_efficiency_pct.mean()
        r["efficiency_drift"] = eff7 - eff60
        past = f[f.day < t]
        for n in (30, 90, 180):
            r[f"failures_{n}d"] = past[past.day >= t - pd.Timedelta(days=n)].groupby("uwi").size()
        r["failures_so_far"] = past.groupby("uwi").size()
        last = past.sort_values("day").groupby("uwi").last()
        r["days_since_failure"] = (t - last.day).dt.days
        r["last_failure_type"] = last.ftype
        r["repair_cost_12m"] = j[(j.day < t) & (j.day >= t - pd.Timedelta(days=365))].groupby("uwi").field_est_total.sum()
        s = stops[(stops.stop >= t + pd.Timedelta(days=LEAD_MIN)) & (stops.stop <= t + pd.Timedelta(days=LEAD_MAX))]
        r["label"] = r.index.isin(s.uwi).astype(int)
        r["snapshot"] = t
        rows.append(r.reset_index())
    out = pd.concat(rows, ignore_index=True)
    for c in ["failures_30d", "failures_90d", "failures_180d", "failures_so_far", "repair_cost_12m",
              "partial_7d", "partial_30d", "down_7d", "down_30d"]:
        out[c] = out[c].fillna(0)
    out["last_failure_type"] = out.last_failure_type.fillna("None")
    out["field_operating_area"] = out.field_operating_area.fillna("Unknown")
    return out


NUMERIC = ["failures_30d", "failures_90d", "failures_180d", "failures_so_far", "days_since_failure",
           "repair_cost_12m", "partial_7d", "partial_30d", "down_7d", "down_30d", "efficiency_drift"]
CATEGORICAL = ["last_failure_type", "field_operating_area", "make"]


def design(df, columns=None):
    X = pd.get_dummies(df[NUMERIC + CATEGORICAL], columns=CATEGORICAL, dtype=float)
    return X if columns is None else X.reindex(columns=columns, fill_value=0.0)


def top_n(score, n):
    sel = np.zeros(len(score), dtype=bool)
    sel[np.argsort(-score, kind="stable")[:n]] = True
    return sel


def score_selection(test, sel, stops):
    """Precision on snapshots; recall over test-period stops that had a snapshot 2-30 days before."""
    eligible = stops[(stops.stop >= test.snapshot.min() + pd.Timedelta(days=LEAD_MIN)) &
                     (stops.stop <= test.snapshot.max() + pd.Timedelta(days=LEAD_MAX))]
    picked = test[sel]
    pairs = picked[["uwi", "snapshot"]].merge(eligible, on="uwi")
    lead = (pairs.stop - pairs.snapshot).dt.days
    caught = pairs[(lead >= LEAD_MIN) & (lead <= LEAD_MAX)][["uwi", "stop"]].drop_duplicates()
    return dict(flagged=int(sel.sum()), precision_pct=round(100 * picked.label.mean(), 1),
                failures_caught=len(caught), label_failures=len(eligible),
                recall_pct=round(100 * len(caught) / len(eligible), 1))


train = snapshots(d, f, j, stops, *TRAIN)
test = snapshots(d, f, j, stops, *TEST)
Xtr = design(train)
Xte = design(test, Xtr.columns)
fill = Xtr.median()
hgb = HistGradientBoostingClassifier(max_iter=200, learning_rate=0.05, random_state=0).fit(Xtr, train.label)
lr = make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000, class_weight="balanced")).fit(Xtr.fillna(fill), train.label)
scores = {"Logistic regression": lr.predict_proba(Xte.fillna(fill))[:, 1],
          "Gradient-boosted trees": hgb.predict_proba(Xte)[:, 1]}

watch = (test.failures_90d >= RECENCY_MIN).to_numpy()
runtime = (test.partial_7d >= PARTIAL_DAYS).to_numpy()
rows = []
for workload, rule in (("Watchlist", watch), ("Either rule", watch | runtime)):
    label = f"{workload} ({int(rule.sum()):,} flags)"
    rows.append({"workload": label, "method": "Rule", **score_selection(test, rule, stops)})
    for name, s in scores.items():
        rows.append({"workload": label, "method": name, **score_selection(test, top_n(s, int(rule.sum())), stops)})
model_eval = pd.DataFrame(rows)
weights = (pd.Series(lr[-1].coef_[0], index=Xtr.columns).rename("standardized_weight").reset_index()
           .rename(columns={"index": "feature"}).sort_values("standardized_weight", key=abs, ascending=False))
print(f"Train: {len(train):,} snapshots, {100 * train.label.mean():.1f}% labelled. Test: {len(test):,} snapshots, {100 * test.label.mean():.1f}% labelled.")
display(model_eval)
display(weights.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Checks and write

# COMMAND ----------

ref = spark.table(WARNING_EVAL).filter("window = '2026 holdout'").toPandas().set_index("rule_name")
mine = tiers.set_index("rule")
pairs = {"Recency watchlist (2+ in 90 days)": "Recency watchlist (2+ in 90 days)",
         "Runtime rule (2+ partial-run days in 7)": "Runtime + efficiency rule",
         "Either rule": "Rule or watchlist"}
checks = {
    f"{a} matches gold_warning_eval": int(mine.loc[a, "alerts"]) == int(ref.loc[b, "alerts"])
    and int(mine.loc[a, "failures_caught"]) == int(ref.loc[b, "failures_caught"])
    for a, b in pairs.items()
}
checks["tiers add up to the either-rule stream"] = int(tiers[tiers.tier_order > 0].alerts.sum()) == int(
    suppress(ctx["watch"] | ctx["runtime"]).sum())
checks["no snapshot feature reads the snapshot day or later"] = bool((train.days_since_failure.dropna() > 0).all() and (test.days_since_failure.dropna() > 0).all())
checks["train and test snapshots do not overlap"] = train.snapshot.max() < test.snapshot.min()
checks["model and rule flag the same number of snapshots"] = bool(model_eval.groupby("workload").flagged.nunique().eq(1).all())
for name, ok in checks.items():
    print(("PASS " if ok else "FAIL ") + name)
assert all(checks.values()), "One or more checks failed"

spark.createDataFrame(tiers).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG_SCHEMA}.gold_alert_tiers")
spark.createDataFrame(model_eval).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG_SCHEMA}.gold_model_eval")
spark.createDataFrame(weights).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG_SCHEMA}.gold_model_weights")
print("Wrote gold_alert_tiers, gold_model_eval, gold_model_weights")
