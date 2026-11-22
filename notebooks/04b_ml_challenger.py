"""Tasks n6 (alert tiers) and n7 (time-boxed ML challenger), run locally on the organizer Parquet files.

Run from the repo root (needs dataset/*.parquet, pandas, scikit-learn):
    python notebooks/04b_ml_challenger.py

Runs locally because the Databricks Free Edition warehouse hit its daily compute limit on Nov 21.
It rebuilds the 04a early-warning test (stop-day labels, 2+ partial-run days in 7, recency watchlist,
14-day suppression) and first checks that it reproduces the 2026 holdout numbers in gold_warning_eval.
Writes results to docs/figures/n6_alert_tiers.csv and docs/figures/n7_model_vs_rules.csv.
"""
import os

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA = os.path.join(ROOT, "dataset")
OUT = os.path.join(ROOT, "docs", "figures")

LOG_START = pd.Timestamp("2025-06-01")
DATA_END = pd.Timestamp("2026-10-27")
LOAD_START = pd.Timestamp("2024-10-01")
LOOK_BACK, LOOK_AHEAD = 60, 60
LEAD_MIN, LEAD_MAX = 2, 30
SUPPRESS_DAYS = 14
PARTIAL_DAYS = 2
RECENCY_DAYS, RECENCY_MIN = 90, 2
HOLDOUT = dict(alert0=pd.Timestamp("2026-01-01"), alert1=DATA_END, label0=pd.Timestamp("2026-01-31"), label1=DATA_END)
TYPE_MAP = {"rods": "Rods", "rod": "Rods", "|rod": "Rods", "pump": "Pump", "pcp": "Pump",
            "tubing": "Tubing", "hole in tbg colar": "Tubing"}
DOWNHOLE = ("Rods", "Pump", "Tubing")
REPAIR = ("rod truck", "workover")


def load():
    cols = ["uwi", "production_date", "field_operating_area", "well_daily_status", "flow_direction",
            "hours_on_prod", "pump_efficiency_pct", "pcp_pump_make"]
    d = pd.read_parquet(os.path.join(DATA, "daily_production.parquet"), columns=cols)
    d["day"] = pd.to_datetime(d.production_date).dt.normalize()
    d = d[(d.day >= LOAD_START) & (d.flow_direction == "production")]
    d = d.sort_values(["uwi", "day", "hours_on_prod"], na_position="first").drop_duplicates(["uwi", "day"], keep="last")

    f = pd.read_parquet(os.path.join(DATA, "production_failures.parquet"))
    f = f.drop_duplicates(["uwi", "failure_start_date", "failure_type"])
    f["day"] = pd.to_datetime(f.failure_start_date).dt.normalize()
    f["ftype"] = f.failure_type.str.strip().str.lower().map(TYPE_MAP).fillna("Other")
    f = f[f.day >= LOG_START].reset_index(drop=True)

    j = pd.read_parquet(os.path.join(DATA, "well_jobs.parquet"))
    j["day"] = pd.to_datetime(j.start_date).dt.normalize()
    j = j.drop_duplicates(["uwi", "day", "job_type", "job_subtype"])
    j = j[j.job_category.str.strip().str.lower().isin(REPAIR) & (j.field_est_total > 0)]
    return d, f, j[["uwi", "day", "field_est_total"]]


def stop_days(d, f):
    """Stop day per rod/pump/tubing failure: start of the down streak covering the logged day or the day before (01d)."""
    hours = {u: g.set_index("day").hours_on_prod for u, g in d[d.uwi.isin(f.uwi)].groupby("uwi")}
    empty = pd.Series(dtype=float)
    offs = np.arange(-LOOK_BACK, LOOK_AHEAD + 1)
    stops = []
    for uwi, day in zip(*f.loc[f.ftype.isin(DOWNHOLE), ["uwi", "day"]].T.values):
        h = hours.get(uwi, empty).reindex(day + pd.to_timedelta(offs, "D")).fillna(0).to_numpy()
        down = h < 1

        def streak_start(i):
            while i > 0 and down[i - 1]:
                i -= 1
            return offs[i]

        i0 = LOOK_BACK
        off = streak_start(i0) if down[i0] else (streak_start(i0 - 1) if down[i0 - 1] else 0)
        stops.append((uwi, day + pd.Timedelta(days=int(off))))
    return pd.DataFrame(stops, columns=["uwi", "stop"])


def daily_grid(d, a0, a1):
    """Day x well arrays: producing flag, partial-run days in the last 7 days, area."""
    dates = pd.date_range(a0, a1, freq="D")
    wells = pd.Index(sorted(d.uwi.unique()))
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
    dates, wells, active, partial_7d, area = daily_grid(d, w["alert0"], w["alert1"])
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
    rows = [{"rule": "Recency watchlist (2+ in 90 days)", **evaluate(watch, ctx)},
            {"rule": "Runtime rule (2+ partial-run days in 7)", **evaluate(runtime, ctx)},
            {"rule": "Either rule", **evaluate(watch | runtime, ctx)}]
    either = suppress(ctx["watch"] | ctx["runtime"])
    both = either & ctx["watch"] & ctx["runtime"]
    rows += [{"rule": "Tier 1: both rules on the alert day", **evaluate(both, ctx)},
             {"rule": "Tier 2: watchlist only", **evaluate(either & ctx["watch"] & ~ctx["runtime"], ctx)},
             {"rule": "Tier 3: runtime rule only", **evaluate(either & ctx["runtime"] & ~ctx["watch"], ctx)}]
    return pd.DataFrame(rows)


def snapshots(d, f, j, stops, start, end):
    """One row per producing well every 14 days; features use data before the snapshot day only."""
    days = pd.date_range(start, end, freq="14D")
    x = d.copy()
    x["partial"] = ((x.hours_on_prod > 0) & (x.hours_on_prod < 23)).astype(int)
    x["down"] = (x.hours_on_prod.fillna(0) < 1).astype(int)
    make = x.pcp_pump_make.str.strip().str.lower().str.split().str[0]
    top = make.value_counts().head(8).index
    x["make"] = np.where(make.isin(top), make, "other")
    rows = []
    for t in days:
        today = x[(x.day == t) & (x.well_daily_status == "Producing")][["uwi", "field_operating_area", "make"]]
        if today.empty:
            continue
        r = today.set_index("uwi")
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
    """Precision on snapshots, recall over 2026 stops that had a snapshot 2-30 days before."""
    eligible = stops[(stops.stop >= test.snapshot.min() + pd.Timedelta(days=LEAD_MIN)) &
                     (stops.stop <= test.snapshot.max() + pd.Timedelta(days=LEAD_MAX))]
    picked = test[sel]
    pairs = picked[["uwi", "snapshot"]].merge(eligible, on="uwi")
    lead = (pairs.stop - pairs.snapshot).dt.days
    caught = pairs[(lead >= LEAD_MIN) & (lead <= LEAD_MAX)][["uwi", "stop"]].drop_duplicates()
    return dict(flagged=int(sel.sum()), precision_pct=round(100 * picked.label.mean(), 1),
                failures_caught=len(caught), label_failures=len(eligible),
                recall_pct=round(100 * len(caught) / len(eligible), 1))


def n7_challenger(d, f, j, stops):
    train = snapshots(d, f, j, stops, "2025-06-02", "2025-11-30")
    test = snapshots(d, f, j, stops, "2026-01-05", DATA_END - pd.Timedelta(days=LEAD_MAX))
    Xtr = design(train)
    Xte = design(test, Xtr.columns)
    hgb = HistGradientBoostingClassifier(max_iter=200, learning_rate=0.05, random_state=0).fit(Xtr, train.label)
    lr = make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000, class_weight="balanced"))
    lr.fit(Xtr.fillna(Xtr.median()), train.label)
    scores = {"Gradient-boosted trees": hgb.predict_proba(Xte)[:, 1],
              "Logistic regression": lr.predict_proba(Xte.fillna(Xtr.median()))[:, 1]}
    watch = (test.failures_90d >= RECENCY_MIN).to_numpy()
    runtime = (test.partial_7d >= PARTIAL_DAYS).to_numpy()
    rows = []
    for rule_name, rule in (("Recency watchlist", watch), ("Either rule", watch | runtime)):
        rows.append({"workload": rule_name, "method": rule_name, **score_selection(test, rule, stops)})
        for name, s in scores.items():
            rows.append({"workload": rule_name, "method": name, **score_selection(test, top_n(s, int(rule.sum())), stops)})
    lr_coef = pd.Series(lr[-1].coef_[0], index=Xtr.columns).sort_values(key=abs, ascending=False)
    info = dict(train_rows=len(train), train_label_rate=round(100 * train.label.mean(), 2),
                test_rows=len(test), test_label_rate=round(100 * test.label.mean(), 2))
    return pd.DataFrame(rows), lr_coef.head(8), info


if __name__ == "__main__":
    pd.set_option("display.width", 200)
    os.makedirs(OUT, exist_ok=True)
    d, f, j = load()
    stops = stop_days(d, f)
    print(f"{len(f):,} failures since Jun 2025, {len(stops):,} rod/pump/tubing stops")

    ctx = holdout_context(d, f, stops)
    tiers = n6_tiers(ctx)
    print("\nn6 · 2026 holdout (compare the first three rows with gold_warning_eval: 28.8/34.9, 17.0/43.6, 21.6/56.8)")
    print(tiers.to_string(index=False))
    tiers.to_csv(os.path.join(OUT, "n6_alert_tiers.csv"), index=False)

    result, coef, info = n7_challenger(d, f, j, stops)
    print(f"\nn7 · snapshots every 14 days: {info}")
    print(result.to_string(index=False))
    print("\nLogistic regression, largest standardized coefficients:")
    print(coef.round(2).to_string())
    result.to_csv(os.path.join(OUT, "n7_model_vs_rules.csv"), index=False)
