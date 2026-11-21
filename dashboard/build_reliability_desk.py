"""Build the Strathcona Reliability Desk AI/BI dashboard from dashboard/strathcona_reliability_desk.json (the design spec).

Usage:
    python dashboard/build_reliability_desk.py test     # run every dataset query on the SQL warehouse
    python dashboard/build_reliability_desk.py build    # write dashboard/strathcona_reliability_desk.lvdash.json

Needs the Databricks CLI with a `datathon` profile that has the `sql` scope (see docs/databricks_api_token_setup.md).
"""
import json
import os
import subprocess
import sys
import tempfile

PROFILE = "datathon"
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "3bc0e5ad165b90c4")
S = "workspace.dsmlc"
END = "2026-10-27"
LAST_12M = "2025-10-27"
LOG_START = "2025-06-01"
NETBACK = f"(SELECT MAX(netback_cad_per_bbl) FROM {S}.gold_well_period)"
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "strathcona_reliability_desk.lvdash.json")

DATASETS = {
    "ds_p1_kpis": ("Spend and lost oil, last 12 months, by area", f"""
WITH area AS (SELECT DISTINCT field_operating_area FROM {S}.gold_well_month WHERE field_operating_area IS NOT NULL),
rep AS (SELECT field_operating_area, SUM(field_est_clean) AS repair_cost_12m FROM {S}.silver_jobs
        WHERE is_repair AND start_date > DATE'{LAST_12M}' GROUP BY field_operating_area),
lost AS (SELECT field_operating_area, SUM(lost_oil_m3) AS lost_m3 FROM {S}.silver_downtime
         WHERE is_downhole AND production_date > DATE'{LAST_12M}' GROUP BY field_operating_area)
SELECT a.field_operating_area,
       COALESCE(r.repair_cost_12m, 0) AS repair_cost_12m,
       COALESCE(l.lost_m3, 0) AS downhole_lost_oil_m3_12m,
       COALESCE(l.lost_m3, 0) * 6.2898 * {NETBACK} AS downhole_lost_oil_value_12m
FROM area a LEFT JOIN rep r USING (field_operating_area) LEFT JOIN lost l USING (field_operating_area)"""),

    "ds_p1_loss_month": ("Lost oil by cause and month", f"""
SELECT DATE_TRUNC('MONTH', production_date) AS month,
       field_operating_area,
       CASE WHEN bucket IN ('Downhole', 'Surface equipment', 'Facility / pipeline', 'Power', 'Planned') THEN bucket ELSE 'Other' END AS downtime_bucket,
       SUM(lost_oil_m3) AS lost_oil_m3
FROM {S}.silver_downtime
WHERE flow_direction = 'production' AND production_date >= DATE'2023-01-01'
  AND bucket NOT IN ('Economic', 'Injection') AND lost_oil_m3 > 0
GROUP BY ALL"""),

    "ds_p1_type_share": ("Share of failures and repair cost by failure type", f"""
WITH f AS (
  SELECT CASE WHEN failure_type_norm IN ('Rods', 'Pump', 'Tubing') THEN failure_type_norm ELSE 'Other' END AS failure_type,
         CASE WHEN job_id IS NOT NULL THEN linked_job_cost / COUNT(*) OVER (PARTITION BY job_id) END AS cost
  FROM {S}.silver_failures WHERE failure_start_date >= DATE'{LOG_START}')
SELECT failure_type,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS share_of_failures_pct,
       ROUND(100 * SUM(COALESCE(cost, 0)) / SUM(SUM(COALESCE(cost, 0))) OVER (), 1) AS share_of_repair_cost_pct
FROM f GROUP BY failure_type"""),

    "ds_p1_repair_area": ("Repair spend by area and job type, last 12 months", f"""
SELECT field_operating_area, job_category_norm AS job_category, SUM(field_est_clean) AS repair_cost
FROM {S}.silver_jobs
WHERE is_repair AND start_date > DATE'{LAST_12M}' AND field_est_clean > 0
GROUP BY ALL"""),

    "ds_p2_counters": ("Re-failure within 90 days", f"""
SELECT ROUND(100 * AVG(refail_90), 1) AS refail_90d_all_pct,
       ROUND(100 * AVG(CASE WHEN failure_type_norm = 'Rods' THEN refail_90 END), 1) AS refail_90d_rods_pct,
       ROUND(100 * AVG(CASE WHEN failure_type_norm IN ('Pump', 'Tubing') THEN refail_90 END), 1) AS refail_90d_pump_tubing_pct,
       COUNT(*) AS failures_with_followup
FROM {S}.gold_failure_gap WHERE in_log_window AND has_90d_followup"""),

    "ds_p2_refail_type": ("Re-failure within 30 and 90 days by type", f"""
SELECT failure_type_norm AS failure_type,
       ROUND(100 * AVG(CASE WHEN has_30d_followup THEN refail_30 END), 1) AS refail_30d_pct,
       ROUND(100 * AVG(CASE WHEN has_90d_followup THEN refail_90 END), 1) AS refail_90d_pct
FROM {S}.gold_failure_gap
WHERE in_log_window AND failure_type_norm IN ('Rods', 'Pump', 'Tubing')
GROUP BY failure_type_norm"""),

    "ds_p2_gap": ("Days to the next failure", f"""
SELECT CASE WHEN days_to_next <= 14 THEN '1. 0-14 days' WHEN days_to_next <= 30 THEN '2. 15-30 days'
            WHEN days_to_next <= 60 THEN '3. 31-60 days' WHEN days_to_next <= 90 THEN '4. 61-90 days'
            WHEN days_to_next <= 180 THEN '5. 91-180 days' ELSE '6. 181+ days' END AS days_to_next_failure,
       COUNT(*) AS failures
FROM {S}.gold_failure_gap WHERE in_log_window AND days_to_next IS NOT NULL
GROUP BY ALL"""),

    "ds_p2_monthly": ("Failures per month by type", f"""
SELECT DATE_TRUNC('MONTH', failure_start_date) AS month,
       field_operating_area,
       CASE WHEN failure_type_norm IN ('Rods', 'Pump', 'Tubing') THEN failure_type_norm ELSE 'Other' END AS failure_type,
       COUNT(*) AS failures
FROM {S}.gold_failure_gap WHERE in_log_window
GROUP BY ALL"""),

    "ds_p2_tier": ("Chance of another failure in 90 days by recent failures", f"""
SELECT tier, ROUND(100 * AVG(failed_next_90), 1) AS p_fail_next_90d_pct, COUNT(*) AS well_snapshots
FROM {S}.gold_recency_tier WHERE NOT is_current
GROUP BY tier"""),

    "ds_p2_chronic": ("Wells and failures by failures per well", f"""
WITH w AS (SELECT uwi, COUNT(*) AS n FROM {S}.gold_failure_gap WHERE in_log_window GROUP BY uwi)
SELECT CASE WHEN n >= 5 THEN '5+' ELSE CAST(n AS STRING) END AS failures_per_well,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS share_of_wells_pct,
       ROUND(100 * SUM(n) / SUM(SUM(n)) OVER (), 1) AS share_of_failures_pct
FROM w GROUP BY ALL"""),

    "ds_p3_list": ("Work list: wells with a failure in the last 90 days", f"""
WITH cur AS (SELECT uwi, field_operating_area, failures_prior_90, last_failure_date FROM {S}.gold_recency_tier
             WHERE is_current AND failures_prior_90 >= 1),
lastf AS (SELECT uwi, failure_type_norm, cause FROM (
            SELECT uwi, failure_type_norm, cause,
                   ROW_NUMBER() OVER (PARTITION BY uwi ORDER BY failure_start_date DESC, failure_id) AS rn
            FROM {S}.silver_failures) WHERE rn = 1),
nm AS (SELECT uwi, MAX(well_name) AS well_name FROM {S}.silver_failures GROUP BY uwi),
lastm AS (SELECT uwi, facility_name, pcp_pump_make_norm FROM (
            SELECT uwi, facility_name, pcp_pump_make_norm, ROW_NUMBER() OVER (PARTITION BY uwi ORDER BY month DESC) AS rn
            FROM {S}.gold_well_month WHERE facility_name IS NOT NULL) WHERE rn = 1),
rep AS (SELECT uwi, SUM(field_est_clean) AS repair_cost_12m FROM {S}.silver_jobs
        WHERE is_repair AND start_date > DATE'{LAST_12M}' GROUP BY uwi),
lost AS (SELECT uwi, SUM(lost_oil_m3) AS lost_12m FROM {S}.silver_downtime
         WHERE is_downhole AND production_date > DATE'{LAST_12M}' GROUP BY uwi),
base AS (
  SELECT c.uwi, n.well_name, c.field_operating_area, m.facility_name, m.pcp_pump_make_norm AS pump_make,
         c.failures_prior_90 AS failures_last_90d, lf.failure_type_norm AS last_failure_type, lf.cause AS last_failure_cause,
         c.last_failure_date, DATEDIFF(DATE'{END}', c.last_failure_date) AS days_since_last_failure,
         COALESCE(r.repair_cost_12m, 0) AS repair_cost_12m, COALESCE(l.lost_12m, 0) AS downhole_lost_oil_m3_12m
  FROM cur c LEFT JOIN lastf lf USING (uwi) LEFT JOIN nm n USING (uwi) LEFT JOIN lastm m USING (uwi)
  LEFT JOIN rep r USING (uwi) LEFT JOIN lost l USING (uwi))
SELECT ROW_NUMBER() OVER (ORDER BY failures_last_90d DESC, repair_cost_12m + downhole_lost_oil_m3_12m * 6.2898 * {NETBACK} DESC, uwi) AS priority_rank,
       uwi, COALESCE(well_name, uwi) AS well_name, field_operating_area, facility_name, pump_make,
       CASE WHEN failures_last_90d >= 3 THEN '3+ failures' WHEN failures_last_90d = 2 THEN '2 failures' ELSE '1 failure' END AS recency_tier,
       failures_last_90d, last_failure_type, last_failure_date, days_since_last_failure,
       ROUND(repair_cost_12m) AS repair_cost_12m, ROUND(downhole_lost_oil_m3_12m, 1) AS downhole_lost_oil_m3_12m,
       ROUND(repair_cost_12m + downhole_lost_oil_m3_12m * 6.2898 * {NETBACK}) AS cost_12m,
       CASE WHEN last_failure_type = 'Rods' AND last_failure_cause IN ('Fatigue', 'Corrosion') THEN 'Rod string upgrade or corrosion inhibition'
            WHEN last_failure_type = 'Rods' THEN 'Rod failure root-cause review'
            WHEN last_failure_type IN ('Pump', 'Tubing') THEN 'Pump and tubing failure review'
            ELSE 'Root-cause review' END AS suggested_action,
       CASE WHEN failures_last_90d >= 2 THEN 1 ELSE 0 END AS on_watchlist,
       CASE WHEN failures_last_90d >= 3 THEN 1 ELSE 0 END AS three_plus,
       CASE WHEN failures_last_90d >= 2 THEN ROUND(repair_cost_12m) ELSE 0 END AS watchlist_repair_cost_12m
FROM base"""),

    "ds_p4_cost": ("Cost per failure by type", f"""
WITH f AS (
  SELECT f.failure_type_norm AS failure_type,
         f.linked_job_cost / COUNT(*) OVER (PARTITION BY f.job_id) AS repair_cost,
         COALESCE(d.downhole_lost_oil_m3, 0) * 6.2898 * {NETBACK} AS lost_oil_value
  FROM {S}.silver_failures f LEFT JOIN {S}.gold_failure_downtime d USING (failure_id)
  WHERE f.failure_start_date >= DATE'{LOG_START}' AND f.linked_job_cost > 0 AND f.failure_type_norm IN ('Rods', 'Pump', 'Tubing'))
SELECT failure_type, COUNT(*) AS failures,
       ROUND(AVG(repair_cost)) AS mean_repair_cost,
       ROUND(AVG(lost_oil_value)) AS mean_lost_oil_value,
       ROUND(AVG(repair_cost + lost_oil_value)) AS mean_cost_per_failure,
       ROUND(MEDIAN(repair_cost + lost_oil_value)) AS median_cost_per_failure,
       ROUND(SUM(repair_cost + lost_oil_value)) AS total_cost
FROM f GROUP BY failure_type"""),

    "ds_p4_kpi": ("Payback headline (base case)", f"""
SELECT ROUND(MIN(CASE WHEN avoided_share = 0.5 THEN payback_years END), 1) AS fastest_payback_50_years,
       ROUND(MIN(CASE WHEN avoided_share = 0.25 THEN payback_years END), 1) AS fastest_payback_25_years,
       SUM(CASE WHEN avoided_share = 0.5 THEN bad_actor_wells END) AS bad_actor_wells
FROM {S}.gold_payback WHERE is_base_case"""),

    "ds_p4_actions": ("Break-even and payback per action (base case)", f"""
SELECT action,
       MAX(bad_actor_wells) AS bad_actor_wells,
       MAX(bad_actor_rule) AS bad_actor_rule,
       ROUND(MAX(failures_per_well_year), 2) AS failures_per_well_year,
       ROUND(MAX(mean_cost_per_failure)) AS mean_cost_per_failure,
       ROUND(MAX(action_cost)) AS action_cost,
       ROUND(MAX(failures_to_recover_cost), 2) AS failures_to_recover_cost,
       ROUND(MAX(CASE WHEN avoided_share = 0.25 THEN annual_savings_per_well END)) AS savings_25,
       ROUND(MAX(CASE WHEN avoided_share = 0.5 THEN annual_savings_per_well END)) AS savings_50,
       ROUND(MAX(CASE WHEN avoided_share = 0.25 THEN payback_years END), 2) AS payback_25_years,
       ROUND(MAX(CASE WHEN avoided_share = 0.5 THEN payback_years END), 2) AS payback_50_years
FROM {S}.gold_payback WHERE is_base_case GROUP BY action"""),

    "ds_p4_grid": ("Payback sensitivity grid", f"""
SELECT action, netback_cad_per_bbl, ROUND(action_cost) AS action_cost,
       CAST(ROUND(100 * avoided_share) AS INT) AS avoided_share_pct,
       ROUND(mean_cost_per_failure) AS mean_cost_per_failure,
       ROUND(annual_savings_per_well) AS annual_savings_per_well,
       ROUND(payback_years, 2) AS payback_years
FROM {S}.gold_payback"""),

    "ds_p5_kpi": ("Ranking validation headline", f"""
SELECT ROUND(100 * capture, 1) AS capture_10_pct,
       ROUND(lift, 2) AS lift_vs_random,
       CONCAT(ROUND(100 * ci_lo, 1), '% to ', ROUND(100 * ci_hi, 1), '%') AS interval_95,
       ROUND(100 * persistence_top10, 1) AS persistence_pct
FROM {S}.gold_ranking_validation
WHERE ranker = 'Cost + recent' AND target = 'cost' AND k = 0.1"""),

    "ds_p5_curve": ("Share of later cost captured, by ranker", f"""
SELECT CAST(ROUND(100 * k) AS INT) AS share_targeted_pct, ranker, ROUND(100 * capture, 1) AS share_captured_pct
FROM {S}.gold_ranking_validation WHERE target = 'cost'"""),

    "ds_p5_watch": ("Recency watchlist precision and recall", f"""
WITH e AS (SELECT failures_prior_90, failed_next_90 FROM {S}.gold_recency_tier WHERE NOT is_current),
f AS (SELECT explode(array(1, 2, 3)) AS k)
SELECT CONCAT(k, '+ failures in last 90 days') AS flag,
       ROUND(100 * AVG(CASE WHEN failures_prior_90 >= k THEN failed_next_90 END), 1) AS precision_pct,
       ROUND(100 * SUM(CASE WHEN failures_prior_90 >= k AND failed_next_90 = 1 THEN 1 ELSE 0 END) / SUM(failed_next_90), 1) AS recall_pct
FROM e CROSS JOIN f GROUP BY k"""),

    "ds_p6_clean": ("Cleaning rules and rows changed (latest run)", f"""
WITH l AS (SELECT *, MAX(run_at) OVER (PARTITION BY target) AS last_run FROM {S}.cleaning_log),
src AS (SELECT 'silver_daily' AS target, COUNT(*) AS source_rows FROM {S}.bronze_daily
        UNION ALL SELECT 'silver_jobs', COUNT(*) FROM {S}.bronze_jobs
        UNION ALL SELECT 'silver_failures', COUNT(*) FROM {S}.bronze_failures)
SELECT l.rule, l.target AS table_name, l.rows_affected,
       ROUND(100 * l.rows_affected / s.source_rows, 3) AS share_of_table_pct
FROM l JOIN src s USING (target)
WHERE l.run_at = l.last_run AND l.rows_affected > 0
  AND l.rule NOT LIKE '%linked to a repair job%' AND l.rule NOT LIKE '%no repair job%'
  AND l.rule NOT LIKE '%(kept)%' AND l.rule NOT LIKE '%dropped%'"""),
}


def run_sql(sql):
    body = {"warehouse_id": WAREHOUSE_ID, "wait_timeout": "50s", "statement": sql, "format": "JSON_ARRAY", "disposition": "INLINE"}
    f = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
    json.dump(body, f)
    f.close()
    try:
        r = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements/", "--profile", PROFILE, "--json", "@" + f.name],
                           capture_output=True, text=True)
    finally:
        os.unlink(f.name)
    j = json.loads(r.stdout)
    if j["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(json.dumps(j["status"])[:500])
    cols = [c["name"] for c in j["manifest"]["schema"].get("columns", [])]
    return cols, j.get("result", {}).get("data_array", []) or [], j["manifest"].get("total_row_count")


def test():
    ok = True
    for name, (_, sql) in DATASETS.items():
        try:
            cols, rows, n = run_sql(sql)
            print(f"PASS {name}: {n} rows | {cols}")
            for r in rows[:3]:
                print("     ", r)
        except Exception as e:
            ok = False
            print(f"FAIL {name}: {e}")
    return ok


def text(name, md, x, y, w, h):
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": [md]}}, "position": {"x": x, "y": y, "width": w, "height": h}}


def field(name, expr=None):
    return {"name": name, "expression": expr or f"`{name}`"}


def widget(name, ds, fields, spec, x, y, w, h, disaggregated=True):
    return {
        "widget": {
            "name": name,
            "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": fields, "disaggregated": disaggregated}}],
            "spec": spec,
        },
        "position": {"x": x, "y": y, "width": w, "height": h},
    }


def counter(name, ds, col, title, x, y, agg=None):
    fname = f"{agg.lower()}({col})" if agg else col
    f = field(fname, f"{agg}(`{col}`)") if agg else field(col)
    spec = {"version": 2, "widgetType": "counter",
            "encodings": {"value": {"fieldName": fname, "displayName": title}},
            "frame": {"showTitle": True, "title": title}}
    return widget(name, ds, [f], spec, x, y, 2, 3, disaggregated=agg is None)


def bar(name, ds, x_enc, y_enc, title, x, y, w, h, color=None, group=False, fields=None, disaggregated=True, desc=None):
    enc = {"x": x_enc, "y": y_enc}
    if color:
        enc["color"] = color
    spec = {"version": 3, "widgetType": "bar", "encodings": enc, "frame": {"showTitle": True, "title": title}}
    if desc:
        spec["frame"].update({"showDescription": True, "description": desc})
    if group:
        spec["mark"] = {"layout": "group"}
    return widget(name, ds, fields, spec, x, y, w, h, disaggregated)


def table(name, ds, cols, title, x, y, w, h):
    spec = {"version": 2, "widgetType": "table",
            "encodings": {"columns": [{"fieldName": c, "displayName": d} for c, d in cols]},
            "frame": {"showTitle": True, "title": title}}
    return widget(name, ds, [field(c) for c, _ in cols], spec, x, y, w, h)


def cat(f, label):
    return {"fieldName": f, "scale": {"type": "categorical"}, "displayName": label}


def qty(f, label):
    return {"fieldName": f, "scale": {"type": "quantitative"}, "displayName": label}


def header(prefix, heading, sentence):
    return [text(f"{prefix}-title", f"## {heading}", 0, 0, 6, 1), text(f"{prefix}-subtitle", sentence, 0, 1, 6, 1)]


def page(name, display, layout):
    return {"name": name, "displayName": display, "pageType": "PAGE_TYPE_CANVAS", "layout": layout}


def build():
    p1 = header("p1", "Where do the money and barrels go?",
                "Rods fail most often, but pump and tubing failures cost the most per repair. Spend also concentrates in a few areas. Last 12 months = Oct 28, 2025 to Oct 27, 2026.") + [
        counter("p1-spend", "ds_p1_kpis", "repair_cost_12m", "Repair spend, last 12 months (C$)", 0, 2, "SUM"),
        counter("p1-lost", "ds_p1_kpis", "downhole_lost_oil_m3_12m", "Downhole lost oil, last 12 months (m³)", 2, 2, "SUM"),
        counter("p1-lost-value", "ds_p1_kpis", "downhole_lost_oil_value_12m", "Value of that lost oil (C$, at C$32.50/boe)", 4, 2, "SUM"),
        bar("p1-loss-month", "ds_p1_loss_month",
            {"fieldName": "month", "scale": {"type": "temporal"}, "displayName": "Month"},
            qty("sum(lost_oil_m3)", "Lost oil (m³)"), "Lost oil by cause, by month (economic shut-ins excluded)", 0, 5, 4, 6,
            color=cat("downtime_bucket", "Cause"),
            fields=[field("month"), field("downtime_bucket"), field("sum(lost_oil_m3)", "SUM(`lost_oil_m3`)")], disaggregated=False),
        bar("p1-type-share", "ds_p1_type_share", cat("failure_type", "Failure type"),
            {"scale": {"type": "quantitative"}, "fields": [
                {"fieldName": "share_of_failures_pct", "displayName": "% of failures"},
                {"fieldName": "share_of_repair_cost_pct", "displayName": "% of repair cost"}]},
            "Rods are the count, pump and tubing the dollars (%, since Jun 2025)", 4, 5, 2, 6, group=True,
            fields=[field("failure_type"), field("share_of_failures_pct"), field("share_of_repair_cost_pct")]),
        bar("p1-repair-area", "ds_p1_repair_area", qty("sum(repair_cost)", "Repair spend (C$)"), cat("field_operating_area", "Area"),
            "Repair spend concentrates in a few areas (last 12 months, C$)", 0, 11, 6, 7, color=cat("job_category", "Job type"),
            fields=[field("field_operating_area"), field("job_category"), field("sum(repair_cost)", "SUM(`repair_cost`)")], disaggregated=False),
        text("p1-next", "*Next: Failures repeat*", 0, 18, 6, 1),
    ]

    p2 = header("p2", "Do failures come back?",
                "More than half of failures are followed by another on the same well within 90 days. Rod failures come back two times in three. Failure log is reliable from Jun 2025.") + [
        counter("p2-all", "ds_p2_counters", "refail_90d_all_pct", "Failures followed by another within 90 days (%)", 0, 2),
        counter("p2-rods", "ds_p2_counters", "refail_90d_rods_pct", "Rod failures that repeat within 90 days (%)", 2, 2),
        counter("p2-pt", "ds_p2_counters", "refail_90d_pump_tubing_pct", "Pump and tubing failures that repeat within 90 days (%)", 4, 2),
        bar("p2-refail-type", "ds_p2_refail_type", cat("failure_type", "Failure type"),
            {"scale": {"type": "quantitative"}, "fields": [
                {"fieldName": "refail_30d_pct", "displayName": "Within 30 days (%)"},
                {"fieldName": "refail_90d_pct", "displayName": "Within 90 days (%)"}]},
            "Two in three rod failures repeat within 90 days", 0, 5, 3, 6, group=True,
            fields=[field("failure_type"), field("refail_30d_pct"), field("refail_90d_pct")]),
        bar("p2-gap", "ds_p2_gap", cat("days_to_next_failure", "Days to the next failure"), qty("failures", "Failures"),
            "Repeat failures come back in about six weeks", 3, 5, 3, 6,
            fields=[field("days_to_next_failure"), field("failures")]),
        bar("p2-monthly", "ds_p2_monthly", {"fieldName": "month", "scale": {"type": "temporal"}, "displayName": "Month"},
            qty("sum(failures)", "Failures"), "Failures run steady at about 170 a month", 0, 11, 2, 6, color=cat("failure_type", "Type"),
            fields=[field("month"), field("failure_type"), field("sum(failures)", "SUM(`failures`)")], disaggregated=False),
        bar("p2-tier", "ds_p2_tier", cat("tier", "Failures in the last 90 days"), qty("p_fail_next_90d_pct", "Fail again within 90 days (%)"),
            "Each recent failure raises the odds of the next (%)", 2, 11, 2, 6,
            fields=[field("tier"), field("p_fail_next_90d_pct")]),
        bar("p2-chronic", "ds_p2_chronic", cat("failures_per_well", "Failures per well since Jun 2025"),
            {"scale": {"type": "quantitative"}, "fields": [
                {"fieldName": "share_of_wells_pct", "displayName": "% of wells"},
                {"fieldName": "share_of_failures_pct", "displayName": "% of failures"}]},
            "Most failures come from wells that fail again (%)", 4, 11, 2, 6, group=True,
            fields=[field("failures_per_well"), field("share_of_wells_pct"), field("share_of_failures_pct")]),
        text("p2-next", "*Next: This week's work list*", 0, 17, 6, 1),
    ]

    p3 = header("p3", "Which wells do we act on this week?",
                "Wells with two or more failures in the last 90 days are the watchlist (64% fail again within 90 days). The list is sorted by recent failures, then by 12-month cost. Data as of Oct 27, 2026.") + [
        counter("p3-watch", "ds_p3_list", "on_watchlist", "Wells on the watchlist (2+ failures)", 0, 2, "SUM"),
        counter("p3-three", "ds_p3_list", "three_plus", "Wells with 3+ failures", 2, 2, "SUM"),
        counter("p3-cost", "ds_p3_list", "watchlist_repair_cost_12m", "Repair cost on watchlist wells, last 12 months (C$)", 4, 2, "SUM"),
        bar("p3-area", "ds_p3_list", qty("count(uwi)", "Wells"), cat("field_operating_area", "Area"),
            "Where wells with a recent failure sit, by failures in the last 90 days", 0, 5, 6, 6, color=cat("recency_tier", "Recent failures"),
            fields=[field("field_operating_area"), field("recency_tier"), field("count(uwi)", "COUNT(`uwi`)")], disaggregated=False),
        table("p3-list", "ds_p3_list", [
            ("priority_rank", "Rank"), ("well_name", "Well"), ("field_operating_area", "Area"), ("facility_name", "Facility"),
            ("pump_make", "Pump make"), ("recency_tier", "Tier"), ("failures_last_90d", "Failures, last 90 days"),
            ("last_failure_type", "Last failure"), ("last_failure_date", "Last failure date"), ("days_since_last_failure", "Days since"),
            ("repair_cost_12m", "Repair cost, 12 months (C$)"), ("downhole_lost_oil_m3_12m", "Lost oil, 12 months (m³)"),
            ("cost_12m", "Cost incl. lost oil, 12 months (C$)"), ("suggested_action", "Suggested action")],
            "Ranked work list (sort by Rank)", 0, 11, 6, 10),
        text("p3-next", "*Next: What acting pays back*", 0, 21, 6, 1),
    ]

    p4 = header("p4", "What does acting pay back?",
                "Two actions are tested on the wells they fit, with a range because we cannot prove how many failures they avoid.") + [
        counter("p4-fast50", "ds_p4_kpi", "fastest_payback_50_years", "Fastest payback if 50% of failures are avoided (years)", 0, 2),
        counter("p4-fast25", "ds_p4_kpi", "fastest_payback_25_years", "Fastest payback if 25% are avoided (years)", 2, 2),
        counter("p4-wells", "ds_p4_kpi", "bad_actor_wells", "Bad-actor wells the two actions target", 4, 2),
        bar("p4-savings", "ds_p4_actions", cat("action", "Action"),
            {"scale": {"type": "quantitative"}, "fields": [
                {"fieldName": "savings_25", "displayName": "25% fewer failures (C$/yr)"},
                {"fieldName": "savings_50", "displayName": "50% fewer failures (C$/yr)"}]},
            "Yearly savings per bad-actor well against a one-off C$34,152 action", 0, 5, 3, 6, group=True,
            fields=[field("action"), field("savings_25"), field("savings_50")]),
        table("p4-breakeven", "ds_p4_actions", [
            ("action", "Action"), ("bad_actor_wells", "Bad-actor wells"), ("failures_per_well_year", "Failures/yr after picking"),
            ("mean_cost_per_failure", "Mean cost per failure (C$)"), ("failures_to_recover_cost", "Failures to avoid to pay back"),
            ("payback_25_years", "Payback at 25% (yrs)"), ("payback_50_years", "Payback at 50% (yrs)")],
            "Break-even per action (base case)", 3, 5, 3, 6),
        table("p4-grid", "ds_p4_grid", [
            ("action", "Action"), ("netback_cad_per_bbl", "Netback (C$/bbl)"), ("action_cost", "Action cost (C$)"),
            ("avoided_share_pct", "Failures avoided (%)"), ("mean_cost_per_failure", "Cost per failure (C$)"),
            ("annual_savings_per_well", "Savings per well (C$/yr)"), ("payback_years", "Payback (yrs)")],
            "Sensitivity: netback ±25%, action cost ×0.5 / ×1 / ×2, 25% or 50% fewer failures", 0, 11, 6, 7),
        bar("p4-cost", "ds_p4_cost", cat("failure_type", "Failure type"),
            {"scale": {"type": "quantitative"}, "fields": [
                {"fieldName": "mean_repair_cost", "displayName": "Repair job (C$)"},
                {"fieldName": "mean_lost_oil_value", "displayName": "Lost oil (C$)"}]},
            "Mean cost per failure, since Jun 2025 (C$)", 0, 18, 3, 6,
            fields=[field("failure_type"), field("mean_repair_cost"), field("mean_lost_oil_value")]),
        table("p4-cost-table", "ds_p4_cost", [
            ("failure_type", "Type"), ("failures", "Failures"), ("mean_cost_per_failure", "Mean cost (C$)"),
            ("median_cost_per_failure", "Median cost (C$)"), ("total_cost", "Total (C$)")],
            "Cost per failure by type", 3, 18, 3, 6),
        text("p4-budget", "**Budget scenarios: pending** (task n4, `gold_budget_scenarios`): which wells to treat for 25 / 50 / 100 / 200-well budgets.", 0, 24, 6, 1),
        text("p4-inputs", "**Inputs and where they come from.** Netback C$32.50 per boe from Strathcona Q3 2026 operating earnings (company-wide). Repair cost uses field estimates, not invoices; a repair job shared by two failures is split between them. Lost oil counts from the day the well stopped. Bad actors are picked on Jun–Dec 2025 failures and their rate is measured on Jan–Oct 2026. Action cost is assumed from the 2023+ Workover median because no regional price was found. Avoided shares of 25% and 50% are assumptions bracketing one vendor case (~34%); action effects are associations, not proven causes.", 0, 25, 6, 2),
        text("p4-next", "*Next: Does the ranking hold up*", 0, 27, 6, 1),
    ]

    p5 = header("p5", "Does the ranking hold up on data it has not seen?",
                "Wells are ranked on 2023-24 history plus Mar-May 2025 repairs and tested on Jun 2025 to Oct 2026 cost. A sensor rule only counts if it beats the simple recency watchlist.") + [
        counter("p5-capture", "ds_p5_kpi", "capture_10_pct", "Later cost caught by the top 10% of wells (%, 95% CI 14.3-19.9)", 0, 2),
        counter("p5-lift", "ds_p5_kpi", "lift_vs_random", "Lift vs random", 2, 2),
        counter("p5-persist", "ds_p5_kpi", "persistence_pct", "Top-10% wells still top 10% later (%)", 4, 2),
        {"widget": {"name": "p5-curve",
                    "queries": [{"name": "main_query", "query": {"datasetName": "ds_p5_curve", "fields": [
                        field("share_targeted_pct"), field("ranker"), field("share_captured_pct")], "disaggregated": True}}],
                    "spec": {"version": 3, "widgetType": "line", "encodings": {
                        "x": qty("share_targeted_pct", "Share of wells targeted (%)"),
                        "y": qty("share_captured_pct", "Share of later cost captured (%)"),
                        "color": cat("ranker", "Ranking")},
                        "frame": {"showTitle": True, "title": "Ranking with recent repairs edges out size; neither clearly wins"}}},
         "position": {"x": 0, "y": 5, "width": 4, "height": 6}},
        bar("p5-watch", "ds_p5_watch", cat("flag", "Watchlist rule"),
            {"scale": {"type": "quantitative"}, "fields": [
                {"fieldName": "precision_pct", "displayName": "Precision (%)"},
                {"fieldName": "recall_pct", "displayName": "Recall (%)"}]},
            "Recency watchlist: the baseline sensor rules must beat", 4, 5, 2, 6, group=True,
            fields=[field("flag"), field("precision_pct"), field("recall_pct")]),
        text("p5-pending", "**Sensor rules vs the watchlist: pending** (tasks s6, s9 in `04a_early_warning`). Precision = share of flagged wells that fail again within 90 days; recall = share of later failures that were flagged.", 0, 11, 6, 2),
        text("p5-next", "*Next: Can we trust the data*", 0, 13, 6, 1),
    ]

    p6 = header("p6", "Can we trust the data?",
                "We cleaned the raw files, logged every rule, and list what the data cannot tell us.") + [
        counter("p6-rows", "ds_p6_clean", "rows_affected", "Rows changed by cleaning rules", 0, 2, "SUM"),
        text("p6-limits", "**What this data cannot tell us.** The failure log starts in Jun 2025. Rod truck cost is missing before 2023. Costs are field estimates, not invoiced costs. Lost oil is an operator estimate. Torque and current repeat day to day (manual entries). The netback is company-wide, not heavy-oil only. Action effects are associations, not proven causes.", 2, 2, 4, 3),
        bar("p6-rules", "ds_p6_clean", qty("rows_affected", "Rows affected"), cat("rule", "Rule"),
            "Where cleaning changed the most rows", 0, 5, 6, 7, fields=[field("rule"), field("rows_affected")]),
        table("p6-log", "ds_p6_clean", [("rule", "Rule"), ("table_name", "Table"), ("rows_affected", "Rows affected"),
                                         ("share_of_table_pct", "Share of source rows (%)")], "Cleaning log (latest run)", 0, 12, 6, 7),
        text("p6-end", "*End of dashboard*", 0, 19, 6, 1),
    ]

    area_ds = ["ds_p1_kpis", "ds_p1_loss_month", "ds_p1_repair_area", "ds_p2_monthly", "ds_p3_list"]
    filt = {"widget": {"name": "filter-area",
                       "queries": [{"name": f"q_{d}_area", "query": {"datasetName": d, "fields": [field("field_operating_area")], "disaggregated": False}} for d in area_ds],
                       "spec": {"version": 2, "widgetType": "filter-multi-select",
                                "encodings": {"fields": [{"fieldName": "field_operating_area", "displayName": "Operating area", "queryName": f"q_{d}_area"} for d in area_ds]},
                                "frame": {"showTitle": True, "title": "Operating area"}}},
            "position": {"x": 0, "y": 0, "width": 2, "height": 2}}

    return {
        "datasets": [{"name": n, "displayName": d, "queryLines": [l + "\n" for l in sql.strip().split("\n")]} for n, (d, sql) in DATASETS.items()],
        "pages": [
            page("p1", "1 · Where the money goes", p1),
            page("p2", "2 · Failures repeat", p2),
            page("p3", "3 · This week's work list", p3),
            page("p4", "4 · What acting pays back", p4),
            page("p5", "5 · Does the ranking hold up", p5),
            page("p6", "6 · Can we trust the data", p6),
            {"name": "filters", "displayName": "Filters", "pageType": "PAGE_TYPE_GLOBAL_FILTERS", "layout": [filt]},
        ],
    }


def check_layout(d):
    problems = []
    for p in d["pages"]:
        if p["pageType"] != "PAGE_TYPE_CANVAS":
            continue
        rows = {}
        for item in p["layout"]:
            pos = item["position"]
            rows.setdefault(pos["y"], []).append((pos["x"], pos["width"], pos["height"]))
        for y, cells in rows.items():
            cells.sort()
            x = 0
            for cx, w, _ in cells:
                if cx != x:
                    problems.append(f"{p['name']} y={y}: gap at x={x}")
                x = cx + w
            if x != 6:
                problems.append(f"{p['name']} y={y}: row width {x}, expected 6")
        for item in p["layout"]:
            w = item["widget"]
            if "spec" in w:
                fields = {f["name"] for q in w["queries"] for f in q["query"]["fields"]}
                enc = json.dumps(w["spec"].get("encodings", {}))
                for fn in __import__("re").findall(r'"fieldName": "([^"]+)"', enc):
                    if fn not in fields:
                        problems.append(f"{w['name']}: encoding {fn} not in query fields")
    return problems


if __name__ == "__main__":
    if sys.argv[1:] == ["test"]:
        sys.exit(0 if test() else 1)
    if sys.argv[1:] == ["build"]:
        d = build()
        probs = check_layout(d)
        for pr in probs:
            print("LAYOUT", pr)
        with open(OUT, "w", encoding="utf8", newline="\n") as fh:
            json.dump(d, fh, indent=1, ensure_ascii=False)
        print(f"wrote {OUT}: {len(d['datasets'])} datasets, {sum(len(p['layout']) for p in d['pages'])} widgets")
        sys.exit(1 if probs else 0)
