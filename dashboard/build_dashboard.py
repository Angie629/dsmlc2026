"""Build the Reliability Investment Dashboard (dashboard/Reliability Investment Dashboard.lvdash.json).

Usage:
    python dashboard/build_dashboard.py test     # run every dataset query on the SQL warehouse
    python dashboard/build_dashboard.py build    # write dashboard/Reliability Investment Dashboard.lvdash.json

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
GRID = 12
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Reliability Investment Dashboard.lvdash.json")

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

    "ds_p2_refail_type": ("Re-failure by days since failure", f"""
WITH e AS (SELECT failure_type_norm AS failure_type, days_to_next AS d FROM {S}.gold_failure_gap
           WHERE failure_start_date BETWEEN DATE'2025-06-01' AND DATE'2026-07-28' AND failure_type_norm IN ('Rods', 'Pump', 'Tubing')),
g AS (SELECT d, CASE WHEN failure_type = 'Rods' THEN 'Rods' ELSE 'Pump and tubing' END AS failure_group FROM e
      UNION ALL SELECT d, 'All failures' FROM e)
SELECT failure_group, day AS days_since_failure,
       ROUND(100 * AVG(CASE WHEN d IS NOT NULL AND d <= day THEN 1.0 ELSE 0.0 END), 1) AS share_repeated_pct
FROM g CROSS JOIN (SELECT explode(sequence(0, 90)) AS day) days
GROUP BY 1, 2"""),

    "ds_p2_gap": ("First-time vs repeat failures per month", f"""
WITH f AS (SELECT field_operating_area, failure_type_norm AS failure_type, failure_start_date AS failure_date,
                  LAG(failure_start_date) OVER (PARTITION BY uwi ORDER BY failure_start_date) AS prev_failure_date
           FROM {S}.gold_failure_gap WHERE failure_start_date >= DATE'{LOG_START}')
SELECT date_trunc('MONTH', failure_date) AS month, field_operating_area, failure_type,
       CASE WHEN prev_failure_date IS NOT NULL AND datediff(failure_date, prev_failure_date) <= 90
            THEN 'Repeat (within 90 days of the last failure)' ELSE 'First failure, or over 90 days since the last' END AS failure_kind,
       COUNT(*) AS failures
FROM f WHERE failure_date >= DATE'2025-09-01'
GROUP BY 1, 2, 3, 4"""),

    "ds_p2_monthly": ("Failures per month by type", f"""
SELECT DATE_TRUNC('MONTH', failure_start_date) AS month,
       field_operating_area,
       CASE WHEN failure_type_norm IN ('Rods', 'Pump', 'Tubing') THEN failure_type_norm ELSE 'Other' END AS failure_type,
       COUNT(*) AS failures
FROM {S}.gold_failure_gap WHERE in_log_window
GROUP BY ALL"""),

    "ds_p2_tier": ("Chance of failure in the next 90 days by recent failures", f"""
SELECT tier, ROUND(100 * AVG(failed_next_90), 1) AS p_fail_next_90d_pct, COUNT(*) AS well_snapshots
FROM {S}.gold_recency_tier WHERE NOT is_current
GROUP BY tier ORDER BY tier"""),

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

    "ds_p3_alerts": ("Current runtime alerts (last 14 days of data)", f"""
WITH a AS (SELECT * FROM {S}.gold_alerts WHERE is_current),
nm AS (SELECT uwi, MAX(well_name) AS well_name FROM {S}.silver_daily WHERE production_date >= DATE'2026-10-01' GROUP BY uwi),
t AS (SELECT uwi, failures_prior_90 FROM {S}.gold_recency_tier WHERE is_current)
SELECT a.alert_date, a.uwi, COALESCE(nm.well_name, a.uwi) AS well_name, a.field_operating_area,
       CAST(a.partial_7d AS INT) AS partial_days_last_7,
       COALESCE(t.failures_prior_90, 0) AS failures_last_90d,
       CASE WHEN COALESCE(t.failures_prior_90, 0) >= 2 THEN 'Yes' ELSE 'No' END AS on_watchlist
FROM a LEFT JOIN nm USING (uwi) LEFT JOIN t USING (uwi)"""),

    "ds_p5_rules": ("Runtime rule vs recency watchlist, 2026 holdout", f"""
SELECT rule_name,
       ROUND(100 * precision, 1) AS precision_pct,
       ROUND(100 * recall, 1) AS recall_pct,
       alerts, alerts_per_area_week, median_lead_days, failures_caught, label_failures
FROM {S}.gold_warning_eval WHERE window = '2026 holdout'"""),

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

    "ba232594": ("Budget Summary", f"""
SELECT scenario AS `Budget Scenario`,
       budget_wells AS `Wells Treated`,
       ROUND(action_cost_total, 0) AS `Total Budget Cost ($)`,
       wells_action_1 AS `Action 1 Wells`,
       wells_action_2 AS `Action 2 Wells`,
       ROUND(failures_per_well_year_expected, 2) AS `Expected Failures / Well-Yr`,
       ROUND(failures_per_well_year_measured, 2) AS `Measured Failures / Well-Yr`,
       expected_savings_per_year_25 AS `Expected Annual Savings @ 25% ($)`,
       measured_savings_per_year_25 AS `Measured Annual Savings @ 25% ($)`,
       ROUND(payback_years_measured_25, 2) AS `Measured Payback @ 25% (Years)`,
       expected_savings_per_year_50 AS `Expected Annual Savings @ 50% ($)`,
       measured_savings_per_year_50 AS `Measured Annual Savings @ 50% ($)`,
       ROUND(payback_years_measured_50, 2) AS `Measured Payback @ 50% (Years)`,
       ROUND(expected_savings_per_year_25 - action_cost_total, 0) AS `Expected Year-1 Net Savings @ 25% ($)`,
       ROUND(expected_savings_per_year_50 - action_cost_total, 0) AS `Expected Year-1 Net Savings @ 50% ($)`,
       ROUND(measured_savings_per_year_25 - action_cost_total, 0) AS `Measured Year-1 Net Savings @ 25% ($)`,
       ROUND(measured_savings_per_year_50 - action_cost_total, 0) AS `Measured Year-1 Net Savings @ 50% ($)`,
       pick_window AS `Pick Window`,
       measure_window AS `Measure Window`
FROM {S}.gold_budget_scenarios
ORDER BY budget_wells"""),

    "35e45ef4": ("Budget Wells", f"""
SELECT s.scenario AS `Budget Scenario`,
       s.budget_wells AS `Scenario Well Limit`,
       w.rank AS `Priority Rank`,
       w.uwi AS `UWI`,
       w.action AS `Recommended Action`,
       w.failures_pick AS `Historical Failures`,
       ROUND(w.expected_rate, 2) AS `Failures / Yr`,
       ROUND(w.failures_pick * w.cost_per_failure) AS `Historical Failure Cost ($)`,
       ROUND(0.25 * w.expected_value_per_year) AS `Expected Annual Savings @ 25% ($)`,
       ROUND(0.5 * w.expected_value_per_year) AS `Expected Annual Savings @ 50% ($)`
FROM {S}.gold_budget_scenarios s
JOIN {S}.gold_budget_wells w ON w.rank <= s.budget_wells
ORDER BY s.budget_wells, w.rank"""),
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


def counter(name, ds, col, title, x, y, agg=None, w=4, h=2):
    fname = f"{agg.lower()}({col})" if agg else col
    f = field(fname, f"{agg}(`{col}`)") if agg else field(col)
    spec = {"version": 2, "widgetType": "counter",
            "encodings": {"value": {"fieldName": fname, "displayName": title}},
            "frame": {"showTitle": True, "title": title}}
    return widget(name, ds, [f], spec, x, y, w, h, disaggregated=agg is None)


def bar(name, ds, x_enc, y_enc, title, x, y, w, h, color=None, group=False, fields=None, disaggregated=True, desc=None, kind="bar"):
    enc = {"x": x_enc, "y": y_enc}
    if color:
        enc["color"] = color
    spec = {"version": 3, "widgetType": kind, "encodings": enc, "frame": {"showTitle": True, "title": title}}
    if desc:
        spec["frame"].update({"showDescription": True, "description": desc})
    if group:
        spec["mark"] = {"layout": "group"}
    return widget(name, ds, fields, spec, x, y, w, h, disaggregated)


def table(name, ds, cols, title, x, y, w, h):
    """cols: (field, label) or (field, label, extra column settings such as format or style)."""
    spec = {"version": 2, "widgetType": "table",
            "encodings": {"columns": [{"fieldName": c[0], "displayName": c[1], **(c[2] if len(c) > 2 else {})} for c in cols]},
            "frame": {"showTitle": True, "title": title}}
    return widget(name, ds, [field(c[0]) for c in cols], spec, x, y, w, h)


def merge(dst, patch):
    for k, v in patch.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            merge(dst[k], v)
        else:
            dst[k] = v
    return dst


def ui(item, spec=None, query=None, boxed_title=True):
    """Apply styling set in the dashboard editor; the editor stores titles as {value, fields}."""
    w = item["widget"]
    w["spec"]["data"] = {"queryName": "main_query"}
    if boxed_title:
        w["spec"]["frame"]["title"] = {"value": w["spec"]["frame"]["title"], "fields": []}
    merge(w["spec"], spec or {})
    merge(w["queries"][0]["query"], query or {})
    return item


def cat(f, label):
    return {"fieldName": f, "scale": {"type": "categorical"}, "displayName": label}


def qty(f, label):
    return {"fieldName": f, "scale": {"type": "quantitative"}, "displayName": label}


def theme(position):
    return {"themeColorType": "visualizationColors", "position": position}


USD_COMPACT = {"type": "number-currency", "currencyCode": "USD", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 2}}


def budget_counter(name, col, x, w, fmt=None):
    value = {"fieldName": f"sum({col})"}
    if fmt:
        value["format"] = fmt
    spec = {"version": 2, "widgetType": "counter", "encodings": {"value": value}, "frame": {"showTitle": True, "title": col}}
    return ui(widget(name, "ba232594", [field(f"sum({col})", f"SUM(`{col}`)")], spec, x, 23, w, 3, disaggregated=False))


def subtitle(prefix, sentence):
    return text(f"{prefix}-subtitle", sentence, 0, 0, GRID, 1)


def page(name, display, layout):
    return {"name": name, "displayName": display, "pageType": "PAGE_TYPE_CANVAS", "layout": layout, "layoutVersion": "GRID_V1"}


WHOLE = {"type": "number-plain", "abbreviation": "none", "decimalPlaces": {"type": "max", "places": 0}}
NO_AXIS_TITLE = {"axis": {"hideTitle": True}}
LEGEND_BOTTOM = {"legend": {"position": "bottom"}}


def cents(group_sep=True):
    return {"type": "number-plain", "abbreviation": "none", "decimalPlaces": {"type": "exact", "places": 2}, "hideGroupSeparator": not group_sep}


CENTER = {"contentAlignment": "center"}


def build():
    p1 = [
        subtitle("p1", "Rods fail most often, but pump and tubing failures cost the most per repair. Spend also concentrates in a few areas. Last 12 months = Oct 28, 2025 to Oct 27, 2026."),
        counter("p1-spend", "ds_p1_kpis", "repair_cost_12m", "Repair spend, last 12 months (C$)", 0, 1, "SUM"),
        ui(counter("p1-lost", "ds_p1_kpis", "downhole_lost_oil_m3_12m", "Downhole lost oil, last 12 months (m³)", 4, 1, "SUM"),
           {"encodings": {"value": {"format": WHOLE}}}),
        counter("p1-lost-value", "ds_p1_kpis", "downhole_lost_oil_value_12m", "Value of that lost oil (C$, at C$32.50/boe)", 8, 1, "SUM"),
        ui(bar("p1-loss-month", "ds_p1_loss_month",
               {"fieldName": "month", "scale": {"type": "temporal"}, "displayName": "Month"},
               qty("sum(lost_oil_m3)", "Lost oil (m³)"), "Lost oil by cause, by month (economic shut-ins excluded)", 0, 3, 8, 6,
               color=cat("downtime_bucket", "Cause"),
               fields=[field("downtime_bucket"), field("month"), field("sum(lost_oil_m3)", "SUM(`lost_oil_m3`)")], disaggregated=False),
           {"encodings": {"x": NO_AXIS_TITLE, "y": {"format": WHOLE}, "color": LEGEND_BOTTOM}}),
        ui(bar("p1-type-share", "ds_p1_type_share", cat("failure_type", "Failure type"),
               {"scale": {"type": "quantitative"}, "fields": [
                   {"fieldName": "share_of_failures_pct", "displayName": "% of failures"},
                   {"fieldName": "share_of_repair_cost_pct", "displayName": "% of repair cost"}]},
               "Share of failures vs. repair cost by failure type (%)", 8, 3, 4, 6, group=True,
               fields=[field("failure_type"), field("share_of_failures_pct"), field("share_of_repair_cost_pct")]),
           {"encodings": {"x": NO_AXIS_TITLE}}),
        ui(bar("p1-repair-area", "ds_p1_repair_area", qty("sum(repair_cost)", "Repair spend (C$)"), cat("field_operating_area", "Area"),
               "Repair spend concentrates in a few areas (last 12 months, C$)", 0, 9, 12, 7, color=cat("job_category", "Job type"),
               fields=[field("job_category"), field("sum(repair_cost)", "SUM(`repair_cost`)"), field("field_operating_area")], disaggregated=False),
           {"encodings": {"y": {"scale": {"sort": {"by": "x-reversed"}}}}}),
    ]

    p2 = [
        subtitle("p2", "More than half of failures are followed by another on the same well within 90 days. Rod failures come back two times in three. Failure log is reliable from Jun 2025."),
        counter("p2-all", "ds_p2_counters", "refail_90d_all_pct", "Failures followed by another within 90 days (%)", 0, 1),
        counter("p2-rods", "ds_p2_counters", "refail_90d_rods_pct", "Rod failures that repeat within 90 days (%)", 4, 1),
        counter("p2-pt", "ds_p2_counters", "refail_90d_pump_tubing_pct", "Pump and tubing failures that repeat within 90 days (%)", 8, 1),
        ui(bar("p2-refail-type", "ds_p2_refail_type", qty("days_since_failure", "Days since failure"), qty("share_repeated_pct", "Repeated (%)"),
               "Share of failures repeated by days since failure (%)", 0, 3, 6, 6, color=cat("failure_group", "Failure group"), kind="line",
               fields=[field("failure_group"), field("days_since_failure"), field("share_repeated_pct")]),
           boxed_title=False),
        ui(bar("p2-gap", "ds_p2_gap", {"fieldName": "month", "scale": {"type": "temporal"}, "displayName": "Month"},
               qty("sum(failures)", "Failures"), "First-time vs repeat failures per month", 6, 3, 6, 6, kind="line",
               color=cat("failure_kind", "Failure kind"),
               fields=[field("failure_kind"), field("month"), field("sum(failures)", "SUM(`failures`)")], disaggregated=False),
           {"encodings": {"x": NO_AXIS_TITLE, "color": {**LEGEND_BOTTOM, "scale": {"mappings": [
               {"color": "#94A3B8", "value": "First failure, or over 90 days since the last"},
               {"color": "#C2410C", "value": "Repeat (within 90 days of the last failure)"}]}}}}),
        ui(bar("p2-monthly", "ds_p2_monthly", {"fieldName": "month", "scale": {"type": "temporal"}, "displayName": "Month"},
               qty("sum(failures)", "Failures"), "Failures per month by type", 4, 9, 4, 6, color=cat("failure_type", "Type"),
               fields=[field("failure_type"), field("month"), field("sum(failures)", "SUM(`failures`)")], disaggregated=False),
           {"encodings": {"x": NO_AXIS_TITLE, "color": {"scale": {"mappings": [{"value": "Tubing", "color": theme(8)}]}}}}),
        ui(bar("p2-chronic", "ds_p2_chronic", cat("failures_per_well", "Failures per well since Jun 2025"),
               {"scale": {"type": "quantitative"}, "fields": [
                   {"fieldName": "share_of_wells_pct", "displayName": "% of wells"},
                   {"fieldName": "share_of_failures_pct", "displayName": "% of failures"}]},
               "Share of wells and failures by failures per well (%)", 8, 9, 4, 6, group=True,
               fields=[field("failures_per_well"), field("share_of_wells_pct"), field("share_of_failures_pct")]),
           {"mark": {"colors": [theme(1), theme(8)]}}),
        ui(bar("p2-prediction", "ds_p2_tier", cat("tier", "Failures in the last 90 days"), qty("p_fail_next_90d_pct", "Fail again within 90 days (%)"),
               "Chance of failure in the next 90 days by recent failures (%)", 0, 9, 4, 6,
               color={"fieldName": "tier", "scale": {"type": "categorical", "mappings": [
                   {"color": "#E5E7EB", "value": "0"}, {"color": "#FDBA74", "value": "1"},
                   {"color": "#EA580C", "value": "2"}, {"color": "#9A3412", "value": "3+"}]}},
               fields=[field("tier"), field("p_fail_next_90d_pct")]),
           {"encodings": {"x": {"scale": {"sort": {"by": "custom-order", "orderedValues": ["0", "1", "2", "3+"]}}}}}, boxed_title=False),
    ]

    p3 = [
        subtitle("p3", "Wells with two or more failures in the last 90 days are the watchlist (64% fail again within 90 days). The list is sorted by recent failures, then by 12-month cost. Data as of Oct 27, 2026."),
        counter("p3-watch", "ds_p3_list", "on_watchlist", "Wells on the watchlist (2+ failures)", 0, 1, "SUM"),
        counter("p3-three", "ds_p3_list", "three_plus", "Wells with 3+ failures", 4, 1, "SUM"),
        counter("p3-cost", "ds_p3_list", "watchlist_repair_cost_12m", "Repair cost on watchlist wells, last 12 months (C$)", 8, 1, "SUM"),
        ui(bar("p3-area", "ds_p3_list", qty("count(uwi)", "Wells"), cat("field_operating_area", "Area"),
               "Where wells with a recent failure sit, by failures in the last 90 days", 0, 3, 12, 6, color=cat("recency_tier", "Recent failures"),
               fields=[field("recency_tier"), field("count(uwi)", "COUNT(`uwi`)"), field("field_operating_area")], disaggregated=False),
           {"encodings": {"y": {"scale": {"sort": {"by": "x-reversed"}}}}}),
        ui(table("p3-list", "ds_p3_list", [
            ("priority_rank", "Rank"), ("uwi", "UWI", {"useForSearch": True}), ("well_name", "Well"), ("field_operating_area", "Area"),
            ("facility_name", "Facility"), ("pump_make", "Pump make"), ("recency_tier", "Tier"), ("failures_last_90d", "Failures, last 90 days"),
            ("last_failure_type", "Last failure"), ("last_failure_date", "Last failure date"), ("days_since_last_failure", "Days since"),
            ("repair_cost_12m", "Repair cost, 12 months (C$)"), ("downhole_lost_oil_m3_12m", "Lost oil, 12 months (m³)"),
            ("cost_12m", "Cost incl. lost oil, 12 months (C$)"), ("suggested_action", "Suggested action")],
            "Ranked work list (sort by Rank)", 0, 9, 12, 10)),
        table("p3-alerts", "ds_p3_alerts", [
            ("alert_date", "Alert date"), ("well_name", "Well"), ("field_operating_area", "Area"),
            ("partial_days_last_7", "Partial-run days, last 7"), ("failures_last_90d", "Failures, last 90 days"),
            ("on_watchlist", "Also on watchlist")],
            "Runtime alerts in the last 14 days (2+ partial-run days in a week; 17% are followed by a failure within 2-30 days)", 0, 19, 12, 6),
    ]

    budget_cols = ["Priority Rank", "UWI", "Recommended Action", "Historical Failures", "Failures / Yr",
                   "Historical Failure Cost ($)", "Expected Annual Savings @ 25% ($)", "Expected Annual Savings @ 50% ($)"]

    # Payback under 2 years in theme color 3, over 2 years in theme color 4 (set in the editor).
    payback_style = {"type": "basic", "rules": [
        {"condition": {"operand": {"type": "data-value", "value": "2"}, "operator": "<"}, "foreground": theme(3)},
        {"condition": {"operand": {"type": "data-value", "value": "2"}, "operator": ">"}, "foreground": theme(4)}]}
    p4 = [
        subtitle("p4", "Two actions are tested on the wells they fit, with a range because we cannot prove how many failures they avoid."),
        counter("p4-fast50", "ds_p4_kpi", "fastest_payback_50_years", "Fastest payback if 50% of failures are avoided (years)", 0, 1),
        ui(counter("p4-fast25", "ds_p4_kpi", "fastest_payback_25_years", "Fastest payback if 25% are avoided (years)", 4, 1)),
        counter("p4-wells", "ds_p4_kpi", "bad_actor_wells", "Bad-actor wells the two actions target", 8, 1),
        ui(bar("p4-savings", "ds_p4_actions", cat("action", "Action"),
               {"scale": {"type": "quantitative"}, "fields": [
                   {"fieldName": "savings_25", "displayName": "25% fewer failures (C$/yr)"},
                   {"fieldName": "savings_50", "displayName": "50% fewer failures (C$/yr)"}]},
               "Yearly savings per bad-actor well against a one-off C$34,152 action", 0, 3, 6, 6, group=True,
               fields=[field("action"), field("savings_25"), field("savings_50")]),
           {"encodings": {"x": {"axis": {"labelAngle": 0}}, "y": {**NO_AXIS_TITLE, "format": {
               "type": "number-currency", "currencyCode": "USD", "abbreviation": "none", "decimalPlaces": {"type": "max", "places": 0}}}},
            "mark": {"colors": [None, theme(8)]}}),
        ui(table("p4-breakeven", "ds_p4_actions", [
            ("action", "Action"), ("bad_actor_wells", "Bad-actor wells"), ("failures_per_well_year", "Failures/yr after picking"),
            ("mean_cost_per_failure", "Mean cost per failure (C$)"), ("failures_to_recover_cost", "Failures to avoid to pay back"),
            ("payback_25_years", "Payback at 25% (yrs)"), ("payback_50_years", "Payback at 50% (yrs)")],
            "Break-even per action (base case)", 0, 9, 12, 3)),
        ui(table("p4-grid", "ds_p4_grid", [
            ("action", "Action"), ("netback_cad_per_bbl", "Netback (C$/bbl)", {**CENTER, "format": cents(group_sep=False)}),
            ("action_cost", "Action cost (C$)", {**CENTER, "format": cents()}),
            ("avoided_share_pct", "Failures avoided (%)", CENTER),
            ("mean_cost_per_failure", "Cost per failure (C$)", {**CENTER, "format": cents()}),
            ("annual_savings_per_well", "Savings per well (C$/yr)", {**CENTER, "format": cents()}),
            ("payback_years", "Payback (yrs)", {**CENTER, "style": payback_style, "format": {
                "type": "number-plain", "abbreviation": "none", "decimalPlaces": {"type": "exact", "places": 1}, "hideGroupSeparator": True}})],
            "Sensitivity: netback ±25%, action cost ×0.5 / ×1 / ×2, 25% or 50% fewer failures", 0, 15, 12, 7),
           {"encodings": {"rowOrder": [{"by": "column-reversed", "field": {"index": 5}}]}},
           query={"orders": [{"direction": "DESC", "expression": "`annual_savings_per_well`"}]}),
        ui(bar("p4-cost", "ds_p4_cost", cat("failure_type", "Failure type"),
               {"scale": {"type": "quantitative"}, "fields": [
                   {"fieldName": "mean_repair_cost", "displayName": "Repair job (C$)"},
                   {"fieldName": "mean_lost_oil_value", "displayName": "Lost oil (C$)"}]},
               "Mean cost per failure, since Jun 2025 (C$)", 6, 3, 6, 6,
               fields=[field("failure_type"), field("mean_repair_cost"), field("mean_lost_oil_value")]),
           {"encodings": {"y": {**NO_AXIS_TITLE, "format": WHOLE}}, "mark": {"colors": [None, theme(8)], "layout": "stack"}}),
        ui(table("p4-cost-table", "ds_p4_cost", [
            ("failure_type", "Type", {"contentAlignment": "left"}), ("failures", "Failures", CENTER),
            ("mean_cost_per_failure", "Mean cost (C$)", {**CENTER, "format": cents()}),
            ("median_cost_per_failure", "Median cost (C$)", {**CENTER, "format": cents()}),
            ("total_cost", "Total (C$)", {**CENTER, "format": cents()})],
            "Cost per failure by type", 0, 12, 12, 3)),
        {"widget": {"name": "p4-budget", "spec": {"widgetType": "text", "version": 1, "encodings": {"text": {
            "lines": ["**Budget scenarios**: which wells to treat for 25 / 50 / 100 / 200-well budgets."], "fields": []}}}},
         "position": {"x": 0, "y": 22, "width": GRID, "height": 1}},
        text("p4-inputs", "**Inputs and where they come from.** Netback C$32.50 per boe from Strathcona Q3 2026 operating earnings (company-wide). Repair cost uses field estimates, not invoices; a repair job shared by two failures is split between them. Lost oil counts from the day the well stopped. Bad actors are picked on Jun–Dec 2025 failures and their rate is measured on Jan–Oct 2026. Action cost is assumed from the 2023+ Workover median because no regional price was found. Avoided shares of 25% and 50% are assumptions bracketing one vendor case (~34%); action effects are associations, not proven causes.", 0, 35, GRID, 2),
        budget_counter("wells-treated", "Total Budget Cost ($)", 0, 2, USD_COMPACT),
        budget_counter("fef63e69", "Measured Annual Savings @ 50% ($)", 2, 3, USD_COMPACT),
        budget_counter("c9851fa3", "Measured Annual Savings @ 25% ($)", 5, 3,
                       {"type": "number-plain", "abbreviation": "none", "decimalPlaces": {"type": "max", "places": 2}}),
        budget_counter("b9a5763b", "Measured Payback @ 50% (Years)", 8, 4),
        ui(widget("treated-bad-actor-wells-selected-budget-scenario-ranked-shrunke", "35e45ef4", [field(c) for c in budget_cols],
                  {"frame": {"showTitle": True, "title": "Treated Bad-Actor Wells in Selected Budget Scenario (Ranked by Shrunken Return per Dollar)"},
                   "version": 2, "widgetType": "table", "encodings": {"columns": [{"fieldName": c} for c in budget_cols]}},
                  0, 26, GRID, 9)),
    ]

    precision_recall = {"scale": {"type": "quantitative"}, "fields": [
        {"fieldName": "precision_pct", "displayName": "Precision (%)"},
        {"fieldName": "recall_pct", "displayName": "Recall (%)"}]}
    p5 = [
        subtitle("p5", "Wells are ranked on 2023-24 history plus Mar-May 2025 repairs and tested on Jun 2025 to Oct 2026 cost. A sensor rule only counts if it beats the simple recency watchlist."),
        counter("p5-capture", "ds_p5_kpi", "capture_10_pct", "Later cost caught by the top 10% of wells (%, 95% CI 14.3-19.9)", 0, 1),
        counter("p5-lift", "ds_p5_kpi", "lift_vs_random", "Lift vs random", 4, 1),
        counter("p5-persist", "ds_p5_kpi", "persistence_pct", "Top-10% wells still top 10% later (%)", 8, 1),
        bar("p5-curve", "ds_p5_curve", qty("share_targeted_pct", "Share of wells targeted (%)"), qty("share_captured_pct", "Share of later cost captured (%)"),
            "Ranking with recent repairs edges out size; neither clearly wins", 0, 3, 8, 6, color=cat("ranker", "Ranking"), kind="line",
            fields=[field("share_targeted_pct"), field("ranker"), field("share_captured_pct")]),
        ui(bar("p5-watch", "ds_p5_watch", cat("flag", "Watchlist rule"), dict(precision_recall),
               "Recency watchlist: the baseline sensor rules must beat", 8, 3, 4, 6, group=True,
               fields=[field("flag"), field("precision_pct"), field("recall_pct")]),
           {"encodings": {"x": {"scale": {"sort": {"by": "y-reversed"}}, "axis": {"labelAngle": 0}}, "y": NO_AXIS_TITLE},
            "mark": {"colors": [None, theme(10)]}}),
        ui(bar("p5-rules", "ds_p5_rules", cat("rule_name", "Rule"), dict(precision_recall),
               "The runtime rule does not beat the watchlist on precision, but adds recall (2026)", 0, 9, 6, 6, group=True,
               fields=[field("rule_name"), field("precision_pct"), field("recall_pct")]),
           {"encodings": {"x": {"axis": {"labelAngle": 0, "hideTitle": True}}, "y": {"axis": {"hideLabels": False, "hideTitle": True}},
                          "label": {"show": False}},
            "mark": {"colors": [None, theme(10)]}}),
        table("p5-rules-table", "ds_p5_rules", [
            ("rule_name", "Rule"), ("alerts", "Alerts"), ("precision_pct", "Precision (%)"), ("recall_pct", "Recall (%)"),
            ("failures_caught", "Failures caught"), ("label_failures", "Failures"),
            ("alerts_per_area_week", "Alerts per area-week"), ("median_lead_days", "Median warning (days)")],
            "Early-warning test on Jan–Oct 2026 (rule frozen on 2025)", 6, 9, 6, 6),
        text("p5-rules-note", "**How to read the early-warning test.** Rule frozen on Jun–Dec 2025: alert when a producing well has 2+ partial-run days in a week (down-day and efficiency rules did not add precision). An alert is a hit if the well stops for a rod, pump or tubing failure 2–30 days later; one alert per well per 14 days. Recommendation: lead with the recency watchlist; add the runtime rule if crews can handle about 9 alerts per area per week, which lifts failures caught from 500 to 813 of 1,432.", 0, 15, GRID, 2),
    ]

    p6 = [
        subtitle("p6", "We cleaned the raw files, logged every rule, and list what the data cannot tell us."),
        counter("p6-rows", "ds_p6_clean", "rows_affected", "Rows changed by cleaning rules", 0, 1, "SUM"),
        text("p6-limits", "**What this data cannot tell us.** The failure log starts in Jun 2025. Rod truck cost is missing before 2023. Costs are field estimates, not invoiced costs. Lost oil is an operator estimate. Torque and current repeat day to day (manual entries). The netback is company-wide, not heavy-oil only. Action effects are associations, not proven causes.", 4, 1, 8, 2),
        bar("p6-rules", "ds_p6_clean", qty("rows_affected", "Rows affected"), cat("rule", "Rule"),
            "Where cleaning changed the most rows", 0, 3, GRID, 7, fields=[field("rule"), field("rows_affected")]),
        table("p6-log", "ds_p6_clean", [("rule", "Rule"), ("table_name", "Table"), ("rows_affected", "Rows affected"),
                                         ("share_of_table_pct", "Share of source rows (%)")], "Cleaning log (latest run)", 0, 10, GRID, 7),
        text("p6-ranking", "**Does cleaning change who is on the list? No.** We ranked wells by 12-month cost (repair $ + lost-oil value, Oct 28, 2025 to Oct 27, 2026) on the raw files and again after cleaning. The top 10, 25, 50 and 100 wells are identical, no top-50 well moves a single place, and total cost changes by 0.01%. The rules that matter for the ranking remove 23 duplicate jobs and 29 duplicate well-days; the pressure fixes (999999 placeholders, out-of-range values) and sensor caps do not enter the cost score.", 0, 17, GRID, 2),
    ]

    area_ds = ["ds_p1_kpis", "ds_p1_loss_month", "ds_p1_repair_area", "ds_p2_monthly", "ds_p3_list", "ds_p3_alerts"]
    filt = {"widget": {"name": "filter-area",
                       "queries": [{"name": f"q_{d}_area", "query": {"datasetName": d, "fields": [field("field_operating_area")], "disaggregated": False}} for d in area_ds],
                       "spec": {"version": 2, "widgetType": "filter-multi-select",
                                "encodings": {"fields": [{"fieldName": "field_operating_area", "displayName": "Operating area", "queryName": f"q_{d}_area"} for d in area_ds]},
                                "frame": {"showTitle": True, "title": "Operating area"}}},
            "position": {"x": 0, "y": 0, "width": 4, "height": 2}}

    # The editor names filter queries after the dashboard and dataset IDs.
    scenario_q = {
        "ba232594": "dashboards/01f1e5ecdc1a1b0b8ab0a66212842a79/datasets/01f1e604c4391a3a9b3d0237f721a023_Budget Scenario",
        "35e45ef4": "dashboards/01f1e5ecdc1a1b0b8ab0a66212842a79/datasets/01f1e606d0801090a3e5c72db3a145a9_Budget Scenario",
    }
    scenario_filter = {"widget": {"name": "budget-scenario",
                                  "queries": [{"name": q, "query": {"datasetName": d, "fields": [
                                      field("Budget Scenario"),
                                      field("Budget Scenario_associativity", "COUNT_IF(`associative_filter_predicate_group`)")],
                                      "disaggregated": False}} for d, q in scenario_q.items()],
                                  "spec": {"version": 2, "widgetType": "filter-single-select", "disallowAll": True,
                                           "selection": {"defaultSelection": {"values": {"dataType": "STRING", "values": [{"value": "Treat top 50 wells"}]}}},
                                           "frame": {"showTitle": True, "title": {"value": "Budget Scenario", "fields": []}},
                                           "encodings": {"fields": [{"fieldName": "Budget Scenario", "queryName": q} for q in scenario_q.values()]}}},
                       "position": {"x": 0, "y": 2, "width": 1, "height": 2}}

    return {
        "datasets": [{"name": n, "displayName": d, "queryLines": [l + "\n" for l in sql.strip().split("\n")]} for n, (d, sql) in DATASETS.items()],
        "pages": [
            page("p1", "Cost Drivers", p1),
            page("p2", "Recurrence Analysis", p2),
            page("p3", "Work Order Priorities", p3),
            page("p4", "Return on Action", p4),
            page("p5", "Model Validation", p5),
            page("p6", "Data Quality", p6),
            {"name": "filters", "displayName": "Filters", "pageType": "PAGE_TYPE_GLOBAL_FILTERS", "layout": [filt, scenario_filter],
             "layoutVersion": "GRID_V1"},
        ],
        "uiSettings": {"theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"}, "applyModeEnabled": False},
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
            if x != GRID:
                problems.append(f"{p['name']} y={y}: row width {x}, expected {GRID}")
        for item in p["layout"]:
            w = item["widget"]
            if "queries" in w:
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
            json.dump(d, fh, indent=2, ensure_ascii=False)
            fh.write("\n")
        print(f"wrote {OUT}: {len(d['datasets'])} datasets, {sum(len(p['layout']) for p in d['pages'])} widgets")
        sys.exit(1 if probs else 0)
