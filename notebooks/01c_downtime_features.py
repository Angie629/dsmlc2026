# Databricks notebook source
# MAGIC %md
# MAGIC # 01c · Downtime buckets and daily sensor features · task f6
# MAGIC
# MAGIC Build plan task f6. Reads `silver_daily` (run `01a_clean_daily` first) and writes:
# MAGIC - `workspace.dsmlc.downtime_bucket_map`: every `(downtime_code_1, downtime_code_2)` pair → bucket, with rows, hours and lost oil
# MAGIC - `workspace.dsmlc.silver_downtime`: one row per well-day with downtime or lost oil, tagged with its bucket
# MAGIC - `workspace.dsmlc.feat_daily_sensor`: rolling pump-sensor features per producing well-day, for early warning
# MAGIC
# MAGIC `silver_daily` is not modified; join to `silver_downtime` on `(uwi, production_date)` when needed.

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG_SCHEMA = "workspace.dsmlc"
SOURCE = f"{CATALOG_SCHEMA}.silver_daily"
MAP_TARGET = f"{CATALOG_SCHEMA}.downtime_bucket_map"
DOWNTIME_TARGET = f"{CATALOG_SCHEMA}.silver_downtime"
FEAT_TARGET = f"{CATALOG_SCHEMA}.feat_daily_sensor"
LOG_TABLE = f"{CATALOG_SCHEMA}.cleaning_log"

RECENT_DAYS = (-6, 0)
BASELINE_DAYS = (-37, -8)
MIN_RECENT_DAYS = 4
MIN_BASELINE_DAYS = 15
MAX_OTHER_SHARE = 0.05

RUN_AT = datetime.now(timezone.utc)
log_rows = []


def log(target, rule, column, rows):
    log_rows.append((RUN_AT, target, rule, column, int(rows)))


daily = spark.table(SOURCE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Downtime code pairs
# MAGIC
# MAGIC `downtime_code_2` mixes at least three coding schemes (long names, short codes like `RODSPART` or `UNECO10`, and `KGB - …` names). Bucketing uses both codes together.
# MAGIC Profiling found 165 distinct pairs; the top 30 cover ~90% of lost oil.

# COMMAND ----------

pairs = (
    daily.filter(F.col("downtime_code_1").isNotNull())
    .groupBy("downtime_code_1", "downtime_code_2")
    .agg(
        F.count("*").alias("rows"),
        F.sum("hours_down").alias("hours_down"),
        F.sum("lost_oil_m3").alias("lost_oil_m3"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bucket rules
# MAGIC
# MAGIC First matching rule wins, checked on `lower(code_1 | code_2)`. Order matters: economic shut-ins and planned work are taken out before equipment rules, so "Turn Around" at a facility counts as planned, and "Unplanned" never matches "planned".
# MAGIC
# MAGIC | Bucket | Meaning | Used in |
# MAGIC |---|---|---|
# MAGIC | Downhole | Rods, pump, tubing, torqued, other downhole equipment | Bad-actor cost score |
# MAGIC | Surface equipment | Drive head, motor, electrical, wellhead | Dashboard |
# MAGIC | Power | Grid outages, gensets | Weather check H1 |
# MAGIC | Facility / pipeline | Battery, plant, flowline, pipeline, ESD | Dashboard |
# MAGIC | Access / weather | Weather conditions, lease conditions, road bans | Weather check H3 |
# MAGIC | Planned | Turnarounds, scheduled work, development, drilling, regulatory | Excluded from score |
# MAGIC | Economic | Uneconomic and long-term shut-in | Excluded from score |
# MAGIC | Injection | Injection-well downtime | Excluded (not producers) |
# MAGIC | Other | Coded but none of the above | Reviewed below |

# COMMAND ----------

def has(text, words):
    cond = F.lit(False)
    for w in words:
        cond = cond | text.contains(w)
    return cond


code1 = F.lower(F.trim(F.col("downtime_code_1")))
code2 = F.lower(F.trim(F.coalesce(F.col("downtime_code_2"), F.lit(""))))
text = F.concat_ws(" | ", code1, code2)

ECONOMIC = ["uneconomic", "uneco", "long term shut", "ltsi"]
ACCESS_WEATHER = ["weather", "road ba", "lease conditions", "poor road", "lcrb"]
PLANNED = [
    "turn around", "scheduled", "sched main", "development", "conc ops", "concurrent", "capc",
    "si:drill", "wellstim", "regulatory", "planned well", "planned plant", "planned pad",
    "plan plant", "plan pad", "planned outage", "plndotgsp", "routine maintenance",
    "facility - maintenance", "mnt1", "install new equipment", "neweq", "construction",
]
DOWNHOLE_CODE2 = ["rodspart", "pum", "code3", "code13"]
POWER = ["power source", "sask power", "grid", "genset", "unplndw", "unplndg"]
SURFACE = ["surface equipment", "surface upset"]
SURFACE_CODE2 = ["engdown", "whsb", "elfail", "eqt", "sem"]
FACILITY = ["facility", "fac -", "pipeline", "flowline", "compressor", "amine", "generator", "inlet sep", "trucking"]
FACILITY_CODE2 = ["ffm", "tie", "esd", "ta"]

is_downhole = (
    code1.startswith("downhole")
    | has(text, ["- downhole -", "unplanned well - downhole", "well - dh -"])
    | code2.isin(DOWNHOLE_CODE2)
)

bucket = (
    F.when(has(text, ECONOMIC), "Economic")
    .when(code1.startswith("injection") | code1.startswith("all - injection"), "Injection")
    .when(has(text, ACCESS_WEATHER), "Access / weather")
    .when(has(text, PLANNED) | ((code1 == "facility") & (code2 == "ta")), "Planned")
    .when(is_downhole, "Downhole")
    .when(has(text, POWER), "Power")
    .when(has(text, SURFACE) | code2.isin(SURFACE_CODE2), "Surface equipment")
    .when(has(text, FACILITY) | code2.isin(FACILITY_CODE2), "Facility / pipeline")
    .otherwise("Other")
)

component = (
    F.when(F.col("bucket") != "Downhole", None)
    .when(has(text, ["torqued"]), "Torqued")
    .when(has(text, ["tubing"]), "Tubing")
    .when(has(text, ["rod"]), "Rods")
    .when(has(text, ["pump"]) | (code2 == "pum"), "Pump")
    .otherwise("Other downhole")
)

bucket_map = pairs.withColumn("bucket", bucket).withColumn("downhole_component", component)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Review the map
# MAGIC
# MAGIC Check the summary, then scan the full map sorted by lost oil. `CODE3` and `CODE13` under `Downhole` have no description in the data; they are counted as "Other downhole".

# COMMAND ----------

total_lost = bucket_map.agg(F.sum("lost_oil_m3")).first()[0]
summary = (
    bucket_map.groupBy("bucket")
    .agg(F.sum("rows").alias("rows"), F.sum("hours_down").alias("hours_down"), F.sum("lost_oil_m3").alias("lost_oil_m3"))
    .withColumn("lost_share", F.round(F.col("lost_oil_m3") / total_lost, 4))
    .orderBy(F.desc("lost_oil_m3"))
)
display(summary)
display(
    bucket_map.filter("bucket = 'Downhole'").groupBy("downhole_component")
    .agg(F.sum("lost_oil_m3").alias("lost_oil_m3"), F.sum("rows").alias("rows"))
    .orderBy(F.desc("lost_oil_m3"))
)
display(bucket_map.orderBy(F.desc("lost_oil_m3")))

# COMMAND ----------

non_economic_lost = bucket_map.filter("bucket NOT IN ('Economic', 'Injection')").agg(F.sum("lost_oil_m3")).first()[0]
other_lost = bucket_map.filter("bucket = 'Other'").agg(F.coalesce(F.sum("lost_oil_m3"), F.lit(0.0))).first()[0]
other_share = other_lost / non_economic_lost
print(f"Lost oil in 'Other': {other_lost:,.0f} m³ = {other_share:.1%} of non-economic lost oil")
display(bucket_map.filter("bucket = 'Other'").orderBy(F.desc("lost_oil_m3")))
assert other_share <= MAX_OTHER_SHARE, "Too much lost oil left in 'Other'; add rules above"

bucket_map.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(MAP_TARGET)
for r in summary.collect():
    log("downtime_bucket_map", f"code pairs' rows in bucket {r['bucket']}", "downtime_code_1, downtime_code_2", r["rows"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `silver_downtime`
# MAGIC
# MAGIC Well-days with any downtime or lost oil. Days with hours down but no code are kept as "Uncoded" (~27.6k rows, ~288 m³ in profiling).

# COMMAND ----------

dmap = spark.table(MAP_TARGET).select("downtime_code_1", "downtime_code_2", "bucket", "downhole_component")
events = daily.filter((F.col("hours_down") > 0) | (F.col("lost_oil_m3") > 0)).select(
    "uwi", "production_date", "field_operating_area", "facility_name", "flow_direction",
    "well_daily_status", "hours_down", "lost_oil_m3",
    "downtime_code_1", "downtime_code_2", "downtime_code_3",
)
downtime = (
    events.join(
        dmap,
        events.downtime_code_1.eqNullSafe(dmap.downtime_code_1) & events.downtime_code_2.eqNullSafe(dmap.downtime_code_2),
        "left",
    )
    .drop(dmap.downtime_code_1)
    .drop(dmap.downtime_code_2)
    .withColumn("bucket", F.coalesce("bucket", F.when(F.col("downtime_code_1").isNull(), "Uncoded")))
    .withColumn("is_downhole", F.col("bucket") == "Downhole")
)
downtime.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(DOWNTIME_TARGET)

sd = spark.table(DOWNTIME_TARGET)
log("silver_downtime", "well-days with downtime or lost oil", "hours_down, lost_oil_m3", sd.count())
log("silver_downtime", "well-days with hours down but no code (Uncoded)", "downtime_code_1", sd.filter("bucket = 'Uncoded'").count())
unmatched = sd.filter(F.col("bucket").isNull()).count()
log("silver_downtime", "coded well-days that did not match the map", "downtime_code_1", unmatched)
display(
    sd.groupBy("bucket").agg(F.count("*").alias("well_days"), F.sum("lost_oil_m3").alias("lost_oil_m3"))
    .orderBy(F.desc("lost_oil_m3"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Sensor coverage
# MAGIC
# MAGIC Early warning only works where torque, current and efficiency are recorded. Profiling: torque null on 74% of rows, current 82%, efficiency 67%. This shows coverage on producing rate days (well ran ≥ 12 h).

# COMMAND ----------

SENSORS = {
    "pcp_pump_rod_torque_ftlb": "torque",
    "pcp_pump_electric_current_a": "current",
    "pump_efficiency_pct": "efficiency",
    "pcp_pump_rpm": "rpm",
    "pcp_pump_volume_throughput_m3d": "throughput",
}
prod = daily.filter((F.col("flow_direction") == "production") & F.col("is_rate_day"))
display(prod.select([F.round(F.avg(F.col(c).isNotNull().cast("int")), 3).alias(f"{a}_coverage") for c, a in SENSORS.items()]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. `feat_daily_sensor`
# MAGIC
# MAGIC Per producing well-day, using only rate days so shut-in days don't drag averages to zero:
# MAGIC - `<sensor>_7d`: mean over days −6 to 0
# MAGIC - `<sensor>_base`: mean over days −37 to −8 (a 30-day baseline that ends a week before, so a fresh problem doesn't hide in its own baseline)
# MAGIC - `<sensor>_chg`: `7d / base − 1` (efficiency: `7d − base`, in points)
# MAGIC - Changes are null unless the 7-day window has ≥ 4 readings and the baseline ≥ 15
# MAGIC - `torque_per_rpm`: daily torque ÷ RPM, which normalizes load for speed changes
# MAGIC
# MAGIC Windows are by calendar day, so gaps in the record shrink the window instead of reaching further back.

# COMMAND ----------

base = (
    daily.filter(F.col("flow_direction") == "production")
    .select(
        "uwi", "production_date", "field_operating_area", "is_rate_day", "hours_on_prod",
        "oil_clean_m3", "water_clean_m3", "pcp_pump_make_norm", "pcp_pump_model",
        *[F.when(F.col("is_rate_day"), F.col(c)).alias(a) for c, a in SENSORS.items()],
    )
    .withColumn("torque_per_rpm", F.when(F.col("rpm") > 0, F.col("torque") / F.col("rpm")))
    .withColumn("day_n", F.datediff("production_date", F.lit("2021-01-01")))
)

FEATURE_SIGNALS = ["torque", "current", "efficiency", "rpm", "throughput", "torque_per_rpm"]
by_well = Window.partitionBy("uwi").orderBy("day_n")
recent = by_well.rangeBetween(*RECENT_DAYS)
baseline = by_well.rangeBetween(*BASELINE_DAYS)

feat = base
for s in FEATURE_SIGNALS:
    feat = (
        feat.withColumn(f"{s}_7d", F.avg(s).over(recent))
        .withColumn(f"{s}_n7", F.count(s).over(recent))
        .withColumn(f"{s}_base", F.avg(s).over(baseline))
        .withColumn(f"{s}_nbase", F.count(s).over(baseline))
    )
    enough = (F.col(f"{s}_n7") >= MIN_RECENT_DAYS) & (F.col(f"{s}_nbase") >= MIN_BASELINE_DAYS)
    if s == "efficiency":
        change = F.col(f"{s}_7d") - F.col(f"{s}_base")
    else:
        change = F.when(F.col(f"{s}_base") > 0, F.col(f"{s}_7d") / F.col(f"{s}_base") - 1)
    feat = feat.withColumn(f"{s}_chg", F.when(enough, change))

feat = feat.drop("day_n")
feat.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(FEAT_TARGET)

ft = spark.table(FEAT_TARGET)
ft_rows = ft.count()
log("feat_daily_sensor", "producing well-days", "uwi, production_date", ft_rows)
for s in ["torque", "current", "efficiency"]:
    log("feat_daily_sensor", f"well-days with a usable {s}_chg", f"{s}_chg", ft.filter(F.col(f"{s}_chg").isNotNull()).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Log and checks

# COMMAND ----------

log_df = spark.createDataFrame(log_rows, "run_at timestamp, target string, rule string, column_name string, rows_affected long")
log_df.write.mode("append").saveAsTable(LOG_TABLE)
display(log_df)

downhole_lost = sd.filter("is_downhole").agg(F.sum("lost_oil_m3")).first()[0]
checks = {
    "every coded well-day matched a bucket": unmatched == 0,
    "map has one row per code pair": spark.table(MAP_TARGET).groupBy("downtime_code_1", "downtime_code_2").count().filter("count > 1").count() == 0,
    "silver_downtime has one row per well-day": sd.groupBy("uwi", "production_date").count().filter("count > 1").count() == 0,
    "features have one row per well-day": ft.groupBy("uwi", "production_date").count().filter("count > 1").count() == 0,
    "'Other' within limit": other_share <= MAX_OTHER_SHARE,
}
for name, ok in checks.items():
    print(("PASS " if ok else "FAIL ") + name)
print(f"Downhole lost oil: {downhole_lost:,.0f} m³ (3-code estimate from profiling was 213,499 m³; the full map should be higher)")
print(f"{FEAT_TARGET}: {ft_rows:,} producing well-days")
assert all(checks.values()), "One or more checks failed"
