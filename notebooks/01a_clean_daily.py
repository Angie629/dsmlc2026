# Databricks notebook source
# MAGIC %md
# MAGIC # 01a · Clean daily production → `silver_daily` · task f4
# MAGIC
# MAGIC Reads the bronze table, applies the cleaning rules from the build plan (task f4), and writes:
# MAGIC - `workspace.dsmlc.silver_daily`: one row per `uwi` × `production_date`
# MAGIC - `workspace.dsmlc.cleaning_log`: one row per rule with the number of rows it changed
# MAGIC
# MAGIC Bronze is never modified. Raw volumes are kept next to cleaned copies because negatives may be accounting corrections (organizer question 3).
# MAGIC `choke_open_pct` is left unchanged until organizers confirm its scale (question 2).

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG_SCHEMA = "workspace.dsmlc"
SOURCE = f"{CATALOG_SCHEMA}.bronze_daily"
TARGET = f"{CATALOG_SCHEMA}.silver_daily"
LOG_TABLE = f"{CATALOG_SCHEMA}.cleaning_log"

PRESSURE_MAX_KPA = 20_000
TUBING_PLACEHOLDER = 999_999
RPM_MAX = 1_000
EFFICIENCY_MAX = 2.0
SENSOR_CAP_QUANTILE = 0.999
MIN_RUNTIME_FOR_RATES = 0.5

RUN_AT = datetime.now(timezone.utc)
log_rows = []


def log(rule, column, rows):
    log_rows.append((RUN_AT, "silver_daily", rule, column, int(rows)))

# COMMAND ----------

raw = spark.table(SOURCE)
source_rows = raw.count()
print(f"{SOURCE}: {source_rows:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Loader column and types

# COMMAND ----------

if "_rescued_data" in raw.columns:
    rescued = raw.filter(F.col("_rescued_data").isNotNull()).count()
    log("rows with values in _rescued_data (column dropped)", "_rescued_data", rescued)
    if rescued:
        display(raw.filter(F.col("_rescued_data").isNotNull()).limit(20))
    raw = raw.drop("_rescued_data")

df = (
    raw.withColumn("surface_latitude", F.col("surface_latitude").cast("double"))
    .withColumn("surface_longitude", F.col("surface_longitude").cast("double"))
    .withColumn("pcp_pump_rpm", F.col("pcp_pump_rpm").cast("double"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Duplicate well-days
# MAGIC
# MAGIC Profiling found 29 duplicate `(uwi, production_date)` rows. Keep the row with the most populated fields so no reading is lost.

# COMMAND ----------

key = ["uwi", "production_date"]
dupes = df.groupBy(key).count().filter("count > 1")
display(dupes.join(df, key).orderBy(key))

filled = sum(F.col(c).isNotNull().cast("int") for c in df.columns)
w = Window.partitionBy(*key).orderBy(F.desc("_filled"))
deduped = (
    df.withColumn("_filled", filled)
    .withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .drop("_filled", "_rn")
)
removed_dupes = source_rows - deduped.count()
log("duplicate (uwi, production_date) rows removed", "uwi, production_date", removed_dupes)
df = deduped

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sensor caps
# MAGIC
# MAGIC Current and torque have extreme spikes (max 84,860 A and 331,460 ft·lb against p99 of 185 A and 5,192 ft·lb). Readings above the 99.9th percentile of positive values are treated as sensor errors.

# COMMAND ----------

caps = (
    df.select(
        F.expr(f"percentile_approx(CASE WHEN pcp_pump_electric_current_a > 0 THEN pcp_pump_electric_current_a END, {SENSOR_CAP_QUANTILE})").alias("current"),
        F.expr(f"percentile_approx(CASE WHEN pcp_pump_rod_torque_ftlb > 0 THEN pcp_pump_rod_torque_ftlb END, {SENSOR_CAP_QUANTILE})").alias("torque"),
    )
    .first()
    .asDict()
)
print(caps)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Null out impossible readings
# MAGIC
# MAGIC Each rule is counted before it is applied, so the log shows exactly how many values changed.

# COMMAND ----------

def outside(col, lo, hi):
    return (F.col(col) < lo) | (F.col(col) > hi)


null_rules = [
    ("tubing pressure placeholder 999999 → null", "tubing_pressure_kpa", F.col("tubing_pressure_kpa") >= TUBING_PLACEHOLDER),
    (f"tubing pressure outside 0–{PRESSURE_MAX_KPA} kPa → null", "tubing_pressure_kpa", outside("tubing_pressure_kpa", 0, PRESSURE_MAX_KPA) & (F.col("tubing_pressure_kpa") < TUBING_PLACEHOLDER)),
    (f"casing pressure outside 0–{PRESSURE_MAX_KPA} kPa → null", "casing_pressure_kpa", outside("casing_pressure_kpa", 0, PRESSURE_MAX_KPA)),
    (f"injection pressure outside 0–{PRESSURE_MAX_KPA} kPa → null", "injection_pressure_kpa", outside("injection_pressure_kpa", 0, PRESSURE_MAX_KPA)),
    (f"RPM outside 0–{RPM_MAX} → null", "pcp_pump_rpm", outside("pcp_pump_rpm", 0, RPM_MAX)),
    (f"motor current above p99.9 ({caps['current']:.0f} A) or negative → null", "pcp_pump_electric_current_a", outside("pcp_pump_electric_current_a", 0, caps["current"])),
    (f"rod torque above p99.9 ({caps['torque']:.0f} ft·lb) or negative → null", "pcp_pump_rod_torque_ftlb", outside("pcp_pump_rod_torque_ftlb", 0, caps["torque"])),
    (f"pump efficiency outside 0–{EFFICIENCY_MAX} → null", "pump_efficiency_pct", outside("pump_efficiency_pct", 0, EFFICIENCY_MAX)),
]

counts = df.select(
    [F.sum(cond.cast("int")).alias(str(i)) for i, (_, _, cond) in enumerate(null_rules)]
).first()

for i, (rule, col, cond) in enumerate(null_rules):
    log(rule, col, counts[str(i)] or 0)
    df = df.withColumn(col, F.when(cond, None).otherwise(F.col(col)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cleaned volume copies
# MAGIC
# MAGIC Raw columns stay as delivered. `*_clean` columns null out negatives for rate and total calculations.

# COMMAND ----------

volume_cols = ["oil_produced_m3", "water_produced_m3", "gas_produced_e3m3"]
neg = df.select([F.sum((F.col(c) < 0).cast("int")).alias(c) for c in volume_cols]).first()

for c in volume_cols:
    clean = c.replace("_produced", "").replace("_m3", "_clean_m3").replace("_e3m3", "_clean_e3m3")
    log(f"negative values → null in {clean} (raw kept)", c, neg[c] or 0)
    df = df.withColumn(clean, F.when(F.col(c) < 0, None).otherwise(F.col(c)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Pump make
# MAGIC
# MAGIC About 32 spellings map to one name per manufacturer. Generic labels ("PCP", "PCP Pump") become null. Anything not in the map keeps its original value and is listed below for review.

# COMMAND ----------

MAKE_MAP = {
    "weatherford": "Weatherford", "weathweford": "Weatherford", "weeatherford": "Weatherford", "wfd": "Weatherford", "wtf": "Weatherford",
    "mantl": "Mantl", "mantle new": "Mantl",
    "europump": "Europump", "euro pump": "Europump", "eurppump": "Europump", "europump (pcm)": "Europump",
    "kudu": "Kudu",
    "pcm": "PCM",
    "bmw": "BMW", "bmw pump": "BMW",
    "lsi": "LSI", "lifting solutions": "LSI",
    "baker hughes": "Baker Hughes",
    "nov": "NOV",
    "quinn": "Quinn",
    "r&m": "R&M",
    "t2": "T2",
}
UNKNOWN_MAKES = ["pcp", "pcp pump"]

make_key = F.lower(F.trim(F.col("pcp_pump_make")))
map_expr = F.create_map(*[F.lit(x) for kv in MAKE_MAP.items() for x in kv])
df = df.withColumn(
    "pcp_pump_make_norm",
    F.when(make_key.isin(UNKNOWN_MAKES), None).otherwise(F.coalesce(map_expr[make_key], F.trim(F.col("pcp_pump_make")))),
)

unmapped = (
    df.filter(F.col("pcp_pump_make").isNotNull() & ~make_key.isin(list(MAKE_MAP) + UNKNOWN_MAKES))
    .groupBy("pcp_pump_make").count()
)
display(unmapped)
log("pump make rows with unmapped spelling (kept as-is)", "pcp_pump_make", unmapped.agg(F.coalesce(F.sum("count"), F.lit(0))).first()[0])
log("pump make rows set to unknown (PCP / PCP Pump)", "pcp_pump_make", df.filter(make_key.isin(UNKNOWN_MAKES)).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Derived columns
# MAGIC
# MAGIC - `water_cut` = water / (oil + water), only when the sum is positive
# MAGIC - `runtime_frac` = hours on production / 24 (hours on + hours down is 24 on every row)
# MAGIC - `is_rate_day`: runtime ≥ 0.5, used for sensor baselines in early warning

# COMMAND ----------

liquid = F.col("oil_clean_m3") + F.col("water_clean_m3")
df = (
    df.withColumn("water_cut", F.when(liquid > 0, F.col("water_clean_m3") / liquid))
    .withColumn("runtime_frac", F.col("hours_on_prod") / 24)
    .withColumn("is_rate_day", F.col("hours_on_prod") / 24 >= MIN_RUNTIME_FOR_RATES)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Write and check

# COMMAND ----------

df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET)

log_df = spark.createDataFrame(log_rows, "run_at timestamp, target string, rule string, column_name string, rows_affected long")
log_df.write.mode("append").saveAsTable(LOG_TABLE)
display(log_df)

# COMMAND ----------

silver = spark.table(TARGET)
silver_rows = silver.count()

checks = {
    "row count = source − duplicates": silver_rows == source_rows - removed_dupes,
    "no duplicate well-days": silver.groupBy(key).count().filter("count > 1").count() == 0,
    "no tubing/casing pressure above max": silver.filter((F.col("tubing_pressure_kpa") > PRESSURE_MAX_KPA) | (F.col("casing_pressure_kpa") > PRESSURE_MAX_KPA)).count() == 0,
    "no negative cleaned volumes": silver.filter((F.col("oil_clean_m3") < 0) | (F.col("water_clean_m3") < 0)).count() == 0,
    "RPM within range": silver.filter(F.col("pcp_pump_rpm") > RPM_MAX).count() == 0,
}
for name, ok in checks.items():
    print(("PASS " if ok else "FAIL ") + name)
print(f"{TARGET}: {silver_rows:,} rows, {silver.select('uwi').distinct().count():,} wells")
assert all(checks.values()), "One or more checks failed"
