# Databricks notebook source
# MAGIC %md
# MAGIC # 01b · Clean jobs and failures → `silver_jobs`, `silver_failures` · task f5
# MAGIC
# MAGIC Applies the cleaning rules from the build plan (task f5) and writes:
# MAGIC - `workspace.dsmlc.silver_jobs`: one row per job, with a stable `job_id`
# MAGIC - `workspace.dsmlc.silver_failures`: one row per failure, linked to its repair job
# MAGIC - rows appended to `workspace.dsmlc.cleaning_log`
# MAGIC
# MAGIC Bronze tables are never modified.

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG_SCHEMA = "workspace.dsmlc"
JOBS_SOURCE = f"{CATALOG_SCHEMA}.bronze_jobs"
FAILURES_SOURCE = f"{CATALOG_SCHEMA}.bronze_failures"
DAILY_SOURCE = f"{CATALOG_SCHEMA}.bronze_daily"
JOBS_TARGET = f"{CATALOG_SCHEMA}.silver_jobs"
FAILURES_TARGET = f"{CATALOG_SCHEMA}.silver_failures"
LOG_TABLE = f"{CATALOG_SCHEMA}.cleaning_log"

REPAIR_CATEGORIES = ["Rod Truck", "Workover"]
LINK_DAYS_BEFORE = 3
LINK_DAYS_AFTER = 14

RUN_AT = datetime.now(timezone.utc)
log_rows = []


def log(target, rule, column, rows):
    log_rows.append((RUN_AT, target, rule, column, int(rows)))


def drop_rescued(df, target):
    if "_rescued_data" not in df.columns:
        return df
    rescued = df.filter(F.col("_rescued_data").isNotNull()).count()
    log(target, "rows with values in _rescued_data (column dropped)", "_rescued_data", rescued)
    if rescued:
        display(df.filter(F.col("_rescued_data").isNotNull()).limit(20))
    return df.drop("_rescued_data")


def dedupe_keep_fullest(df, key):
    filled = sum(F.col(c).isNotNull().cast("int") for c in df.columns)
    w = Window.partitionBy(*key).orderBy(F.desc("_filled"))
    return (
        df.withColumn("_filled", filled)
        .withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_filled", "_rn")
    )


def map_values(col, mapping, default):
    key = F.lower(F.trim(F.col(col)))
    m = F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])
    return F.coalesce(m[key], F.lit(default))

# COMMAND ----------

daily_wells = spark.table(DAILY_SOURCE).select("uwi").distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Jobs
# MAGIC ### 1. Load, dedupe

# COMMAND ----------

jobs_raw = drop_rescued(spark.table(JOBS_SOURCE), "silver_jobs")
jobs_source_rows = jobs_raw.count()
print(f"{JOBS_SOURCE}: {jobs_source_rows:,} rows")

jobs = (
    jobs_raw.withColumn("start_date", F.col("start_date").cast("date"))
    .withColumn("end_date", F.col("end_date").cast("date"))
)

job_key = ["uwi", "start_date", "job_type", "job_subtype"]
display(jobs.groupBy(job_key).count().filter("count > 1"))
jobs = dedupe_keep_fullest(jobs, job_key)
jobs_removed = jobs_source_rows - jobs.count()
log("silver_jobs", "duplicate (uwi, start_date, job_type, job_subtype) rows removed", ", ".join(job_key), jobs_removed)

jobs = jobs.withColumn(
    "job_id",
    F.sha2(F.concat_ws("|", "uwi", F.col("start_date").cast("string"), F.coalesce("job_type", F.lit("")), F.coalesce("job_subtype", F.lit(""))), 256).substr(1, 16),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Durations
# MAGIC
# MAGIC `job_duration_days` is fractional (median 0.4 days) and has 65 negative values; 17 jobs end before they start. Bad values become null. `duration_days_from_dates` is whole days from the dates, for checking.

# COMMAND ----------

bad_duration = F.col("job_duration_days") < 0
end_before_start = F.col("end_date") < F.col("start_date")
c = jobs.select(
    F.sum(bad_duration.cast("int")).alias("neg"),
    F.sum(end_before_start.cast("int")).alias("rev"),
).first()
log("silver_jobs", "negative job_duration_days → null", "job_duration_days", c["neg"] or 0)
log("silver_jobs", "end_date before start_date → duration null", "end_date", c["rev"] or 0)

jobs = (
    jobs.withColumn("job_duration_days", F.when(bad_duration | end_before_start, None).otherwise(F.col("job_duration_days")))
    .withColumn("duration_days_from_dates", F.when(end_before_start, None).otherwise(F.datediff("end_date", "start_date")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Job category
# MAGIC
# MAGIC 29 raw values, several of them equipment descriptions. Mapped to Rod Truck, Workover, Drilling, Completion, Abandon, Other.

# COMMAND ----------

CATEGORY_MAP = {
    "rod truck": "Rod Truck",
    "workover": "Workover",
    "drilling": "Drilling", "drilling-surrogate": "Drilling", "drilling - origanal": "Drilling",
    "pre set surface hole": "Drilling", "pre-set surface": "Drilling", "pre-set": "Drilling",
    "completion": "Completion", "initial completion": "Completion", "re-completion": "Completion",
    "recompletion": "Completion", "completion/workover": "Completion",
    "abandon": "Abandon",
}
jobs = jobs.withColumn("job_category_norm", map_values("job_category", CATEGORY_MAP, "Other"))
display(jobs.groupBy("job_category", "job_category_norm").count().orderBy(F.desc("count")))
log("silver_jobs", "job_category mapped to Other", "job_category", jobs.filter("job_category_norm = 'Other'").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Cost and empty columns
# MAGIC
# MAGIC `actual_final_job_cost` and `job_status_1/2` are empty. `field_est_total` ≤ 0 is excluded from cost sums.

# COMMAND ----------

nonpos = jobs.filter(F.col("field_est_total") <= 0).count()
log("silver_jobs", "field_est_total <= 0 → field_est_clean null", "field_est_total", nonpos)

jobs = (
    jobs.withColumn("field_est_clean", F.when(F.col("field_est_total") > 0, F.col("field_est_total")))
    .withColumn("is_repair", F.col("job_category_norm").isin(REPAIR_CATEGORIES))
    .drop("job_status_1", "job_status_2", "actual_final_job_cost")
)
log("silver_jobs", "empty columns dropped", "job_status_1, job_status_2, actual_final_job_cost", 0)
log("silver_jobs", "jobs on wells missing from daily (kept)", "uwi", jobs.join(daily_wells, "uwi", "left_anti").count())

# COMMAND ----------

jobs.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(JOBS_TARGET)
silver_jobs = spark.table(JOBS_TARGET)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Failures
# MAGIC ### 5. Load, dedupe

# COMMAND ----------

fail_raw = drop_rescued(spark.table(FAILURES_SOURCE), "silver_failures")
fail_source_rows = fail_raw.count()
print(f"{FAILURES_SOURCE}: {fail_source_rows:,} rows")

fail = fail_raw.withColumn("failure_start_date", F.col("failure_start_date").cast("date"))

fail_key = ["uwi", "failure_start_date", "failure_type"]
display(fail.groupBy(fail_key).count().filter("count > 1"))
fail = dedupe_keep_fullest(fail, fail_key)
fail_removed = fail_source_rows - fail.count()
log("silver_failures", "duplicate (uwi, failure_start_date, failure_type) rows removed", ", ".join(fail_key), fail_removed)

fail = fail.withColumn(
    "failure_id",
    F.sha2(F.concat_ws("|", "uwi", F.col("failure_start_date").cast("string"), F.coalesce("failure_type", F.lit(""))), 256).substr(1, 16),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Failure type and rod kind
# MAGIC
# MAGIC `failure_type` has 29 raw values (`Rod`, `|Rod`, `PCP`, free text). Rod kind comes from `failure_detail`, so it is known only for failed wells.

# COMMAND ----------

TYPE_MAP = {
    "rods": "Rods", "rod": "Rods", "|rod": "Rods",
    "pump": "Pump", "pcp": "Pump",
    "tubing": "Tubing", "hole in tbg colar": "Tubing",
    "surface failure": "Surface",
    "casing": "Casing",
    "packer": "Packer",
}
fail = fail.withColumn(
    "failure_type_norm",
    F.when(F.col("failure_type").isNull(), "Unknown").otherwise(map_values("failure_type", TYPE_MAP, "Other")),
)
display(fail.groupBy("failure_type", "failure_type_norm").count().orderBy(F.desc("count")))
log("silver_failures", "failure_type mapped to Other", "failure_type", fail.filter("failure_type_norm = 'Other'").count())
log("silver_failures", "failure_type null → Unknown", "failure_type", fail.filter("failure_type_norm = 'Unknown'").count())

detail = F.lower(F.col("failure_detail"))
fail = fail.withColumn(
    "rod_kind",
    F.when(detail.contains("corod"), "COROD").when(detail.contains("sucker rod"), "Sucker rod"),
)
display(fail.groupBy("failure_type_norm", "rod_kind").count().orderBy("failure_type_norm", F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Empty columns
# MAGIC
# MAGIC `failure_end_date` is null on 2,872 of 2,873 rows, so failure duration must come from daily downtime or the linked job.

# COMMAND ----------

EMPTY_FAILURE_COLS = [
    "failure_end_date", "cause_category", "action_taken",
    "failure_system_category", "failure_type_category", "cause_comments",
]
nonnull = fail.select([F.count(c).alias(c) for c in EMPTY_FAILURE_COLS]).first().asDict()
print(nonnull)
for c_name, n in nonnull.items():
    log("silver_failures", "near-empty column dropped (non-null rows shown)", c_name, n)
fail = fail.drop(*EMPTY_FAILURE_COLS)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Link each failure to its repair job
# MAGIC
# MAGIC First Rod Truck or Workover job on the same well from 3 days before to 14 days after the failure. Profiling matched 2,812 of 2,873 failures this way.
# MAGIC Two failures on the same day can share one job (`job_shared`); count job cost once per `job_id` downstream.

# COMMAND ----------

repair = silver_jobs.filter("is_repair").select(
    "uwi", "job_id",
    F.col("start_date").alias("linked_job_start"),
    F.col("job_category_norm").alias("linked_job_category"),
    F.col("job_type").alias("linked_job_type"),
    F.col("job_subtype").alias("linked_job_subtype"),
    F.col("field_est_clean").alias("linked_job_cost"),
)

cand = (
    fail.select("failure_id", "uwi", "failure_start_date")
    .join(repair, "uwi")
    .withColumn("days_to_job", F.datediff("linked_job_start", "failure_start_date"))
    .filter(F.col("days_to_job").between(-LINK_DAYS_BEFORE, LINK_DAYS_AFTER))
)
w = Window.partitionBy("failure_id").orderBy("linked_job_start", "job_id")
link = (
    cand.withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .drop("_rn", "uwi", "failure_start_date")
)
shared = link.groupBy("job_id").count().filter("count > 1").select("job_id", F.lit(True).alias("job_shared"))
link = link.join(shared, "job_id", "left").fillna({"job_shared": False})

fail = fail.join(link, "failure_id", "left")

linked = fail.filter(F.col("job_id").isNotNull()).count()
log("silver_failures", "failures linked to a repair job within -3..+14 days", "job_id", linked)
log("silver_failures", "failures with no repair job in window", "job_id", fail.filter(F.col("job_id").isNull()).count())
log("silver_failures", "failures on wells missing from daily (kept)", "uwi", fail.join(daily_wells, "uwi", "left_anti").count())

# COMMAND ----------

fail.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(FAILURES_TARGET)

log_df = spark.createDataFrame(log_rows, "run_at timestamp, target string, rule string, column_name string, rows_affected long")
log_df.write.mode("append").saveAsTable(LOG_TABLE)
display(log_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Checks

# COMMAND ----------

sj = spark.table(JOBS_TARGET)
sf = spark.table(FAILURES_TARGET)
sj_rows, sf_rows = sj.count(), sf.count()

checks = {
    "jobs row count = source − duplicates": sj_rows == jobs_source_rows - jobs_removed,
    "failures row count = source − duplicates": sf_rows == fail_source_rows - fail_removed,
    "job_id unique": sj.select("job_id").distinct().count() == sj_rows,
    "failure_id unique": sf.select("failure_id").distinct().count() == sf_rows,
    "no negative durations": sj.filter(F.col("job_duration_days") < 0).count() == 0,
    "no non-positive cost in field_est_clean": sj.filter(F.col("field_est_clean") <= 0).count() == 0,
    "linking did not duplicate failures": sf_rows == fail_source_rows - fail_removed,
}
for name, ok in checks.items():
    print(("PASS " if ok else "FAIL ") + name)

repair_cost = sj.filter("is_repair").agg(F.sum("field_est_clean")).first()[0]
print(f"{JOBS_TARGET}: {sj_rows:,} jobs, {sj.select('uwi').distinct().count():,} wells; repair $ = {repair_cost:,.0f} (profiling: ~212.4M)")
print(f"{FAILURES_TARGET}: {sf_rows:,} failures, {sf.select('uwi').distinct().count():,} wells; linked to a job: {linked:,} (profiling: ~2,812)")
assert all(checks.values()), "One or more checks failed"
