# Known issues

Data problems found after the clean tables were frozen (Checkpoint 1, Fri Nov 20, 11 PM). Log them here instead of changing silver tables.

| Found | By | Table / column | Issue | Rows affected | Effect on results | Handled how |
|---|---|---|---|---|---|---|
| Nov 21 | Team (local pandas run) | daily: `pcp_pump_rod_torque_ftlb`, `pcp_pump_electric_current_a`, `pcp_pump_rpm` | Values repeat the previous day's reading 90%, 95% and 96% of the time (median 29, 11 and 13 distinct values per well since Oct 2024). They look like periodic manual entries, not daily sensor averages. `pump_efficiency_pct` and volumes do vary daily. | Most PCP rows | Torque/current drift cannot give a daily early warning | Early-warning rules use runtime, efficiency and fluid rate; torque/current only as context |
| Nov 21 | Team (local pandas run) | failures: `failure_start_date` | Logged after the well stops: 66% of 2025 rod/pump failures were already down on the failure day or the day before; median 2 days from stop to logged date (90th pct 7). | ~720 of 1,095 | Windows anchored on the logged date leak downtime into "before failure"; lost oil from day −1 misses earlier downtime | Align early warning to the stop day; extend the payback lost-oil window back to the stop day |
| Nov 21 | Team (local pandas run) | daily: sensor coverage | Torque covered for only 6% of 2025 Cactus Lake failures (current 4%), the area with the most failures. Efficiency coverage is ~90% everywhere. | Cactus Lake | Torque/current rules can't run in the largest failure area | Rules built on efficiency and runtime |
