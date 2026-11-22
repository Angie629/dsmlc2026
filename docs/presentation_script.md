# Presentation script: Bad Actors (5-minute video)

Deck: `docs/Bad Actors - DSMLC Datathon 2026.pptx` (slides 1–18 are the talk; slides 19–27 are the appendix). The same lines are in each slide's speaker notes.

Target: 5:00, about 3 minutes of slides and a 2-minute live dashboard demo (six tabs, about 20 seconds each). Times are cumulative. Speakers are a suggestion; swap freely.

For the demo, record a screen capture of the dashboard in Databricks, light mode. If the workspace is over its compute limit, use the screenshots in `dashboard/` and say the same lines over them.

---

## 0:00 · Slides 1–2 · Title and contents (Angie, 10 s)

> We're Ananya, Angie and Jacob. Our project, Bad Actors, finds the wells that keep failing and shows which fixes pay back.

## 0:10 · Slides 3–5 · The question and the problem (Angie, 25 s)

> Our question: which producing wells will drive Strathcona's repeat downhole failures and lost oil next period, and which targeted actions on those wells pay back?
>
> It matters because rod truck and workover repairs cost about 33 million dollars a year, and 57 percent of failures come back on the same well within 90 days.

## 0:35 · Slides 6–8 · Our approach (Jacob, 25 s)

> We built a pipeline in Databricks, from raw files to cleaned tables to a dashboard, and logged every cleaning rule. Then we asked four questions and tested each on data the method hadn't seen: can history rank the wells, which actions pay back, can we warn before a failure, and does it hold up on 2026?

## 1:00 · Slides 9–11 · Findings: failures repeat (Jacob, 20 s)

> The strongest signal is recent failures. A well with none in the last 90 days fails again 26 percent of the time; with two or more, 61 to 70 percent. Long history helps less: our ranking does only about as well as ranking wells by size.

## 1:20 · Slide 12 · Payback (Ananya, 20 s)

> We priced two actions: rod string upgrades or corrosion inhibition, and pump and tubing failure prevention. If they halve failures, both pay back in about two years. Treating the top 25 wells costs 850 thousand dollars.

## 1:40 · Slide 13 · Early warning (Ananya, 20 s)

> For early warning, the simple watchlist was right 29 percent of the time in 2026, better than a sensor rule. Together they catch 57 percent of failures. A simple machine-learning model does 3 to 7 points better at the same workload, so it's our next step.

## 2:00 · Slide 14 · Dashboard demo (Jacob, 2 min)

Screen recording, light mode. About 20 seconds per tab.

> *Overview.* This is our Reliability Investment Dashboard. The map shows every well that failed in the last 90 days; darker orange means more failures. I'll click a well, and the counters, cost chart and ranked table all follow it. Today 114 wells are on the watchlist.
>
> *Cost Drivers.* Where the money goes. Rods fail most often, but pump and tubing failures cost the most per repair, and monthly repair spend has crept up about 5 percent a year since 2023.
>
> *Recurrence Analysis.* Failures come back. More than half repeat within 90 days, mostly within about six weeks, and the more recent failures a well has, the more likely the next one.
>
> *Work Order Priorities.* This is the Monday-morning list. I'll filter to Cactus Lake: recent failures first, then cost, with a suggested action per well. Below, this week's alerts, with the most reliable tier on top.
>
> *Return on Action.* Pick a budget, say the top 50 wells, and see the cost, savings and payback. In the sensitivity table, green means payback under two years.
>
> *Validation and Data Quality.* Every result was set on past data and tested on 2026: the ranking, the alert rules, and the model challenger. At the bottom, the cleaning log, and proof that cleaning doesn't change which wells come out on top.

## 4:00 · Slides 15–17 · Recommendation and 90-day pilot (Angie, 45 s)

> We recommend three things. Work the watchlist every week, recent failures first. Fund the top 25 bad actors for 850 thousand dollars. And pilot the model alongside the rules.
>
> Start in Cactus Lake, where the money is, and Winter, where failures repeat most; together they hold 62 percent of failures. In the first 30 days, run weekly reviews. By day 60, pick a budget. By day 90, measure the 90-day re-failure rate against the baseline, then scale area by area.

## 4:45 · Slide 18 · Conclusion (Angie, 15 s)

> In short: recent repeat failures, not long history, point to the wells to fix. Fix the top 25, measure in 90 days, and let the model earn its place. Thank you.

---

## If we reach the finals (10-minute version)

Use the same deck. Keep every line above, and add these at a slower pace (about 1.5 minutes per section):

- **Approach, slide 8, plus appendix A1:** the stop-day alignment. Failures are logged a median 2 days after the well stops, so we anchor on the stop. Also how the out-of-time splits work, and the bootstrap intervals (16.5%, 95% interval 14.3–19.9%).
- **Payback, slide 12:** the sensitivity. Netback ±25% moves payback by only about 0.1 years. Halving the action cost halves payback. Also the Cactus Lake pump-speed finding (0.55 vs 1.50 rod failures per well-year), presented as an association.
- **Early warning, slide 13, plus appendix A5:** the three alert tiers, and the model's features and time split. The model's largest weights are operating area, which a rule could also use.
- **Dashboard:** spend longer on Return on Action (budget scenarios and the well list) and on the Validation & Data Quality tab.
- **Implementation, appendix A2:** money (action cost assumed, netback sourced), manpower, logistics (act when the rig is already on the well) and time (impact shows within a quarter).
- **Risks, appendix A3:** walk through the top three: smaller action effect, cost estimates vs invoices, and alert fatigue, each with its mitigation.
- **Limitations, appendix A4:** each one says which way it moves the result. Most make the payback conservative.
- **Data trust, appendix A6:** cleaning doesn't change the top 100 wells.

Have these appendix answers ready for likely judge questions:

| Question | Answer |
|---|---|
| Why not lead with ML? | It beats the rules by 3–7 points of hit rate, but on one time split with a short failure log. We pilot it next to the rules. |
| How sure is the payback? | Action cost and the size of the effect are assumptions. We show the range (25–50% fewer failures) and the sensitivity. |
| Is it causal? | No. The equipment and RPM differences are associations. Trial the changes with comparison wells. |
| Does cleaning drive the ranking? | No. The top 100 wells are identical with and without cleaning. |
