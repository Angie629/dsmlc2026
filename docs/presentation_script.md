# Presentation script: Bad Actors (5-minute video)

Deck: `docs/Bad Actors - DSMLC Datathon 2026.pptx` (slides 1–18 are the talk; slides 19–26 are the appendix). The same lines are in each slide's speaker notes.

Target: about 700 words, 5:00. Times are cumulative. Speakers are a suggestion; swap freely.

For the dashboard demo, record a screen capture of the dashboard in Databricks. If the workspace is over its compute limit, use the screenshots in `dashboard/` instead and say the same lines over them.

---

## 0:00 · Slides 1–2 · Title and contents (Angie, 15 s)

> We're Ananya, Angie and Jacob. Our project is called Bad Actors: finding the wells that keep failing, and showing which fixes pay back. We'll cover our question, our approach, what we found, and our plan.

## 0:15 · Slides 3–5 · The question and the problem (Angie, 35 s)

> Our question: which producing wells will drive Strathcona's repeat downhole failures and lost oil next period, and which targeted actions on those wells pay back?
>
> Here's why it matters. Rod truck and workover repairs cost about 33 million dollars a year, and 57 percent of failures come back on the same well within 90 days. The same wells keep costing money.

## 0:50 · Slides 6–8 · Our approach (Jacob, 40 s)

> We built a pipeline in Databricks: raw files to cleaned tables to analysis tables to a dashboard. Every cleaning rule is logged, and cleaning doesn't change the ranking.
>
> Then we asked four questions, and tested each one on data the method hadn't seen. Can history rank the wells? Which two actions pay back? Can we warn before a failure? And does every result hold on 2026 data we held back?

## 1:30 · Slides 9–11 · Findings: failures repeat (Jacob, 45 s)

> The strongest signal is recent failures. A well with no failures in the last 90 days fails again 26 percent of the time. With two or more, it's 61 to 70 percent. Today 114 wells sit on that watchlist, holding a quarter of last year's repair spend.
>
> Long history helps less than you'd expect. Our cost-based ranking catches 16.5 percent of later cost in the top 10 percent of wells. Ranking by well size catches about the same. So we lead with recent failures and use cost second.

## 2:15 · Slide 12 · Payback (Ananya, 30 s)

> Rods are three quarters of failures, but pump and tubing failures cost the most per repair. So we priced two actions: rod string upgrades or corrosion inhibition, and pump and tubing failure prevention. If they cut failures by half, both pay back in about two years; at a quarter, three and a half to four. Treating the top 25 wells costs 850 thousand dollars and pays back in one to two years.

## 2:45 · Slide 13 · Early warning (Ananya, 30 s)

> For early warning, the simple watchlist was right 29 percent of the time in 2026. That's better than a sensor rule on partial-run days. Together they catch 813 of 1,432 failures, and alerts where both fire are right 34 percent of the time. We also tested a simple machine-learning model at the same workload. It does better, by 4 to 7 points, so it's our next step rather than our first.

## 3:15 · Slide 14 · Dashboard demo (Angie, 60 s)

Screen recording. Move through the tabs while speaking.

> This is our Reliability Investment Dashboard in Databricks.
>
> *Cost Drivers:* where the repair dollars and lost oil go, by failure type and area.
>
> *Work Order Priorities:* this week's list. Filter to Cactus Lake. Wells with recent failures are at the top, then cost, with a suggested action for each.
>
> *Return on Action:* pick a budget scenario, say the top 50 wells, and you see the cost, savings and payback. The sensitivity table goes green when payback is under two years.
>
> *Model Validation:* the out-of-time test and how each alert rule did in 2026.
>
> A foreman can open this on Monday morning and know where to send a crew.

## 4:15 · Slides 15–17 · Recommendation and 90-day pilot (Jacob, 35 s)

> We recommend three things. Work the list every week, recent failures first. Fund the top 25 bad actors for 850 thousand dollars. And pilot a model next to the rules.
>
> Start in Cactus Lake, where the money is, and Winter, where failures repeat most. Together they have 62 percent of failures. In the first 30 days, run weekly reviews. By day 60, pick a budget. By day 90, measure the 90-day re-failure rate against the baseline, then scale area by area.

## 4:50 · Slide 18 · Conclusion (Angie, 10 s)

> In short: recent repeat failures, not long history, point to the wells to fix. Fix the top 25 and measure in 90 days. Thank you.

---

## If we reach the finals (10-minute version)

Use the same deck. Keep every line above, and add these at a slower pace (about 1.5 minutes per section):

- **Approach, slide 8, plus appendix A1:** the stop-day alignment. Failures are logged a median 2 days after the well stops, so we anchor on the stop. Also how the out-of-time splits work, and the bootstrap intervals (16.5%, 95% interval 14.3–19.9%).
- **Payback, slide 12:** the sensitivity. Netback ±25% moves payback by only about 0.1 years. Halving the action cost halves payback. Also the Cactus Lake pump-speed finding (0.55 vs 1.50 rod failures per well-year), presented as an association.
- **Early warning, slide 13, plus appendix A5:** the three alert tiers, and the model's features and time split. The model's largest weights are operating area, which a rule could also use.
- **Implementation, appendix A2:** money (action cost assumed, netback sourced), manpower, logistics (act when the rig is already on the well) and time (impact shows within a quarter).
- **Risks, appendix A3:** walk through the top three: smaller action effect, cost estimates vs invoices, and alert fatigue, each with its mitigation.
- **Limitations, appendix A4:** each one says which way it moves the result. Most make the payback conservative.
- **Data trust, appendix A6:** cleaning doesn't change the top 100 wells.

Have these appendix answers ready for likely judge questions:

| Question | Answer |
|---|---|
| Why not lead with ML? | It beats the rules by 4–7 points of hit rate, but on one time split with a short failure log. We pilot it next to the rules. |
| How sure is the payback? | Action cost and the size of the effect are assumptions. We show the range (25–50% fewer failures) and the sensitivity. |
| Is it causal? | No. The equipment and RPM differences are associations. Trial the changes with comparison wells. |
| Does cleaning drive the ranking? | No. The top 100 wells are identical with and without cleaning. |
