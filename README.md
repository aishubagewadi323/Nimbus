# Nimbus

# NimbusAI — Customer Churn & Retention Analysis

**Author:** Aishwarya  
**Tools:** PostgreSQL · MongoDB · Python · Power BI

---

## Overview

NimbusAI is a B2B SaaS platform with 1,204 customers. This project investigates a 22.1% churn rate by combining structured business data from PostgreSQL with real-time behavioral data from MongoDB. The goal is to identify why customers leave and recommend specific, data-backed retention actions.

---

## Data Sources

| Database | Type | Contains |
|---|---|---|
| nimbus_core | PostgreSQL | customers, subscriptions, billing_invoices, support_tickets, plans, team_members, feature_flags |
| nimbus_events | MongoDB | user_activity_logs (51,485 docs), onboarding_events (8,000 docs), nps_survey_responses (3,000 docs) |

---

## Files

```
NimbusAI-Assignment/
│
├── nimbus_TASK1.sql                  # SQL queries — PostgreSQL
├── nimbus_mongo_queries_clean.py     # MongoDB aggregation pipelines
├── nimbus_tasks2_3.py                # Data wrangling, hypothesis test, segmentation
├── NimbusAI_Churn_Dashboard.pbix     # Power BI interactive dashboard
├── NimbusAI_AllTables.xlsx           # Full data export from both databases
└── README.md
```

---

# Nimbus Data Analyst Assignment

## Tasks Completed
- Task 1: SQL Queries (PostgreSQL)
- Task 2: MongoDB Aggregations
- Task 3: Python Analysis
- Task 4: Power BI Dashboard

## Key Insights
- High churn among detractors (39.5%)
- Major drop-off in onboarding funnel
- Revenue growing steadily

## Tools Used
- PostgreSQL (pgAdmin)
- MongoDB
- Python (pandas, scipy)
- Power BI
## Setup & Running

### PostgreSQL Queries
```sql
-- Open nimbus_TASK1.sql in pgAdmin 4
-- Run this first:
SET search_path TO nimbus;
```

### MongoDB Queries
```bash
# Load data
mongosh nimbus_events "path/to/nimbus_events.js"

# Install and run
pip install pymongo pandas
python nimbus_mongo_queries_clean.py
```

### Python Analysis
```bash
# Extract NimbusAI_Tasks2_3.zip first (includes data folder)
pip install pandas numpy matplotlib seaborn scipy openpyxl
python nimbus_tasks2_3.py

# Output: terminal results + nimbus_analysis_charts.png
```

---

## Data Cleaning

The dataset contained several intentional real-world quality issues that were handled as follows:

- **Mixed ID types** — customer_id stored as both int and string in MongoDB; coerced to int
- **Inconsistent field names** — member_id / userId / userID normalized to a single field
- **Duplicate events** — 4,154 duplicate activity log records removed by matching customer_id, timestamp, and event_type
- **Outliers** — session duration capped at 3x IQR to remove idle/bot sessions
- **Timezone mismatch** — MongoDB UTC timestamps stripped of timezone info to align with SQL
- **NULL NPS scores** — retained as a separate "No Score" segment rather than imputed

Before: 51,485 activity records | After: 47,331 records  
Master dataframe: 1,204 customers × 26 features

---

## Hypothesis Test

**Question:** Does NPS score predict churn?

| | |
|---|---|
| H0 | NPS segment has no relationship with churn |
| H1 | NPS segment is significantly associated with churn |
| Test | Chi-Square (both variables are categorical) |
| Result | Chi² = 98.5, p < 0.0001 → Reject H0 |

Detractors (NPS ≤ 6) churn at **39.5%** vs **9.9%** for Promoters — a 4x difference.

---

## Customer Segmentation

Customers were scored using a composite health score:

```
Health Score = LTV * 0.35 + Engagement * 0.30 + NPS * 0.25 - Risk * 0.10
```

Segmented into 4 groups using quartiles:

| Segment | Count | Churn Rate | Avg LTV |
|---|---|---|---|
| Champions | 297 | 13.8% | $2,678 |
| Healthy | 305 | 15.7% | $1,842 |
| Needs Attention | 298 | 22.8% | $1,367 |
| At Risk | 304 | 35.9% | $944 |

---

## Dashboard

5 visualizations built in Power BI:

1. Churn Rate by Plan Tier
2. Monthly Revenue Growth by Plan Tier
3. NPS Score vs Churn Rate *(combines SQL + MongoDB data)*
4. Onboarding Funnel — Where Users Drop Off
5. Monthly Support Ticket Volume by Priority

Interactive filters: **Plan Tier** · **Date Range**

---

## Key Findings

**1. NPS predicts churn before it happens**  
Detractors churn at 39.5% vs 9.9% for Promoters. NPS is a leading indicator — by the time a customer churns, the warning signal was already there.

**2. Solo users churn — team users stay**  
51.6% of users who create a project never invite a teammate. Team adoption eliminates switching cost and is the strongest retention lever in the data.

**3. Free-tier power users are untapped revenue**  
20 free-tier users average 34 sessions and use 7–9 features — enterprise-level engagement with zero revenue contribution.

---

## Recommendations

| Priority | Action | Expected Impact |
|---|---|---|
| High | Contact NPS Detractors within 48hrs of score ≤ 6 | Reduce churn by ~2 percentage points |
| High | Add Day-2 nudge to invite a teammate | Fix 51.6% onboarding drop-off |
| Medium | Offer 20% discount to top 20 free-tier power users | Convert to paid with zero acquisition cost |
