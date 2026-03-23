"""
NimbusAI - Task 2: MongoDB Aggregation Pipelines
Focus: Customer Churn & Retention Analysis
Database: nimbus_events
Collections: user_activity_logs, onboarding_events, nps_survey_responses

Run: python nimbus_mongo_queries.py
Requires: pymongo, pandas
Install: pip install pymongo pandas
"""

from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["nimbus_events"]

print("Connected to nimbus_events database")
print("=" * 60)


# ============================================================
# Q1: Average sessions per user per week by subscription tier
# Include 25th, 50th, 75th percentile session durations
# ============================================================
# Approach:
#   Step 1 - Normalize inconsistent ID fields (member_id / userId / userID)
#   Step 2 - Group by user + ISO week to count weekly sessions
#   Step 3 - Average weekly session counts per user
#   Step 4 - Compute duration percentiles using sortArray
# ============================================================

print("\nQ1: Average Sessions per User per Week")
print("-" * 60)

pipeline_q1 = [
    {
        "$addFields": {
            "norm_member_id": {
                "$ifNull": ["$member_id", {"$ifNull": ["$userId", "$userID"]}]
            },
            "norm_customer_id": {
                "$toInt": {"$ifNull": ["$customer_id", "$customerId"]}
            },
            "parsed_ts": {"$toDate": "$timestamp"}
        }
    },
    {
        "$addFields": {
            "year_week": {
                "$dateToString": {"format": "%G-%V", "date": "$parsed_ts"}
            }
        }
    },
    {
        "$group": {
            "_id": {
                "customer_id": "$norm_customer_id",
                "member_id": "$norm_member_id",
                "week": "$year_week"
            },
            "sessions_this_week": {"$sum": 1},
            "durations": {"$push": "$session_duration_sec"}
        }
    },
    {
        "$group": {
            "_id": {
                "customer_id": "$_id.customer_id",
                "member_id": "$_id.member_id"
            },
            "avg_sessions_per_week": {"$avg": "$sessions_this_week"},
            "all_durations": {"$push": "$durations"}
        }
    },
    {
        "$addFields": {
            "flat_durations": {
                "$reduce": {
                    "input": "$all_durations",
                    "initialValue": [],
                    "in": {"$concatArrays": ["$$value", "$$this"]}
                }
            }
        }
    },
    {
        "$group": {
            "_id": None,
            "avg_sessions_per_user_per_week": {"$avg": "$avg_sessions_per_week"},
            "user_count": {"$sum": 1},
            "all_durations": {"$push": "$flat_durations"}
        }
    },
    {
        "$addFields": {
            "durations_flat": {
                "$reduce": {
                    "input": "$all_durations",
                    "initialValue": [],
                    "in": {"$concatArrays": ["$$value", "$$this"]}
                }
            }
        }
    },
    {
        "$addFields": {
            "sorted_durations": {
                "$sortArray": {"input": "$durations_flat", "sortBy": 1}
            },
            "n": {"$size": "$durations_flat"}
        }
    },
    {
        "$addFields": {
            "p25_duration_sec": {
                "$arrayElemAt": [
                    "$sorted_durations",
                    {"$floor": {"$multiply": [0.25, "$n"]}}
                ]
            },
            "p50_duration_sec": {
                "$arrayElemAt": [
                    "$sorted_durations",
                    {"$floor": {"$multiply": [0.50, "$n"]}}
                ]
            },
            "p75_duration_sec": {
                "$arrayElemAt": [
                    "$sorted_durations",
                    {"$floor": {"$multiply": [0.75, "$n"]}}
                ]
            }
        }
    },
    {
        "$project": {
            "_id": 0,
            "user_count": 1,
            "avg_sessions_per_user_per_week": {
                "$round": ["$avg_sessions_per_user_per_week", 2]
            },
            "p25_session_duration_sec": "$p25_duration_sec",
            "p50_session_duration_sec": "$p50_duration_sec",
            "p75_session_duration_sec": "$p75_duration_sec"
        }
    }
]

results_q1 = list(db.user_activity_logs.aggregate(pipeline_q1))
for r in results_q1:
    print(r)


# ============================================================
# Q2: DAU and 7-day retention rate per product feature
# DAU = distinct users per feature per day
# 7-day retention = returned within 7 days of first use
# ============================================================

print("\nQ2: DAU and 7-Day Retention by Feature")
print("-" * 60)

pipeline_q2_dau = [
    {
        "$match": {
            "feature": {"$exists": True, "$ne": None, "$ne": ""}
        }
    },
    {
        "$addFields": {
            "norm_member_id": {
                "$ifNull": ["$member_id", {"$ifNull": ["$userId", "$userID"]}]
            },
            "event_date": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": {"$toDate": "$timestamp"}
                }
            }
        }
    },
    {
        "$group": {
            "_id": {"feature": "$feature", "date": "$event_date"},
            "daily_users": {"$addToSet": "$norm_member_id"}
        }
    },
    {"$addFields": {"dau": {"$size": "$daily_users"}}},
    {
        "$group": {
            "_id": "$_id.feature",
            "avg_dau": {"$avg": "$dau"},
            "peak_dau": {"$max": "$dau"},
            "days_active": {"$sum": 1}
        }
    },
    {"$sort": {"avg_dau": -1}}
]

pipeline_q2_retention = [
    {
        "$match": {
            "feature": {"$exists": True, "$ne": None, "$ne": ""}
        }
    },
    {
        "$addFields": {
            "norm_member_id": {
                "$ifNull": ["$member_id", {"$ifNull": ["$userId", "$userID"]}]
            },
            "parsed_ts": {"$toDate": "$timestamp"}
        }
    },
    {
        "$group": {
            "_id": {
                "feature": "$feature",
                "member_id": "$norm_member_id"
            },
            "first_use": {"$min": "$parsed_ts"},
            "last_use": {"$max": "$parsed_ts"}
        }
    },
    {
        "$addFields": {
            "returned_within_7d": {
                "$and": [
                    {"$gt": ["$last_use", "$first_use"]},
                    {
                        "$lte": [
                            {"$subtract": ["$last_use", "$first_use"]},
                            {"$multiply": [7, 24, 60, 60, 1000]}
                        ]
                    }
                ]
            }
        }
    },
    {
        "$group": {
            "_id": "$_id.feature",
            "total_users": {"$sum": 1},
            "retained_users": {
                "$sum": {"$cond": ["$returned_within_7d", 1, 0]}
            }
        }
    },
    {
        "$addFields": {
            "retention_rate_7d_pct": {
                "$round": [
                    {
                        "$multiply": [
                            {"$divide": ["$retained_users", "$total_users"]},
                            100
                        ]
                    },
                    1
                ]
            }
        }
    },
    {"$sort": {"retention_rate_7d_pct": -1}}
]

dau_results = list(db.user_activity_logs.aggregate(pipeline_q2_dau))
ret_results = list(db.user_activity_logs.aggregate(pipeline_q2_retention))

dau_df = pd.DataFrame(dau_results).rename(columns={"_id": "feature"})
ret_df = pd.DataFrame(ret_results).rename(columns={"_id": "feature"})
q2 = ret_df.merge(dau_df, on="feature", how="left")
print(q2[["feature", "avg_dau", "peak_dau", "total_users",
          "retention_rate_7d_pct"]].to_string(index=False))


# ============================================================
# Q3: Onboarding Funnel
# signup > first_login > workspace_created >
# first_project > invited_teammate
# Drop-off rates + median time between steps in hours
# ============================================================

print("\nQ3: Onboarding Funnel Analysis")
print("-" * 60)

FUNNEL_STEPS = [
    "signup", "first_login", "workspace_created",
    "first_project", "invited_teammate"
]

pipeline_q3 = [
    {
        "$addFields": {
            "cust_id": {
                "$toInt": {"$ifNull": ["$customer_id", "$customerId"]}
            },
            "parsed_ts": {"$toDate": "$timestamp"},
            "is_done": {
                "$or": [
                    {"$eq": ["$completed", True]},
                    {"$eq": ["$completed", "true"]},
                    {"$eq": ["$completed", 1]}
                ]
            }
        }
    },
    {
        "$match": {
            "step": {"$in": FUNNEL_STEPS},
            "is_done": True
        }
    },
    {
        "$group": {
            "_id": "$cust_id",
            "steps": {
                "$push": {"step": "$step", "ts": "$parsed_ts"}
            }
        }
    },
    {
        "$addFields": {
            "signup_ts": {
                "$min": {
                    "$map": {
                        "input": {
                            "$filter": {
                                "input": "$steps",
                                "cond": {"$eq": ["$$this.step", "signup"]}
                            }
                        },
                        "in": "$$this.ts"
                    }
                }
            },
            "login_ts": {
                "$min": {
                    "$map": {
                        "input": {
                            "$filter": {
                                "input": "$steps",
                                "cond": {"$eq": ["$$this.step", "first_login"]}
                            }
                        },
                        "in": "$$this.ts"
                    }
                }
            },
            "workspace_ts": {
                "$min": {
                    "$map": {
                        "input": {
                            "$filter": {
                                "input": "$steps",
                                "cond": {
                                    "$eq": ["$$this.step", "workspace_created"]
                                }
                            }
                        },
                        "in": "$$this.ts"
                    }
                }
            },
            "project_ts": {
                "$min": {
                    "$map": {
                        "input": {
                            "$filter": {
                                "input": "$steps",
                                "cond": {
                                    "$eq": ["$$this.step", "first_project"]
                                }
                            }
                        },
                        "in": "$$this.ts"
                    }
                }
            },
            "teammate_ts": {
                "$min": {
                    "$map": {
                        "input": {
                            "$filter": {
                                "input": "$steps",
                                "cond": {
                                    "$eq": ["$$this.step", "invited_teammate"]
                                }
                            }
                        },
                        "in": "$$this.ts"
                    }
                }
            }
        }
    },
    {
        "$group": {
            "_id": None,
            "total":         {"$sum": 1},
            "did_signup":    {"$sum": {"$cond": [{"$ne": ["$signup_ts",    None]}, 1, 0]}},
            "did_login":     {"$sum": {"$cond": [{"$ne": ["$login_ts",     None]}, 1, 0]}},
            "did_workspace": {"$sum": {"$cond": [{"$ne": ["$workspace_ts", None]}, 1, 0]}},
            "did_project":   {"$sum": {"$cond": [{"$ne": ["$project_ts",   None]}, 1, 0]}},
            "did_teammate":  {"$sum": {"$cond": [{"$ne": ["$teammate_ts",  None]}, 1, 0]}},
            "gap_s_l": {
                "$push": {
                    "$cond": [
                        {"$and": [
                            {"$ne": ["$signup_ts", None]},
                            {"$ne": ["$login_ts", None]}
                        ]},
                        {"$subtract": ["$login_ts", "$signup_ts"]},
                        "$$REMOVE"
                    ]
                }
            },
            "gap_l_w": {
                "$push": {
                    "$cond": [
                        {"$and": [
                            {"$ne": ["$login_ts", None]},
                            {"$ne": ["$workspace_ts", None]}
                        ]},
                        {"$subtract": ["$workspace_ts", "$login_ts"]},
                        "$$REMOVE"
                    ]
                }
            },
            "gap_w_p": {
                "$push": {
                    "$cond": [
                        {"$and": [
                            {"$ne": ["$workspace_ts", None]},
                            {"$ne": ["$project_ts", None]}
                        ]},
                        {"$subtract": ["$project_ts", "$workspace_ts"]},
                        "$$REMOVE"
                    ]
                }
            },
            "gap_p_t": {
                "$push": {
                    "$cond": [
                        {"$and": [
                            {"$ne": ["$project_ts", None]},
                            {"$ne": ["$teammate_ts", None]}
                        ]},
                        {"$subtract": ["$teammate_ts", "$project_ts"]},
                        "$$REMOVE"
                    ]
                }
            }
        }
    },
    {
        "$addFields": {
            "s1": {"$sortArray": {"input": "$gap_s_l", "sortBy": 1}},
            "s2": {"$sortArray": {"input": "$gap_l_w", "sortBy": 1}},
            "s3": {"$sortArray": {"input": "$gap_w_p", "sortBy": 1}},
            "s4": {"$sortArray": {"input": "$gap_p_t", "sortBy": 1}},
            "login_conv_pct":     {"$round": [{"$multiply": [{"$divide": ["$did_login",     "$did_signup"]},     100]}, 1]},
            "workspace_conv_pct": {"$round": [{"$multiply": [{"$divide": ["$did_workspace", "$did_login"]},      100]}, 1]},
            "project_conv_pct":   {"$round": [{"$multiply": [{"$divide": ["$did_project",   "$did_workspace"]},  100]}, 1]},
            "teammate_conv_pct":  {"$round": [{"$multiply": [{"$divide": ["$did_teammate",  "$did_project"]},    100]}, 1]}
        }
    },
    {
        "$project": {
            "_id": 0,
            "signup":                    "$did_signup",
            "first_login":               "$did_login",
            "workspace_created":         "$did_workspace",
            "first_project":             "$did_project",
            "invited_teammate":          "$did_teammate",
            "login_conversion_pct":      "$login_conv_pct",
            "workspace_conversion_pct":  "$workspace_conv_pct",
            "project_conversion_pct":    "$project_conv_pct",
            "teammate_conversion_pct":   "$teammate_conv_pct",
            "dropoff_login_pct":         {"$subtract": [100, "$login_conv_pct"]},
            "dropoff_workspace_pct":     {"$subtract": [100, "$workspace_conv_pct"]},
            "dropoff_project_pct":       {"$subtract": [100, "$project_conv_pct"]},
            "dropoff_teammate_pct":      {"$subtract": [100, "$teammate_conv_pct"]},
            "median_signup_to_login_hrs": {
                "$round": [{"$divide": [
                    {"$arrayElemAt": ["$s1", {"$floor": {"$divide": [{"$size": "$s1"}, 2]}}]},
                    3600000
                ]}, 1]
            },
            "median_login_to_workspace_hrs": {
                "$round": [{"$divide": [
                    {"$arrayElemAt": ["$s2", {"$floor": {"$divide": [{"$size": "$s2"}, 2]}}]},
                    3600000
                ]}, 1]
            },
            "median_workspace_to_project_hrs": {
                "$round": [{"$divide": [
                    {"$arrayElemAt": ["$s3", {"$floor": {"$divide": [{"$size": "$s3"}, 2]}}]},
                    3600000
                ]}, 1]
            },
            "median_project_to_teammate_hrs": {
                "$round": [{"$divide": [
                    {"$arrayElemAt": ["$s4", {"$floor": {"$divide": [{"$size": "$s4"}, 2]}}]},
                    3600000
                ]}, 1]
            }
        }
    }
]

results_q3 = list(db.onboarding_events.aggregate(pipeline_q3))
for r in results_q3:
    for k, v in r.items():
        print(f"  {k}: {v}")


# ============================================================
# Q4: Top 20 most engaged FREE-tier users — upsell targets
#
# Engagement Score:
#   ES = session_count * 1.0
#      + avg_duration_min * 0.5
#      + distinct_features * 2.0  (highest weight — breadth
#        signals upgrade intent as user hits free tier limits)
#      + recent_sessions_30d * 1.5 (recency = warm lead)
#
# FREE TIER IDs from PostgreSQL:
#   SELECT customer_id FROM nimbus.subscriptions s
#   JOIN nimbus.plans p ON s.plan_id = p.plan_id
#   WHERE p.plan_tier = 'free'
#     AND (s.end_date IS NULL OR s.end_date >= CURRENT_DATE)
# ============================================================

print("\nQ4: Top 20 Free-Tier Users — Upsell Targets")
print("-" * 60)

FREE_TIER_IDS = [
    9, 23, 31, 55, 61, 76, 89, 101, 109, 118,
    130, 154, 179, 203, 218, 230, 245, 260, 271, 281,
    299, 311, 325, 338, 352, 365, 388, 401, 415, 422
]

THIRTY_DAYS_AGO = datetime.utcnow() - timedelta(days=30)

pipeline_q4 = [
    {
        "$addFields": {
            "norm_customer_id": {
                "$toInt": {"$ifNull": ["$customer_id", "$customerId"]}
            },
            "norm_member_id": {
                "$ifNull": ["$member_id", {"$ifNull": ["$userId", "$userID"]}]
            },
            "parsed_ts":    {"$toDate": "$timestamp"},
            "duration_sec": {"$toDouble": "$session_duration_sec"}
        }
    },
    {"$match": {"norm_customer_id": {"$in": FREE_TIER_IDS}}},
    {
        "$addFields": {
            "is_recent": {"$gte": ["$parsed_ts", THIRTY_DAYS_AGO]},
            "event_date": {
                "$dateToString": {
                    "format": "%Y-%m-%d", "date": "$parsed_ts"
                }
            }
        }
    },
    {
        "$group": {
            "_id": {
                "customer_id": "$norm_customer_id",
                "member_id":   "$norm_member_id"
            },
            "session_count":     {"$sum": 1},
            "avg_duration_sec":  {"$avg": "$duration_sec"},
            "distinct_features": {"$addToSet": "$feature"},
            "active_days":       {"$addToSet": "$event_date"},
            "recent_sessions":   {
                "$sum": {"$cond": ["$is_recent", 1, 0]}
            }
        }
    },
    {
        "$addFields": {
            "distinct_features_count": {"$size": "$distinct_features"},
            "active_days_count":       {"$size": "$active_days"},
            "avg_duration_min": {
                "$divide": [
                    {"$ifNull": ["$avg_duration_sec", 0]}, 60
                ]
            },
            "engagement_score": {
                "$add": [
                    {"$multiply": ["$session_count", 1.0]},
                    {"$multiply": [
                        {"$divide": [
                            {"$ifNull": ["$avg_duration_sec", 0]}, 60
                        ]},
                        0.5
                    ]},
                    {"$multiply": [
                        {"$size": "$distinct_features"}, 2.0
                    ]},
                    {"$multiply": ["$recent_sessions", 1.5]}
                ]
            }
        }
    },
    {"$sort": {"engagement_score": -1}},
    {"$limit": 20},
    {
        "$project": {
            "_id":                0,
            "customer_id":        "$_id.customer_id",
            "member_id":          "$_id.member_id",
            "session_count":      1,
            "avg_session_min":    {"$round": ["$avg_duration_min", 1]},
            "distinct_features":  "$distinct_features_count",
            "active_days":        "$active_days_count",
            "recent_sessions_30d":"$recent_sessions",
            "engagement_score":   {"$round": ["$engagement_score", 2]}
        }
    }
]

results_q4 = list(db.user_activity_logs.aggregate(pipeline_q4))
for i, r in enumerate(results_q4, 1):
    print(f"  #{i}: customer_id={r.get('customer_id')} | "
          f"sessions={r.get('session_count')} | "
          f"features={r.get('distinct_features')} | "
          f"score={r.get('engagement_score')}")

client.close()
print("\n" + "=" * 60)
print("All 4 MongoDB queries complete.")
