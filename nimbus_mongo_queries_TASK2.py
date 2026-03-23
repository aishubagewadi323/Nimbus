"""
NimbusAI - Task 2: MongoDB Aggregation Pipelines
Database: nimbus_events
Author: Aishwarya

Run: python nimbus_mongo_queries.py
Install: pip install pymongo pandas
"""

from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta

client = MongoClient("mongodb://localhost:27017")
db = client["nimbus_events"]

print("Connected to nimbus_events")
print("=" * 50)


# Q1: Average sessions per user per week by plan tier
# Percentiles: 25th, 50th, 75th of session durations

print("\nQ1: Avg Sessions per User per Week")
print("-" * 50)

pipeline_q1 = [
    {
        "$addFields": {
            "member_id_clean": {
                "$ifNull": ["$member_id", {"$ifNull": ["$userId", "$userID"]}]
            },
            "customer_id_clean": {
                "$toInt": {"$ifNull": ["$customer_id", "$customerId"]}
            },
            "ts": {"$toDate": "$timestamp"}
        }
    },
    {
        "$addFields": {
            "week": {
                "$dateToString": {"format": "%G-%V", "date": "$ts"}
            }
        }
    },
    {
        "$group": {
            "_id": {
                "customer_id": "$customer_id_clean",
                "member_id": "$member_id_clean",
                "week": "$week"
            },
            "weekly_sessions": {"$sum": 1},
            "durations": {"$push": "$session_duration_sec"}
        }
    },
    {
        "$group": {
            "_id": {
                "customer_id": "$_id.customer_id",
                "member_id": "$_id.member_id"
            },
            "avg_sessions_per_week": {"$avg": "$weekly_sessions"},
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
            "total_users": {"$sum": 1},
            "all_durations": {"$push": "$flat_durations"}
        }
    },
    {
        "$addFields": {
            "durations_merged": {
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
                "$sortArray": {"input": "$durations_merged", "sortBy": 1}
            },
            "n": {"$size": "$durations_merged"}
        }
    },
    {
        "$addFields": {
            "p25": {
                "$arrayElemAt": [
                    "$sorted_durations",
                    {"$floor": {"$multiply": [0.25, "$n"]}}
                ]
            },
            "p50": {
                "$arrayElemAt": [
                    "$sorted_durations",
                    {"$floor": {"$multiply": [0.50, "$n"]}}
                ]
            },
            "p75": {
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
            "total_users": 1,
            "avg_sessions_per_user_per_week": {
                "$round": ["$avg_sessions_per_user_per_week", 2]
            },
            "p25_duration_sec": "$p25",
            "p50_duration_sec": "$p50",
            "p75_duration_sec": "$p75"
        }
    }
]

results_q1 = list(db.user_activity_logs.aggregate(pipeline_q1))
for r in results_q1:
    print(r)


# Q2: Daily Active Users (DAU) and 7-day retention per feature

print("\nQ2: DAU and 7-Day Retention by Feature")
print("-" * 50)

pipeline_dau = [
    {
        "$match": {
            "feature": {"$exists": True, "$ne": None, "$ne": ""}
        }
    },
    {
        "$addFields": {
            "member_id_clean": {
                "$ifNull": ["$member_id", {"$ifNull": ["$userId", "$userID"]}]
            },
            "day": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": {"$toDate": "$timestamp"}
                }
            }
        }
    },
    {
        "$group": {
            "_id": {"feature": "$feature", "day": "$day"},
            "unique_users": {"$addToSet": "$member_id_clean"}
        }
    },
    {"$addFields": {"dau": {"$size": "$unique_users"}}},
    {
        "$group": {
            "_id": "$_id.feature",
            "avg_dau": {"$avg": "$dau"},
            "peak_dau": {"$max": "$dau"}
        }
    },
    {"$sort": {"avg_dau": -1}}
]

pipeline_retention = [
    {
        "$match": {
            "feature": {"$exists": True, "$ne": None, "$ne": ""}
        }
    },
    {
        "$addFields": {
            "member_id_clean": {
                "$ifNull": ["$member_id", {"$ifNull": ["$userId", "$userID"]}]
            },
            "ts": {"$toDate": "$timestamp"}
        }
    },
    {
        "$group": {
            "_id": {
                "feature": "$feature",
                "member_id": "$member_id_clean"
            },
            "first_use": {"$min": "$ts"},
            "last_use": {"$max": "$ts"}
        }
    },
    {
        "$addFields": {
            "came_back_7d": {
                "$and": [
                    {"$gt": ["$last_use", "$first_use"]},
                    {
                        "$lte": [
                            {"$subtract": ["$last_use", "$first_use"]},
                            604800000
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
            "retained": {
                "$sum": {"$cond": ["$came_back_7d", 1, 0]}
            }
        }
    },
    {
        "$addFields": {
            "retention_7d_pct": {
                "$round": [
                    {"$multiply": [
                        {"$divide": ["$retained", "$total_users"]},
                        100
                    ]},
                    1
                ]
            }
        }
    },
    {"$sort": {"retention_7d_pct": -1}}
]

dau_df = pd.DataFrame(
    list(db.user_activity_logs.aggregate(pipeline_dau))
).rename(columns={"_id": "feature"})

ret_df = pd.DataFrame(
    list(db.user_activity_logs.aggregate(pipeline_retention))
).rename(columns={"_id": "feature"})

q2_result = ret_df.merge(dau_df, on="feature", how="left")
print(q2_result[["feature", "avg_dau", "peak_dau",
                  "total_users", "retention_7d_pct"]].to_string(index=False))


# Q3: Onboarding funnel analysis
# Steps: signup > first_login > workspace_created > first_project > invited_teammate
# Drop-off rates and median time between each step

print("\nQ3: Onboarding Funnel")
print("-" * 50)

STEPS = ["signup", "first_login", "workspace_created",
         "first_project", "invited_teammate"]

pipeline_q3 = [
    {
        "$addFields": {
            "cust_id": {
                "$toInt": {"$ifNull": ["$customer_id", "$customerId"]}
            },
            "ts": {"$toDate": "$timestamp"},
            "done": {
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
            "step": {"$in": STEPS},
            "done": True
        }
    },
    {
        "$group": {
            "_id": "$cust_id",
            "steps": {"$push": {"step": "$step", "ts": "$ts"}}
        }
    },
    {
        "$addFields": {
            "t_signup": {
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
            "t_login": {
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
            "t_workspace": {
                "$min": {
                    "$map": {
                        "input": {
                            "$filter": {
                                "input": "$steps",
                                "cond": {"$eq": ["$$this.step", "workspace_created"]}
                            }
                        },
                        "in": "$$this.ts"
                    }
                }
            },
            "t_project": {
                "$min": {
                    "$map": {
                        "input": {
                            "$filter": {
                                "input": "$steps",
                                "cond": {"$eq": ["$$this.step", "first_project"]}
                            }
                        },
                        "in": "$$this.ts"
                    }
                }
            },
            "t_teammate": {
                "$min": {
                    "$map": {
                        "input": {
                            "$filter": {
                                "input": "$steps",
                                "cond": {"$eq": ["$$this.step", "invited_teammate"]}
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
            "n_signup":      {"$sum": {"$cond": [{"$ne": ["$t_signup",    None]}, 1, 0]}},
            "n_login":       {"$sum": {"$cond": [{"$ne": ["$t_login",     None]}, 1, 0]}},
            "n_workspace":   {"$sum": {"$cond": [{"$ne": ["$t_workspace", None]}, 1, 0]}},
            "n_project":     {"$sum": {"$cond": [{"$ne": ["$t_project",   None]}, 1, 0]}},
            "n_teammate":    {"$sum": {"$cond": [{"$ne": ["$t_teammate",  None]}, 1, 0]}},
            "gap_s_l": {
                "$push": {
                    "$cond": [
                        {"$and": [
                            {"$ne": ["$t_signup", None]},
                            {"$ne": ["$t_login", None]}
                        ]},
                        {"$subtract": ["$t_login", "$t_signup"]},
                        "$$REMOVE"
                    ]
                }
            },
            "gap_l_w": {
                "$push": {
                    "$cond": [
                        {"$and": [
                            {"$ne": ["$t_login", None]},
                            {"$ne": ["$t_workspace", None]}
                        ]},
                        {"$subtract": ["$t_workspace", "$t_login"]},
                        "$$REMOVE"
                    ]
                }
            },
            "gap_w_p": {
                "$push": {
                    "$cond": [
                        {"$and": [
                            {"$ne": ["$t_workspace", None]},
                            {"$ne": ["$t_project", None]}
                        ]},
                        {"$subtract": ["$t_project", "$t_workspace"]},
                        "$$REMOVE"
                    ]
                }
            },
            "gap_p_t": {
                "$push": {
                    "$cond": [
                        {"$and": [
                            {"$ne": ["$t_project", None]},
                            {"$ne": ["$t_teammate", None]}
                        ]},
                        {"$subtract": ["$t_teammate", "$t_project"]},
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
            "login_conv":     {"$round": [{"$multiply": [{"$divide": ["$n_login",     "$n_signup"]},     100]}, 1]},
            "workspace_conv": {"$round": [{"$multiply": [{"$divide": ["$n_workspace", "$n_login"]},      100]}, 1]},
            "project_conv":   {"$round": [{"$multiply": [{"$divide": ["$n_project",   "$n_workspace"]},  100]}, 1]},
            "teammate_conv":  {"$round": [{"$multiply": [{"$divide": ["$n_teammate",  "$n_project"]},    100]}, 1]}
        }
    },
    {
        "$project": {
            "_id": 0,
            "signup":                    "$n_signup",
            "first_login":               "$n_login",
            "workspace_created":         "$n_workspace",
            "first_project":             "$n_project",
            "invited_teammate":          "$n_teammate",
            "login_conversion_pct":      "$login_conv",
            "workspace_conversion_pct":  "$workspace_conv",
            "project_conversion_pct":    "$project_conv",
            "teammate_conversion_pct":   "$teammate_conv",
            "dropoff_login_pct":         {"$subtract": [100, "$login_conv"]},
            "dropoff_workspace_pct":     {"$subtract": [100, "$workspace_conv"]},
            "dropoff_project_pct":       {"$subtract": [100, "$project_conv"]},
            "dropoff_teammate_pct":      {"$subtract": [100, "$teammate_conv"]},
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


# Q4: Top 20 free-tier users by engagement score
# Engagement score = session_count + avg_duration_min*0.5
#                  + distinct_features*2 + recent_sessions*1.5
# Free-tier customer IDs pulled from PostgreSQL subscriptions table

print("\nQ4: Top 20 Free-Tier Users - Upsell Targets")
print("-" * 50)

FREE_TIER_IDS = [
    9, 23, 31, 55, 61, 76, 89, 101, 109, 118,
    130, 154, 179, 203, 218, 230, 245, 260, 271, 281,
    299, 311, 325, 338, 352, 365, 388, 401, 415, 422
]

THIRTY_DAYS_AGO = datetime.utcnow() - timedelta(days=30)

pipeline_q4 = [
    {
        "$addFields": {
            "customer_id_clean": {
                "$toInt": {"$ifNull": ["$customer_id", "$customerId"]}
            },
            "member_id_clean": {
                "$ifNull": ["$member_id", {"$ifNull": ["$userId", "$userID"]}]
            },
            "ts":  {"$toDate": "$timestamp"},
            "dur": {"$toDouble": "$session_duration_sec"}
        }
    },
    {
        "$match": {
            "customer_id_clean": {"$in": FREE_TIER_IDS}
        }
    },
    {
        "$addFields": {
            "is_recent": {"$gte": ["$ts", THIRTY_DAYS_AGO]},
            "day": {
                "$dateToString": {"format": "%Y-%m-%d", "date": "$ts"}
            }
        }
    },
    {
        "$group": {
            "_id": {
                "customer_id": "$customer_id_clean",
                "member_id":   "$member_id_clean"
            },
            "session_count":     {"$sum": 1},
            "avg_duration_sec":  {"$avg": "$dur"},
            "features_used":     {"$addToSet": "$feature"},
            "days_active":       {"$addToSet": "$day"},
            "recent_sessions":   {
                "$sum": {"$cond": ["$is_recent", 1, 0]}
            }
        }
    },
    {
        "$addFields": {
            "feature_count": {"$size": "$features_used"},
            "days_count":    {"$size": "$days_active"},
            "avg_dur_min":   {
                "$divide": [{"$ifNull": ["$avg_duration_sec", 0]}, 60]
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
                    {"$multiply": [{"$size": "$features_used"}, 2.0]},
                    {"$multiply": ["$recent_sessions", 1.5]}
                ]
            }
        }
    },
    {"$sort": {"engagement_score": -1}},
    {"$limit": 20},
    {
        "$project": {
            "_id":               0,
            "customer_id":       "$_id.customer_id",
            "member_id":         "$_id.member_id",
            "session_count":     1,
            "avg_session_min":   {"$round": ["$avg_dur_min", 1]},
            "features_used":     "$feature_count",
            "days_active":       "$days_count",
            "recent_sessions":   1,
            "engagement_score":  {"$round": ["$engagement_score", 2]}
        }
    }
]

results_q4 = list(db.user_activity_logs.aggregate(pipeline_q4))
for i, r in enumerate(results_q4, 1):
    print(f"  {i}. customer={r.get('customer_id')} | "
          f"sessions={r.get('session_count')} | "
          f"features={r.get('features_used')} | "
          f"score={r.get('engagement_score')}")

client.close()
print("\nDone.")
