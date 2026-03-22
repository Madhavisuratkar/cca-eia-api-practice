def active_inactive_filter(query_filter):
    return [
        {"$match": query_filter},
        {"$group": {
            "_id": {"organisation": "$organisation", "user_name": "$user_name"},
            "login_count": {"$sum": 1}
        }}
    ]
def inactive_filter(app_name, cutoff_date, exclusion_filter):
    return [
        {"$addFields": {"datetime_obj": {"$dateFromString": {"dateString": "$datetime"}}}},
        {"$match": {"app_name": app_name, "date": {"$lt": cutoff_date}, **exclusion_filter}},
        {"$addFields": {
            "month_num": {"$month": "$datetime_obj"},
            "year": {"$year": "$datetime_obj"}
        }},
        {"$addFields": {
            "month_name": {
                "$switch": {
                    "branches": [
                        {"case": {"$eq": ["$month_num", 1]}, "then": "Jan"},
                        {"case": {"$eq": ["$month_num", 2]}, "then": "Feb"},
                        {"case": {"$eq": ["$month_num", 3]}, "then": "Mar"},
                        {"case": {"$eq": ["$month_num", 4]}, "then": "Apr"},
                        {"case": {"$eq": ["$month_num", 5]}, "then": "May"},
                        {"case": {"$eq": ["$month_num", 6]}, "then": "Jun"},
                        {"case": {"$eq": ["$month_num", 7]}, "then": "Jul"},
                        {"case": {"$eq": ["$month_num", 8]}, "then": "Aug"},
                        {"case": {"$eq": ["$month_num", 9]}, "then": "Sep"},
                        {"case": {"$eq": ["$month_num", 10]}, "then": "Oct"},
                        {"case": {"$eq": ["$month_num", 11]}, "then": "Nov"},
                        {"case": {"$eq": ["$month_num", 12]}, "then": "Dec"},
                    ],
                    "default": "Unknown"
                }
            }
        }},
        {"$group": {
            "_id": {
                "month": {"$concat": [{"$toString": "$year"}, "-", "$month_name"]},
                "organisation": "$organisation"
            },
            "login_count": {"$sum": 1}
        }}
    ]

def user_filter(app_name, exclusion_filter):
    return [
        {"$match": {"app_name": app_name, **exclusion_filter, "user_name": {"$ne": None}}},
        {"$group": {"_id": "$organisation", "users": {"$addToSet": "$user_name"}}},
        {"$project": {"_id": 1, "user_count": {"$size": "$users"}}},
        {"$sort": {"user_count": -1, "_id": 1}}
    ]

def user_login_query(app_name, cutoff_date_str, exclusion_filter):
    return [
        {"$match": {
            "app_name": app_name,
            "date": {"$gte": cutoff_date_str},  # use string comparison
            **exclusion_filter
        }},
        {"$group": {
            "_id": {"day": {"$substr": ["$date", 0, 10]}},  # YYYY-MM-DD
            "unique_users": {"$addToSet": "$user_name"}
        }},
        {"$project": {
            "_id": 1,
            "count": {"$size": "$unique_users"}
        }},
        {"$sort": {"_id.day": 1}}
    ]

def features_count_filter(query_filter):
    return [
            {"$match": query_filter},
            {"$group": {
                "_id": {"endpoint": "$endpoint", "organisation": "$organisation"},
                "count": {"$sum": 1}
            }}
        ]

def upload_trend_filter(portfolios_query):
    return [
        {"$match": portfolios_query},
        {"$group": {
            "_id": {"date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$uploaded_date"}}},
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id.date": 1}}
    ]
