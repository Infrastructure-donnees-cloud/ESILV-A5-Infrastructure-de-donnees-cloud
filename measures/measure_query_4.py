def measure_query_4(col, provider_name):

    results4 = col.aggregate(
        [
            {
                "$match": {
                    "balance.curr_balance": {"$gt": 100},
                }
            },
            {"$unwind": "$charges"},
            {
                "$addFields": {"phone_number": {"$toString": "$phone_number"}},
            },
            {
                "$addFields": {
                    "provider_phone_number": {
                        "$toString": "$charges.provider_phone_number"
                    }
                }
            },
            {
                "$addFields": {
                    "member_phone_last_2digits": {
                        "$substr": [
                            "$phone_number",
                            {"$subtract": [{"$strLenCP": "$phone_number"}, 1]},
                            -1,
                        ]
                    },
                    "provider_phone_last_2digits": {
                        "$substr": [
                            "$provider_phone_number",
                            {
                                "$subtract": [
                                    {"$strLenCP": "provider_phone_number"},
                                    1,
                                ]
                            },
                            -1,
                        ]
                    },
                }
            },
            {
                "$match": {
                    "$expr": {
                        "$eq": [
                            "$member_phone_last_2digits",
                            "$provider_phone_last_2digits",
                        ]
                    }
                }
            },
            {
                "$project": {
                    "lastname": 1,
                    "firstname": 1,
                    "phone_number": 1,
                    "provider_phone_number": "$charges.provider_phone_number",
                }
            },
        ]
    )

    for p in results4:
        print(p)
