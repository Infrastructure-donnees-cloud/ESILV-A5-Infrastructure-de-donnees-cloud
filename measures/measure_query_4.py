def measure_query_4(col, provider_name):

    results4 = col.aggregate(
        [
            {
                "$match": {
                    "balance.curr_balance": {"$gt": 0},
                    "charges.provider_name": {"$eq": "Citigroup"},
                }
            },
            {"$unwind": "$charges"},
            {
                "$addFields": {
                    "member_phone_last_2digits": {
                        "$substr": [
                            "$phone_number",
                            {"$subtract": [{"$strLenCP": "$phone_number"}, 2]},
                            -1,
                        ]
                    },
                    "provider_phone_last_2digits": {
                        "$substr": [
                            "$charges.provider_phone_number",
                            {
                                "$subtract": [
                                    {"$strLenCP": "$charges.provider_phone_number"},
                                    2,
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
                    "lastname": True,
                    "firstname": True,
                    "phone_number": True,
                    "provider_phone_number": "$charges.provider_phone_number",
                }
            },
        ]
    )

    for p in results4:
        print(p)
