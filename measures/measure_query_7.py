def measure_query_7(db, col_members, col_payments):

    col_temp_query7 = db["col_query7"]

    col_temp_query7.drop()
    results7_1 = col_members.aggregate(
        [
            {"$unwind": "$charges"},
            {
                "$project": {
                    "charges.payments": 1,
                    "charges.charge_amount": 1,
                    "charges.charge_dt": 1,
                }
            },
        ]
    )
    avg_rembourseent_time = {}
    index = 0
    for row in results7_1:
        # print(row["charges"]["payments"])
        results7_2 = col_payments.aggregate(
            [
                {"$match": {"payment_no": {"$in": row["charges"]["payments"]}}},
                {"$sort": {"payment_dt": 1}},
                {
                    "$group": {
                        "_id": 1,
                        "total_payment_principal": {"$sum": "$payment_amt"},
                        "total_payment_interest": {"$sum": "$payment_interest"},
                        "last_payment_date": {"$last": "$payment_dt"},
                    }
                },
                {
                    "$match": {
                        "total_payment_principal": {
                            "$gte": row["charges"]["charge_amount"]
                        }
                    }
                },
                {
                    "$project": {
                        "_id": row["charges"][
                            "charge_dt"
                        ],  # the date and hour of the loaning is considered as a unique id*/
                        "time_delta_in_days": {
                            "$divide": [
                                {
                                    "$subtract": [
                                        {
                                            "$dateFromString": {
                                                "dateString": row["charges"][
                                                    "charge_dt"
                                                ],
                                                "format": "%Y-%m-%d",  # /* Date of Loaning */
                                            }
                                        },
                                        {
                                            "$dateFromString": {
                                                "dateString": "$last_payment_date",
                                                "format": "%Y-%m-%d",
                                            }
                                        },
                                    ]
                                },
                                1000 * 60 * 60,
                            ]
                        },
                        "total_payment_principal": 1,
                    }
                },
            ]
        )
        # print(type(results7_2))
        for p in results7_2:

            new_doc = db.col_query7.insert_one(p)
            index += 1
        if index == 21:
            break
    col = db.col_query7
    results7_3 = col.aggregate(
        [
            {
                "$project": {
                    "total_payment_principal": 1,
                    "time_delta_in_days": 1,
                    "range": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$gte": ["$total_payment_principal", 0]},
                                    {"$lte": ["$total_payment_principal", 20000]},
                                ]
                            },
                            "0-20000",
                            {
                                "$cond": [
                                    {
                                        "$and": [
                                            {
                                                "$gte": [
                                                    "$total_payment_principal",
                                                    20001,
                                                ]
                                            },
                                            {
                                                "$lte": [
                                                    "$total_payment_principal",
                                                    40000,
                                                ]
                                            },
                                        ]
                                    },
                                    "20001-40000",
                                    "40001-above",
                                ]
                            },
                        ]
                    },
                }
            },
            {
                "$group": {
                    "_id": "$range",
                    "average_repayment_time": {"$avg": "$time_delta_in_days"},
                }
            },
        ]
    )

    print(results7_3)
    for p in results7_3:
        print(p)
    col.drop()
