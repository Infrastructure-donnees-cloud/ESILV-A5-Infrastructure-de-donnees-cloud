import uuid


def measure_query_8(col_members, col_payments):
    query8_1 = [
        {"$unwind": "$charges"},
        {
            "$project": {
                "charges.payments": 1,
                "charges.charge_amount": 1,
                "charges.charge_dt.date": 1,
                "charges.category_desc": 1,
                "charges.provider_no": 1,
            }
        },
    ]

    query8_3 = [
        {
            "$group": {
                "_id": {
                    "$concat": [
                        "provider_",
                        {"$toString": "$provider_no"},
                        "-",
                        "$category",
                    ]
                },
                "provider_no": {"$last": "$provider_no"},
                "category": {"$last": "$category"},
                "monthly_rate": {"$avg": "$monthly_average_interest_rate"},
            }
        },
        {"$sort": {"provider_no": 1}},
    ]

    results8_1 = col_members.aggregate(query8_1)
    for row in results8_1:
        print(row)
        results8_2 = col_payments.aggregate(
            [
                {"$unwind": "$charges"},
                {
                    "$project": {
                        "charges.payments": 1,
                        "charges.charge_amount": 1,
                        "charges.charge_dt": 1,
                        "charges.category_desc": 1,
                        "charges.provider_no": 1,
                    }
                },
                {"$match": {"payment_no": {"$in": row["charges"]["payments"]}}},
                # {"$sort": {"payment_dt": 1}},
                # {
                #     "$group": {
                #         "_id": str(uuid.uuid1()),
                #         "monthly_average_interest_rate": {
                #             "$avg": {
                #                 "$divide": ["$payment_interest", "$payment_principal"]
                #             }
                #         },
                #         "last_payment_date": {"$last": "$payment_dt"},
                #     }
                # },
                {
                    "$addFields": {
                        "category": row["charges"]["category_desc"],
                        "provider_no": row["charges"]["provider_no"],
                    }
                },
            ]
        )
        for p in results8_2:
            print(p)
