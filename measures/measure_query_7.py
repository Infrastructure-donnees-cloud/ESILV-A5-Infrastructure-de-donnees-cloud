def measure_query_7(col_members, col_payments):
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
                        # "time_delta_in_days": {
                        #     "$dateDiff": {
                        #         "startDate": {
                        #             "$dateFromString": {
                        #                 "dateString": row["charges"]["charge_dt"],
                        #                 "format": "%Y-%m-%d",  # /* Date of Loaning */
                        #             }
                        #         },
                        #         "endDate": {
                        #             "$dateFromString": {
                        #                 "dateString": "$last_payment_date",
                        #                 "format": "%Y-%m-%d",
                        #             }
                        #         },
                        #         "unit": "day",
                        #     }
                        # },
                        "total_payment_principal": 1,
                    }
                },
            ]
        )
        # print(results7_2)
        for p in results7_2:
            print(p)
        print("Go next")
