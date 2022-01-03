def measure_query_8(col):
    results7_1 = col.aggregate(
        [
            {"$unwind": "$charges"},
            {
                "$project": {
                    "charges.payments": 1,
                    "charges.charge_amount": 1,
                    "charges.charge_dt.date": 1,
                }
            },
        ]
    )

    for row in results7_1:
        print(row)
        break
        # results7_2 = col.aggregate(
        #     [
        #         {"$match": {"payment_no": {"$in": row.charge.payments}}},
        #         {"$sort": {"payment_dt": 1}},
        #         {
        #             "$group": {
        #                 "_id": 1,
        #                 "total_payment_principal": {"$sum": "$payment_principal"},
        #                 "total_payment_interest": {"$sum": "$payment_interest"},
        #                 "last_payment_date": {"$last": "$payment_dt"},
        #             }
        #         },
        #         {
        #             "$match": {
        #                 "total_payment_principal": {"$gte": row.charge.charge_amount}
        #             }
        #         },
        #         {
        #             "$project": {
        #                 "_id": row.charge.charge_dt.date,  # the date and hour of the loaning is considered as a unique id*/
        #                 "time_delta_in_days": {
        #                     "$dateDiff": {
        #                         "startDate": {
        #                             "$dateFromString": {
        #                                 "dateString": row.charge.charge_dt.date  # /* Date of Loaning */
        #                             }
        #                         },
        #                         "endDate": {
        #                             "$dateFromString": {
        #                                 "dateString": "$last_payment_date"
        #                             }
        #                         },
        #                         "unit": "day",
        #                     }
        #                 },
        #                 "total_payment_principal": 1,
        #             }
        #         },
        #     ]
        # )

    for p in results6:
        print(p)
