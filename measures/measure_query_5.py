def measure_query_5(col):
    results5 = col.aggregate(
        [
            {"$unwind": "$charges"},
            {
                "$group": {
                    "_id": "$member_no",
                    "detail": {"$first": "$$ROOT"},
                    "total_debt_to_pay": {"$sum": "$charges.charge_amount"},
                }
            },
            {
                "$replaceRoot": {
                    "newRoot": {
                        "$mergeObjects": [
                            {"total_debt_to_pay": "$total_debt_to_pay"},
                            "$detail",
                        ]
                    }
                }
            },
            {
                "$project": {
                    "member_no": 1,
                    "current_balance": "$balance.curr_balance",
                    "total_debt_to_pay": "$totalh1_debt_to_pay",
                    "corp_no": "$corporation.corp_no",
                    "region": "$corporation.region_no",
                }
            },
        ]
    )
    for p in results5:
        print(p)
