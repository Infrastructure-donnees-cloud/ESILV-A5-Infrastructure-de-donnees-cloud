from statistics import mean


def measure_query_5(db):
    times = []
    query = [
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
    for i in range(0, 10):
        results_time = db.command(
            "explain",
            {"aggregate": "members", "pipeline": query, "cursor": {}},
            verbosity="executionStats",
        )
        execution_time = (
            results_time["shards"]["RS_credit"]["stages"][0]["$cursor"][
                "executionStats"
            ]["executionTimeMillis"]
            + results_time["shards"]["RS_credit"]["stages"][1][
                "executionTimeMillisEstimate"
            ]
            + results_time["shards"]["RS_credit"]["stages"][2][
                "executionTimeMillisEstimate"
            ]
            + results_time["shards"]["RS_credit"]["stages"][3][
                "executionTimeMillisEstimate"
            ]
            + results_time["shards"]["RS_credit"]["stages"][4][
                "executionTimeMillisEstimate"
            ]
        )
        times.append(execution_time)
    times.remove(max(times))
    times.remove(min(times))
    return mean(times)
