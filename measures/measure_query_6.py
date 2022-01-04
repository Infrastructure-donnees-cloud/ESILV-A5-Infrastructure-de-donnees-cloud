from statistics import mean


def measure_query_6(db):
    times = []
    query = [
        {"$unwind": "$charges"},
        {
            "$group": {
                "_id": {
                    "category": "$charges.category_desc",
                    "provider_name": "$charges.provider_name",
                },
                "somme": {"$sum": "$charges.charge_amount"},
            }
        },
        {
            "$group": {
                "_id": "$_id.category",
                "maxi": {
                    "$max": {
                        "amount": "$somme",
                        "provider_name": "$_id.provider_name",
                    },
                },
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
