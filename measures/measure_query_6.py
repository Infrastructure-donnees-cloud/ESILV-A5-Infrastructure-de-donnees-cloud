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
        execution_time = 0
        for shards in results_time["shards"]:
            infos = results_time["shards"][shards]
            execution_time += (
                infos["stages"][0]["$cursor"]["executionStats"]["executionTimeMillis"]
                + infos["stages"][1]["executionTimeMillisEstimate"]
                + infos["stages"][2]["executionTimeMillisEstimate"]
                + infos["stages"][3]["executionTimeMillisEstimate"]
            )
        times.append(execution_time)
    times.remove(max(times))
    times.remove(min(times))
    return mean(times)
