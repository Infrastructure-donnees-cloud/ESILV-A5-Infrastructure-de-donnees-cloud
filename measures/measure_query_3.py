from statistics import mean


def measure_query_3(db, category_desc, street):
    times = []
    query = [
        {"$unwind": "$charges"},
        {
            "$match": {
                "$and": [
                    {"charges.category_desc": category_desc},
                    {"location.street": {"$regex": street}},
                ]
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
            )
        times.append(execution_time)
    times.remove(max(times))
    times.remove(min(times))
    return mean(times)
