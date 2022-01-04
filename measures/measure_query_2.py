from statistics import mean


def measure_query_2(db, corp_name, provider_city):
    times = []
    query2 = [
        {"$unwind": "$charges"},
        {
            "$match": {
                "$and": [
                    {"corporation.corp_name": corp_name},
                    {"charges.provider_city": provider_city},
                ]
            }
        },
        {"$project": {"lastname": 1, "charges.payments": 1}},
    ]
    for i in range(0, 10):
        results2_time = db.command(
            "explain",
            {"aggregate": "members", "pipeline": query2, "cursor": {}},
            verbosity="executionStats",
        )
        execution_time = 0
        for shards in results2_time["shards"]:
            infos = results2_time["shards"][shards]
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
