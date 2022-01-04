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
        execution_time = (
            results2_time["shards"]["RS_credit"]["stages"][0]["$cursor"][
                "executionStats"
            ]["executionTimeMillis"]
            + results2_time["shards"]["RS_credit"]["stages"][1][
                "executionTimeMillisEstimate"
            ]
            + results2_time["shards"]["RS_credit"]["stages"][2][
                "executionTimeMillisEstimate"
            ]
            + results2_time["shards"]["RS_credit"]["stages"][3][
                "executionTimeMillisEstimate"
            ]
        )
        times.append(execution_time)
    times.remove(max(times))
    times.remove(min(times))
    return mean(times)
