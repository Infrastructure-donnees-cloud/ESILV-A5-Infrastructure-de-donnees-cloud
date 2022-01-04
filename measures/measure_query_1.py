from statistics import mean


def measure_query_1(db, category_desc, year):
    times = []
    query1 = [
        {"$unwind": "$charges"},
        {"$match": {"charges.category_desc": category_desc}},
        {
            "$project": {
                "year": {
                    "$year": {
                        "$dateFromString": {
                            "dateString": "$charges.charge_dt",
                            "format": "%Y-%m-%d",
                        }
                    }
                },
                "lastname": 1,
                "firstname": 1,
                "member_no": 1,
            }
        },
        {"$match": {"year": year}},
    ]
    for i in range(0, 10):

        results1_time = db.command(
            "explain",
            {"aggregate": "members", "pipeline": query1, "cursor": {}},
            verbosity="executionStats",
        )
        execution_time = 0
        for shards in results1_time["shards"]:
            infos = results1_time["shards"][shards]
            execution_time += (
                infos["stages"][0]["$cursor"]["executionStats"]["executionTimeMillis"]
                + infos["stages"][1]["executionTimeMillisEstimate"]
                + infos["stages"][2]["executionTimeMillisEstimate"]
                + infos["stages"][3]["executionTimeMillisEstimate"]
                + infos["stages"][4]["executionTimeMillisEstimate"]
            )
        times.append(execution_time)
    times.remove(max(times))
    times.remove(min(times))
    return mean(times)
