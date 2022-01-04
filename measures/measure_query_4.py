from statistics import mean


def measure_query_4(db, provider_name):
    times = []
    query4 = [
        {
            "$match": {
                "balance.curr_balance": {"$gt": 100},
                "charges.provider_name": {"$eq": provider_name},
            }
        },
        {"$unwind": "$charges"},
        {
            "$addFields": {
                "member_phone_last_2digits": {
                    "$substr": [
                        "$phone_number",
                        {"$subtract": [{"$strLenCP": "$phone_number"}, 2]},
                        -1,
                    ]
                },
                "provider_phone_last_2digits": {
                    "$substr": [
                        "$charges.provider_phone_number",
                        {
                            "$subtract": [
                                {
                                    "$strLenCP": {
                                        "$toString": "$charges.provider_phone_number"
                                    }
                                },
                                2,
                            ]
                        },
                        -1,
                    ]
                },
            }
        },
        {
            "$match": {
                "$expr": {
                    "$eq": [
                        "$member_phone_last_2digits",
                        "$provider_phone_last_2digits",
                    ]
                }
            }
        },
        {
            "$project": {
                "lastname": 1,
                "firstname": 1,
                "phone_number": 1,
                "provider_phone_number": "$charges.provider_phone_number",
            }
        },
    ]
    for i in range(0, 10):

        results1_time = db.command(
            "explain",
            {"aggregate": "members", "pipeline": query4, "cursor": {}},
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
