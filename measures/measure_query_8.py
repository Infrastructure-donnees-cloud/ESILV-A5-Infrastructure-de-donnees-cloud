import uuid
from statistics import mean


def measure_query_8(db, col_members, col_payments):
    times = []
    query8_1 = [
        {"$unwind": "$charges"},
        {
            "$project": {
                "charges.payments": 1,
                "charges.charge_amount": 1,
                "charges.charge_dt.date": 1,
                "charges.category_desc": 1,
                "charges.provider_no": 1,
            }
        },
    ]

    query8_3 = [
        {
            "$group": {
                "_id": {
                    "$concat": [
                        "provider_",
                        {"$toString": "$provider_no"},
                        "-",
                        "$category",
                    ]
                },
                "provider_no": {"$last": "$provider_no"},
                "category": {"$last": "$category"},
                "monthly_rate": {"$avg": "$monthly_average_interest_rate"},
            }
        },
        {"$sort": {"provider_no": 1}},
    ]
    for i in range(0, 10):
        results8_1 = col_members.aggregate(query8_1)

        col_temp_query8 = db["col_query8"]
        col_temp_query8.drop()
        results_time_1 = db.command(
            "explain",
            {"aggregate": "members", "pipeline": query8_1, "cursor": {}},
            verbosity="executionStats",
        )
        execution_time = 0
        for shards in results_time_1["shards"]:
            infos = results_time_1["shards"][shards]
            execution_time += (
                infos["stages"][0]["$cursor"]["executionStats"]["executionTimeMillis"]
                + infos["stages"][1]["executionTimeMillisEstimate"]
                + infos["stages"][2]["executionTimeMillisEstimate"]
            )

        index = 0
        for row in results8_1:
            index += 1
            query8_2 = [
                {"$match": {"payment_no": {"$in": row["charges"]["payments"]}}},
                {"$sort": {"payment_dt": 1}},
                {
                    "$group": {
                        "_id": str(uuid.uuid1()),
                        "monthly_average_interest_rate": {
                            "$avg": {
                                "$divide": ["$payment_interest", "$payment_principal"]
                            }
                        },
                        "last_payment_date": {"$last": "$payment_dt"},
                    }
                },
                {
                    "$addFields": {
                        "category": row["charges"]["category_desc"],
                        "provider_no": row["charges"]["provider_no"],
                    }
                },
            ]
            results_time_2 = db.command(
                "explain",
                {"aggregate": "payments", "pipeline": query8_2, "cursor": {}},
                verbosity="executionStats",
            )
            for shards in results_time_2["shards"]:
                infos = results_time_2["shards"][shards]
                execution_time += (
                    infos["stages"][0]["$cursor"]["executionStats"][
                        "executionTimeMillis"
                    ]
                    + infos["stages"][1]["executionTimeMillisEstimate"]
                    + infos["stages"][2]["executionTimeMillisEstimate"]
                )
            execution_time += 1
            results8_2 = col_payments.aggregate(query8_2)

            for p in results8_2:
                new_doc = db.col_query8.insert_one(p)
            if index > 500:
                break
        # results8_3 = db.col_query8.aggregate(query8_3)
        # for p in results8_3:
        #     print(p)

        results_time_3 = db.command(
            "explain",
            {"aggregate": "col_query8", "pipeline": query8_3, "cursor": {}},
            verbosity="executionStats",
        )
        execution_time += (
            results_time_3["stages"][0]["$cursor"]["executionStats"][
                "executionTimeMillis"
            ]
            + results_time_3["stages"][1]["executionTimeMillisEstimate"]
            + results_time_3["stages"][2]["executionTimeMillisEstimate"]
        )
        times.append(execution_time)
        # print(results7_3)
        # for p in results7_3:
        #     print(p)
    col_temp_query8 = db["col_query8"]
    col_temp_query8.drop()
    times.remove(max(times))
    times.remove(min(times))
    return mean(times)
