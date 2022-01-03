def measure_query_2(col, corp_name, provider_city):

    results2 = col.aggregate(
        [
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
    )

    for p in results2:
        print(p)
