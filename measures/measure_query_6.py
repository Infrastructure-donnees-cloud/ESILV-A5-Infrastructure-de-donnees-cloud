def measure_query_6(col):
    results6 = col.aggregate(
        [
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
    )

    for p in results6:
        print(p)
