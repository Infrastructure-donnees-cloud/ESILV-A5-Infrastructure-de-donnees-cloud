def measure_query_3(col, category_desc, street):

    results3 = col.aggregate(
        [
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
    )
    for p in results3:
        print(p)
