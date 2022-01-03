def measure_query_1(col, category_desc, year):
    results1 = col.aggregate(
        [
            {"$unwind": "$charges"},
            {"$match": {"charges.category_desc": "Real Estate loan"}},
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
                }
            },
            {"$match": {"year": 2021}},
        ]
    )
    for p in results1:
        print(p)


# test = db.command(
#     "aggregate",
#     "members",
#     pipeline=[
#         {"$unwind": "$charge"},
#         {"$match": {"charge.category_desc": "Travel"}},
#         {
#             "$project": {
#                 "year": {"$year": "$charge.charge_dt"},
#                 "lastname": 1,
#                 "firstname": 1,
#             }
#         },
#         {"$match": {"year": "2020"}},
#     ],
#     explain=True,
# )


# results1 = col.find()
# print(test2)
# for p in results1:
#     print(p)
# print(results1)
