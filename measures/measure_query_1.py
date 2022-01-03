# print(col.find().explain()["executionStats"])
results1 = col.aggregate(
    [
        {"$unwind": "$charges"},
        {"$match": {"charges.category_no": 2}},
        {
            "$project": {
                "year": {"$year": "$charges.charge_dt"},
                "lastname": 1,
                "firstname": 1,
            }
        },
        {"$match": {"year": 2021}},
    ]
)

test = db.command(
    "aggregate",
    "members",
    pipeline=[
        {"$unwind": "$charge"},
        {"$match": {"charge.category_desc": "Travel"}},
        {
            "$project": {
                "year": {"$year": "$charge.charge_dt"},
                "lastname": 1,
                "firstname": 1,
            }
        },
        {"$match": {"year": "2020"}},
    ],
    explain=True,
)

test2 = db.command(
    "aggregate",
    "members",
    pipeline=[
        {"$unwind": "$charge"},
        {"$match": {"charge.category_desc": "Travel"}},
        {
            "$project": {
                "year": {"$year": "$charge.charge_dt"},
                "lastname": 1,
                "firstname": 1,
            }
        },
        {"$match": {"year": "2020"}},
    ],
    cursor={},
)

# results1 = col.find()
# print(test2)
# for p in results1:
#     print(p)
# print(results1)
