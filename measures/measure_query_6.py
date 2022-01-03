from pymongo import MongoClient, collection


url = "mongodb017.westeurope.cloudapp.azure.com"
port = 32002
client = MongoClient(url, port)
print(client.is_mongos)
print("Listes de db :", client.list_database_names())
db = client.db_credit
# try: db.command("serverStatus")
# except Exception as e: print(e)
# else: print("You are connected!")
col = db.members
# col.drop()
# col = db.providers
# col.drop()
# col = db.payments
# col.drop()
# for p in col.find():
#     print(p)
# print(col.find().explain()["executionStats"])
# results1 = col.aggregate([
#   {
#     "$unwind": "$charges"
#   },
#   {
#     "$match": {
#       "charges.category_no": 2
#     }
#   },
#   {
#     "$project": {
#       "year": {
#         "$year": "$charges.charge_dt"
#       },
#       "lastname": 1,
#       "firstname": 1
#     }
#   },
#   {
#     "$match": {
#       "year": 2021
#     }
#   }
# ])

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


results2 = col.aggregate(
    [
        {"$unwind": "$charges"},
        {
            "$match": {
                "$and": [
                    {"corporation.corp_name": "Robertson-Scott"},
                    {"charges.provider_city": "Manchester"},
                ]
            }
        },
        {"$project": {"lastname": 1, "charges.payments": 1}},
    ]
)

for p in results2:
    print(p)

# results3 = col.aggregate({"$unwind" : "$charges"},{"$match":{"$and":[{"charges.category_desc":"Travel"},{"location.street":{$regex:"Rivoli"}}]}})


results3 = col.aggregate(
    [
        {"$unwind": "$charges"},
        {
            "$match": {
                "$and": [
                    {"charges.category_desc": "Real Estate loan"},
                    {"location.street": {"$regex": "West"}},
                ]
            }
        },
    ]
)
# for p in results3:
#     print(p)

results4 = col.aggregate(
    [
        {
            "$match": {
                "balance.curr_balance": {"$gt": 100},
                "charges.provider_name": {"$eq": "Citigroup"},
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
                                {"$strLenCP": "$charges.provider_phone_number"},
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
                "lastname": True,
                "firstname": True,
                "phone_number": True,
                "provider_phone_number": "$charges.provider_phone_number",
            }
        },
    ]
)

for p in results4:
    print(p)

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
                    "$max": {"amount": "$somme", "provider_name": "$_id.provider_name"},
                },
            }
        },
    ]
)


for p in results6:
    print(p)

client.close()
