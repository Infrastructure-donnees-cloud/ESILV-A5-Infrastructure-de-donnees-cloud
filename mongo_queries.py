from pymongo import MongoClient
import pandas as pd
from bson.json_util import dumps, loads
import pymongo



def db_connection():

    url = "mongodb017.westeurope.cloudapp.azure.com"
    port = 32002

    client = MongoClient(url, port)
    db = client['db_credit']

    return db


def query_1(category, year):

    col = db_connection()['members']
    
    results1 = col.aggregate(
        [
            {"$unwind": "$charges"},
            {"$match": {"charges.category_desc": str(category)}},
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
            {"$match": {"year": int(year)}},
        ]
    )
    
    return list(results1)[0:25]


def query_2(corp_name):

    col = db_connection()['members']
    
    results2 = col.aggregate(
        [
            {"$unwind": "$charges"},
            {
                "$match": {
                    "$and": [
                        {"corporation.corp_name": corp_name},
                        {"charges.provider_city": "Savannah"},
                    ]
                }
            },
            {"$project": {"lastname": 1, "charges.payments": 1}},
        ]
    )
    
    return list(results2)[0:25]



def query_3():

    col = db_connection()['members']
    
    results3 = col.aggregate(
        [
            {"$unwind": "$charges"},
            {
                "$match": {
                    "$and": [
                        {"charges.category_desc": "Real Estate loan"},
                        {"location.street": {"$regex": "Circle"}},
                    ]
                }
            },
        ]
    )
    
    return list(results3)[0:25]


def query_4():

    col = db_connection()['members']
    
    results4 = col.aggregate(
        [
            {
                "$match": {
                    "balance.curr_balance": {"$gt": 0},
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
    
    return list(results4)[0:25]


def query_5():

    col = db_connection()['members']
    
    results5 = col.aggregate(
        [
            {"$unwind": "$charges"},
            {
                "$group": {
                    "_id": "$member_no",
                    "detail": {"$first": "$$ROOT"},
                    "total_debt_to_pay": {"$sum": "$charges.charge_amount"},
                }
            },
            {
                "$replaceRoot": {
                    "newRoot": {
                        "$mergeObjects": [
                            {"total_debt_to_pay": "$total_debt_to_pay"},
                            "$detail",
                        ]
                    }
                }
            },
            {
                "$project": {
                    "member_no": 1,
                    "current_balance": "$balance.curr_balance",
                    "total_debt_to_pay": "$totalh1_debt_to_pay",
                    "corp_no": "$corporation.corp_no",
                    "region": "$corporation.region_no",
                }
            },
        ]
    )
    
    return list(results5)[0:25]


def query_6(category_desc = None):

    col = db_connection()['members']
    
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
    
    return list(results6)


def query_7():

    db = db_connection()
    col_members = db['members']
    col_payments = db['payments']

    col = db_connection()['members']
    
    col_temp_query7 = db["col_query7"]

    col_temp_query7.drop()
    results7_1 = col_members.aggregate(
        [
            {"$unwind": "$charges"},
            {
                "$project": {
                    "charges.payments": 1,
                    "charges.charge_amount": 1,
                    "charges.charge_dt": 1,
                }
            },
        ]
    )
    avg_rembourseent_time = {}
    index = 0
    for row in results7_1:
        # print(row["charges"]["payments"])
        results7_2 = col_payments.aggregate(
            [
                {"$match": {"payment_no": {"$in": row["charges"]["payments"]}}},
                {"$sort": {"payment_dt": 1}},
                {
                    "$group": {
                        "_id": 1,
                        "total_payment_principal": {"$sum": "$payment_amt"},
                        "total_payment_interest": {"$sum": "$payment_interest"},
                        "last_payment_date": {"$last": "$payment_dt"},
                    }
                },
                {
                    "$match": {
                        "total_payment_principal": {
                            "$gte": row["charges"]["charge_amount"]
                        }
                    }
                },
                {
                    "$project": {
                        "_id": row["charges"][
                            "charge_dt"
                        ],  # the date and hour of the loaning is considered as a unique id*/
                        "time_delta_in_days": {
                            "$divide": [
                                {
                                    "$subtract": [
                                        {
                                            "$dateFromString": {
                                                "dateString": row["charges"][
                                                    "charge_dt"
                                                ],
                                                "format": "%Y-%m-%d",  # /* Date of Loaning */
                                            }
                                        },
                                        {
                                            "$dateFromString": {
                                                "dateString": "$last_payment_date",
                                                "format": "%Y-%m-%d",
                                            }
                                        },
                                    ]
                                },
                                1000 * 60 * 60,
                            ]
                        },
                        "total_payment_principal": 1,
                    }
                },
            ]
        )
        # print(type(results7_2))
        for p in results7_2:

            new_doc = db.col_query7.insert_one(p)
            index += 1
        if index == 21:
            break

    col = db.col_query7

    results7_3 = col.aggregate(
        [
            {
                "$project": {
                    "total_payment_principal": 1,
                    "time_delta_in_days": 1,
                    "range": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$gte": ["$total_payment_principal", 0]},
                                    {"$lte": ["$total_payment_principal", 20000]},
                                ]
                            },
                            "0-20000",
                            {
                                "$cond": [
                                    {
                                        "$and": [
                                            {
                                                "$gte": [
                                                    "$total_payment_principal",
                                                    20001,
                                                ]
                                            },
                                            {
                                                "$lte": [
                                                    "$total_payment_principal",
                                                    40000,
                                                ]
                                            },
                                        ]
                                    },
                                    "20001-40000",
                                    "40001-above",
                                ]
                            },
                        ]
                    },
                }
            },
            {
                "$group": {
                    "_id": "$range",
                    "average_repayment_time": {"$avg": "$time_delta_in_days"},
                }
            },
        ]
    )
    
    col_temp_query7.drop()

    return list(results7_3)


#print(query_7())


#print(query_6())

#df = pd.json_normalize(query_4())
#print(df)


def get_list_collections():

    res = []

    db = db_connection()

    collections = list(db.list_collections())

    for col in collections:
        try:
            res.append(col['name'])
        except:
            None

    return res


def db_stats():
    """
    From GET take:  login, password : database credentials(optional, currently ignored)
    Return json with database stats,as returned by mongo (db.stats())
    """
    try:
        db = db_connection()
        resp = db.command({'dbstats': 1})
        return resp['raw']['RS_credit/mongodb033:32003,mongodb035:32003,mongodb088:32003']
    except:
        print('Error')

#db_stats()


def get_number_objects():

    stats = db_stats()
    res = stats['objects']
    return res

#get_number_objects()

def get_data_size():

    stats = db_stats()
    res = stats['dataSize']
    return res

def get_indexes():

    stats = db_stats()
    res = stats['indexes']
    return res


def get_storage_size():

    stats = db_stats()
    res = stats['storageSize']
    return res    


#a = db_connection()['providers']

#print(a.index_information())