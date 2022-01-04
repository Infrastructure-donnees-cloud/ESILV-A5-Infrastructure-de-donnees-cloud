from pymongo import MongoClient
import pandas as pd
from bson.json_util import dumps, loads
from statistics import mean
import uuid



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
            {"$project": {"lastname": 1, "firstname":1, "charges.payments": 1}},
        ]
    )
    
    return list(results2)[0:25]



def query_3(category, street):

    col = db_connection()['members']
    
    results3 = col.aggregate(
        [
            {"$unwind": "$charges"},
            {
                "$match": {
                    "$and": [
                        {"charges.category_desc": str(category)},
                        {"location.street": {"$regex": str(street)}},
                    ]
                }
            },
        ]
    )
    
    return list(results3)[0:25]


def query_4(capital, provider):

    col = db_connection()['members']

    results4 = col.aggregate(
        [
        {
            "$match": {
                "balance.curr_balance": {"$gt": int(capital)},
                "charges.provider_name": {"$eq": str(provider)},
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
    ])

    return list(results4)[0:25]
    
#print(query_4('Bank of America'))


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
                    "total_debt_to_pay": "$total_debt_to_pay",
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




def query_8(category_desc = None):

    db = db_connection()
    col_members = db['members']
    col_payments = db['payments']

    
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

    results8_1 = col_members.aggregate(query8_1)

    col_temp_query8 = db["col_query8"]
    col_temp_query8.drop()

    index = 0
    for row in results8_1:
        index += 1
        # print(row)
        results8_2 = col_payments.aggregate(
            [

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
        )

        for p in results8_2:
            new_doc = db.col_query8.insert_one(p)
        if index > 50:
            break

    results8_3 = db.col_query8.aggregate(query8_3)
    

    col_temp_query8 = db["col_query8"]
    col_temp_query8.drop()
    
    return list(results8_3)


#print(query_8())

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

#print(db_stats())

def get_number_collection():

    stats = db_stats()
    res = stats['collections']
    return res


def get_number_objects():

    stats = db_stats()
    res = stats['objects']
    return res

def get_avg_object_size():

    stats = db_stats()
    res = stats['avgObjSize']
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
