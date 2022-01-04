from pymongo import MongoClient
from measure_query_1 import measure_query_1
from measure_query_2 import measure_query_2
from measure_query_3 import measure_query_3
from measure_query_4 import measure_query_4
from measure_query_5 import measure_query_5
from measure_query_6 import measure_query_6

from measure_query_7 import measure_query_7


url = "mongodb017.westeurope.cloudapp.azure.com"
port = 32002
client = MongoClient(url, port)
print(client.is_mongos)
print("Listes de db :", client.list_database_names())
db = client.db_credit

col_members = db.members
col_payments = db.payments


# print(db.collection.getShardDistribution())


# print("Time query 1 : ", measure_query_1(db, "Real Estate loan", 2021))
# print("Time query 2 : ", measure_query_2(db, "Ewing-Olson", "Manchester"))
# print("Time query 3 : ", measure_query_3(db, "Real Estate loan", "West"))
# print("Time query 5 : ", measure_query_5(db))
# print("Time query 6 : ", measure_query_5(db))
measure_query_7(db, col_members, col_payments)

print("Listes de db :", client.list_database_names())
client.close()
