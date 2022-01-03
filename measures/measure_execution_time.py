from pymongo import MongoClient
from measure_query_2 import measure_query_2
from measure_query_3 import measure_query_3
from measure_query_4 import measure_query_4
from measure_query_6 import measure_query_6
from measure_query_1 import measure_query_1


url = "mongodb017.westeurope.cloudapp.azure.com"
port = 32002
client = MongoClient(url, port)
print(client.is_mongos)
print("Listes de db :", client.list_database_names())
db = client.db_credit

col = db.members

measure_query_1(col, "Real Estate loan", 2021)
# measure_query_2(col, "Ewing-Olson", "Manchester")
# measure_query_3(col, "Real Estate loan", "West")
# measure_query_4(col, "")
# measure_query_6(col)

# print(col.find_one())


client.close()
