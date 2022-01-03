from pymongo import MongoClient
from measure_query_2 import measure_query_2


url = "mongodb017.westeurope.cloudapp.azure.com"
port = 32002
client = MongoClient(url, port)
print(client.is_mongos)
print("Listes de db :", client.list_database_names())
db = client.db_credit

col = db.members

measure_query_2(col, "Ewing-Olson", "Manchester")

client.close()
