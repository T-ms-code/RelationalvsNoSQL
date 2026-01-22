# from pymongo import MongoClient

# uri = "mongodb://mongo_rs1:27017,mongo_rs2:27018,mongo_rs3:27019/?replicaSet=rs0"

# print("Conectare...")
# client = MongoClient(uri, serverSelectionTimeoutMS=5000)

# print("Ping...")
# print(client.admin.command("ping"))

# print("Server info:")
# print(client.admin.command("hello"))

# print("DONE OK")

from pymongo import MongoClient

client = MongoClient("mongodb://mongo_rs1:27017,mongo_rs2:27018,mongo_rs3:27019/?replicaSet=rs0")
db = client.admin

conf = db.command("replSetGetConfig")
status = db.command("replSetGetStatus")

print("Config:")
print(conf)
print("\nStatus:")
print(status)

