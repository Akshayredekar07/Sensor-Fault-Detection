import pandas as pd
import json
from pymongo.mongo_client import MongoClient
import os
passward = os.getenv("MONGO_DB_URL")

uri = f"mongodb+srv://akshayredekar4441:{passward}@cluster0.tbm5wnd.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri)

# Create database name and collection name
DATABASE_NAME = "SensorData"
COLLECTION_NAME = "waferfault"

# Read data from DataFrame
df=pd.read_csv(r"C:\Users\Akshay\Downloads\sensor_fault_detection\notebooks\wafer_23012020_041211.csv")
df=df.drop("Unnamed: 0",axis=1)

# Convert the data into json
json_record=list(json.loads(df.T.to_json()).values())

#now dump the data into the database
client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record)

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)