import asyncio
import requests
import json
import pymongo
from unstructured.partition.auto import partition
from openai import AzureOpenAI

class DocumentProcessor:
    def __init__(self, azure_endpoint, api_version, api_key, mongo_uri, fga_api_url, fga_store_id, fga_api_token, authorization_model_id, db_name, collection_name):
        self.az_client = AzureOpenAI(azure_endpoint=azure_endpoint, api_version=api_version, api_key=api_key)
        self.mongo_client = pymongo.MongoClient(mongo_uri)
        self.fga_api_url = fga_api_url
        self.fga_store_id = fga_store_id
        self.fga_api_token = fga_api_token
        self.authorization_model_id = authorization_model_id
        self.db_name = db_name
        self.collection_name = collection_name

    def generate_embeddings(self, text, model=""): 
        return self.az_client.embeddings.create(input = [text], model=model).data[0].embedding

    def check_authorization(self, tuple_key):
        url = f"{self.fga_api_url}/stores/{self.fga_store_id}/check"
        headers = {
            "Authorization": f"Bearer {self.fga_api_token}",
            "content-type": "application/json",
        }
        data = {
            "authorization_model_id": self.authorization_model_id,
            "tuple_key": tuple_key
        }
        response = requests.post(url, headers=headers, data=json.dumps(data))
        return response.json()

    def add_tuple(self, USER, RESOURCE):
        url = f"{self.fga_api_url}/stores/{self.fga_store_id}/write"
        headers = {
            "Authorization": f"Bearer {self.fga_api_token}",
            "content-type": "application/json",
        }
        data = {
            "writes": {
                "tuple_keys": [
                {
                    "user": "user:"+USER,
                    "relation": "viewer",
                    "object": "doc:"+RESOURCE
                }
                ]
            },
            "authorization_model_id": self.authorization_model_id
        }
        response = requests.post(url, headers=headers, data=json.dumps(data))
        return response.json()

    def search_tool(self, text, USER_ID):
        response = self.mongo_client[self.db_name][self.collection_name].aggregate([
        {
            "$vectorSearch": {
                "index": "vector_index",
                "queryVector": self.az_client.embeddings.create(model="text-embedding-ada-002",input=text).data[0].embedding,
                "path": "embeddings",
                "limit": 5,
                "numCandidates": 30
            }
        }, {"$project":{"_id":0, "embeddings":0, "metadata":0}}
        ])
        for doc in response:
            tuple_key = {"user":"user:"+USER_ID,"relation":"viewer","object":"doc:"+doc["source"]}
            response = self.check_authorization(tuple_key)
            if response['allowed']:
                print(f"Access Granted: User '{USER_ID}' has permission to read document '{doc['source']}'.")
            else:
                print(f"Access Denied: User '{USER_ID}' does not have permission to read document '{doc['source']}'.")

    def partition_pdf(self, resource):
        mdb_db = self.mongo_client[self.db_name]
        mdb_collection = mdb_db[self.collection_name]
        print("Clearing the db first...")
        mdb_collection.delete_many({})
        print("Database cleared.")
        print("Starting PDF document partitioning...")
        elements = partition(resource)
        for element in elements:
            mdb_collection.insert_one({
                "text":str(element.text),
                "embeddings":self.generate_embeddings(str(element.text), "text-embedding-ada-002"),
                "metadata": {
                    "raw_element":element.to_dict(),
                },
                "source":resource
            })
        print("PDF partitioning and database insertion completed successfully.")

    def fga_setup(self, user, resource):
        response = self.add_tuple(user, resource)
        print(f"FGA setup response: {response}")
    
    async def main(self, user, resource):
        print("Starting FGA setup...")
        self.fga_setup(user, resource)
        self.partition_pdf(resource)
        print("Waiting for index to be updated. This may take a few seconds...")
        await asyncio.sleep(15)
        print("Starting search tool...")
        self.search_tool("test",user)
        self.search_tool("test",user+"-denyme")
        print("Process completed successfully.")

if __name__ == "__main__":
    dp = DocumentProcessor(
        azure_endpoint="",
        api_version="2024-04-01-preview",
        api_key="",
        mongo_uri="mongodb://localhost:27017?directConnection=true",
        fga_api_url='http://localhost:8080',
        fga_store_id='01J8VP1HYCHN459VT76DQG0W2R',
        fga_api_token='',
        authorization_model_id='01J8VP3BMPZNFJ480G5ZNF3H0C',
        db_name="demo",
        collection_name="mdb_fga"
    )
    asyncio.run(dp.main("demo_user", "demo.pdf"))