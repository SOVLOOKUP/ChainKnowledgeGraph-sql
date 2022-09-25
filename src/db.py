from surrealdb import HTTPClient

class Surrealdb:
    def __init__(self, url: str, namespace:str, database:str, username:str, password:str):
        self.client = HTTPClient(url,namespace=namespace,database=database,username=username,password=password)

    async def  __aenter__(self):
        await self.client.connect()
        return self.client

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        await self.client.disconnect()
