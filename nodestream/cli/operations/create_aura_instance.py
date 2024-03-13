from .operation import Operation
from ..commands.nodestream_command import NodestreamCommand
import json
from httpx import AsyncClient

class CreateAuraInstance(Operation):
    def __init__(
        self,
        aura_instance_name: str,
        region: str,
        instance_type: str,
        memory: str,
        cloud_provider: str, 
        tenant_id: str,
        aura_client_id: str,
        aura_client_secret: str,
    ) -> None:
        self.aura_instance_name = aura_instance_name
        self.region = region
        self.instance_type = instance_type
        self.memory = memory
        self.cloud_provider = cloud_provider
        self.tenant_id = tenant_id
        self.aura_client_id = aura_client_id
        self.aura_client_secret = aura_client_secret

    async def perform(self, command: NodestreamCommand):
        token = await self.get_bearer_token()
        response = await self.call_aura_create_api(token)
        print(json.dumps(response.json(), indent=4))
        

    async def get_bearer_token(self):
        async with AsyncClient() as client:
            response = await client.post(
                "https://api.neo4j.io/oauth/token",
                headers={"Content-type": "application/x-www-form-urlencoded"},
                data={"grant_type": "client_credentials"},
                auth=(self.aura_client_id, self.aura_client_secret)
            )
            return response.json()['access_token']
        
    async def call_aura_create_api(self, token: str):

        url = "https://api.neo4j.io/v1/instances"

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "nodestream",
        }

        data = {
            "version": "5",
            "region": self.region,
            "memory": self.memory,
            "name": self.aura_instance_name,
            "type": self.instance_type,
            "tenant_id": self.tenant_id,
            "cloud_provider": self.cloud_provider
        }

        async with AsyncClient() as client:
            response = await client.post(
                url=url,
                headers=headers,
                json=data,
            )
            return response
