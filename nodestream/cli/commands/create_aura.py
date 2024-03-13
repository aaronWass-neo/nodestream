from .nodestream_command import NodestreamCommand
from ..operations import CreateAuraInstance
from cleo.helpers import argument


class CreateAura(NodestreamCommand):
    name = "create aura"
    description = "Create neo4j Aura instances via the Aura API"
    arguments = [
        argument("name", "instance name"),
        argument("region", "region to create new aura instance"),
        argument("instance_type", "instance type - possible values: professional-db, professional-ds, enterprise-db, enterprise-ds"),
        argument("memory", "GB RAM for instance"),
        argument("cloud_provider", "cloud provider the instance will be hosted in"),
        argument("tenant_id", "tenant_id for the new aura instance"),
        argument("aura_client_id", "aura api client id"),
        argument("aura_client_secret", "aura api client secret"),
    ]

    async def handle_async(self):
        name = self.argument("name")
        region = self.argument("region")
        instance_type = self.argument("instance_type")
        memory = self.argument("memory")
        cloud_provider = self.argument("cloud_provider")
        tenant_id = self.argument("tenant_id")
        aura_client_id = self.argument("aura_client_id")
        aura_client_secret = self.argument("aura_client_secret")

        await self.run_operation(
            CreateAuraInstance(name, region, instance_type, memory, cloud_provider, tenant_id, aura_client_id, aura_client_secret)
        )
