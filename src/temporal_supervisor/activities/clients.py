from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from common.client_manager import ClientManager


@dataclass
class WealthManagementClient:
    client_id: str
    first_name: str
    last_name: str
    address: str
    phone: str
    email: str
    marital_status: str


class ClientActivities:
    retry_policy = RetryPolicy(
        initial_interval=timedelta(seconds=1),
        backoff_coefficient=2,
        maximum_interval=timedelta(seconds=30),
    )

    @staticmethod
    @activity.defn
    async def get_client(client_id: str) -> "WealthManagementClient | None":
        activity.logger.info(f"get_client: {client_id}")
        client_dict = ClientManager().get_client(client_id=client_id)
        if "error" in client_dict:
            return None
        client_dict["client_id"] = client_id
        return WealthManagementClient(**client_dict)

    @staticmethod
    @activity.defn
    async def update_client(client_id: str, field_dict: dict) -> str:
        activity.logger.info(f"update_client: {client_id}, {field_dict}")
        return ClientManager().update_client(client_id, field_dict)
