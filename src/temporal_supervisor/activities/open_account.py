from dataclasses import asdict, is_dataclass

from temporalio import activity, workflow

with workflow.unsafe.imports_passed_through():
    from temporal_supervisor.client_helper import connect_client
    from temporal_supervisor.workflows.open_account_workflow import OpenInvestmentAccountWorkflow


class OpenAccount:
    """Tools the agent uses to drive an in-progress open-account child workflow.

    These run as Activities because they open a Temporal client connection to
    signal/update the child workflow (non-deterministic I/O).
    """

    @staticmethod
    async def _handle(workflow_id: str):
        client = await connect_client()
        return client.get_workflow_handle_for(OpenInvestmentAccountWorkflow.run, workflow_id)

    @staticmethod
    @activity.defn
    async def get_current_client_info(workflow_id: str) -> dict:
        """Retrieve the current client details for the in-progress account opening.

        Args:
            workflow_id: The open-account child workflow ID returned by open_new_investment_account.
        """
        handle = await OpenAccount._handle(workflow_id)
        client = await handle.execute_update(OpenInvestmentAccountWorkflow.get_client_details)
        return asdict(client) if is_dataclass(client) else client

    @staticmethod
    @activity.defn
    async def approve_kyc(workflow_id: str) -> str:
        """Approve KYC for the in-progress account opening (the client info is correct).

        Args:
            workflow_id: The open-account child workflow ID returned by open_new_investment_account.
        """
        handle = await OpenAccount._handle(workflow_id)
        await handle.signal(OpenInvestmentAccountWorkflow.verify_kyc)
        return "KYC approved. The account is now waiting for compliance review."

    @staticmethod
    @activity.defn
    async def update_client_details(workflow_id: str, client_fields: dict) -> str:
        """Update client fields during account opening. Also completes KYC.

        Args:
            workflow_id: The open-account child workflow ID.
            client_fields: A dict of only the fields to change.
        """
        handle = await OpenAccount._handle(workflow_id)
        return await handle.execute_update(
            OpenInvestmentAccountWorkflow.update_client_details, args=[client_fields]
        )
