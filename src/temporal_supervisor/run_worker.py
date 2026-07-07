import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
from temporalio.worker import Worker

from temporal_supervisor.client_helper import connect_client
from temporal_supervisor.model_config import TASK_QUEUE
from temporal_supervisor.activities.beneficiaries import Beneficiaries
from temporal_supervisor.activities.investments import Investments
from temporal_supervisor.activities.clients import ClientActivities
from temporal_supervisor.activities.open_account import OpenAccount
from temporal_supervisor.activities.event_stream_activities import EventStreamActivities
from temporal_supervisor.activities.approval import ApprovalActivities
from temporal_supervisor.workflows.supervisor_workflow import WealthManagementWorkflow
from temporal_supervisor.workflows.open_account_workflow import OpenInvestmentAccountWorkflow

load_dotenv()


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(filename)s:%(lineno)s | %(message)s",
    )

    client = await connect_client()

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[WealthManagementWorkflow, OpenInvestmentAccountWorkflow],
        activities=[
            Beneficiaries.list_beneficiaries,
            Beneficiaries.add_beneficiary,
            Beneficiaries.delete_beneficiary,
            Investments.list_investments,
            Investments.open_investment,
            Investments.close_investment,
            ClientActivities.get_client,
            ClientActivities.update_client,
            OpenAccount.get_current_client_info,
            OpenAccount.approve_kyc,
            OpenAccount.update_client_details,
            EventStreamActivities.append_chat_interaction,
            EventStreamActivities.append_status_update,
            EventStreamActivities.delete_conversation,
            ApprovalActivities.describe_approval,
        ],
    )
    print(f"Worker running on task queue '{TASK_QUEUE}'. Ctrl+C to exit.")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
