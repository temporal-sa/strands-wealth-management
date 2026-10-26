import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
from temporalio.worker import Worker

from temporal_supervisor.client_helper import connect_client
from temporal_supervisor.model_config import LLM_MAX_CALLS_PER_SECOND, LLM_TASK_QUEUE, TASK_QUEUE
from temporal_supervisor.activities.beneficiaries import Beneficiaries
from temporal_supervisor.activities.investments import Investments
from temporal_supervisor.activities.clients import ClientActivities
from temporal_supervisor.activities.custodian import Custodian
from temporal_supervisor.activities.funding import Funding
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
            Investments.validate_new_account,
            Investments.find_promotion,
            Investments.open_investment,
            Investments.remove_investment,
            Investments.close_investment,
            Funding.debit_linked_account,
            Funding.credit_linked_account,
            Custodian.register_account,
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
    # Model calls run on their own task queue so they can be rate limited without
    # slowing down the tools. The StrandsPlugin on the client registers the model
    # activities on every worker, so this one needs nothing else. The limit is
    # enforced by the Temporal service across all workers polling the queue.
    llm_worker = Worker(
        client,
        task_queue=LLM_TASK_QUEUE,
        max_task_queue_activities_per_second=LLM_MAX_CALLS_PER_SECOND,
    )
    limit = f"{LLM_MAX_CALLS_PER_SECOND}/s" if LLM_MAX_CALLS_PER_SECOND else "unlimited"
    print(f"Worker running on task queues '{TASK_QUEUE}' and '{LLM_TASK_QUEUE}' (model calls: {limit}). Ctrl+C to exit.")
    await asyncio.gather(worker.run(), llm_worker.run())


if __name__ == "__main__":
    asyncio.run(main())
