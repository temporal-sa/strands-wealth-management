import asyncio
import logging
import os
import sys
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv

from common.user_message import ProcessUserMessageInput
from temporal_supervisor.client_helper import connect_client
from temporal_supervisor.model_config import TASK_QUEUE
from temporal_supervisor.workflows.supervisor_workflow import WealthManagementWorkflow, ChatInput

load_dotenv()
logging.getLogger("temporalio").setLevel(logging.WARNING)


async def ainput(prompt: str = "") -> str:
    loop = asyncio.get_event_loop()
    return (await loop.run_in_executor(None, input, prompt)).strip()


def _text(item) -> str:
    if hasattr(item, "text_response"):
        return item.text_response
    return item.get("text_response", "") if isinstance(item, dict) else str(item)


async def main():
    client = await connect_client()
    workflow_id = f"wealth-management-{uuid.uuid4()}"
    handle = await client.start_workflow(
        WealthManagementWorkflow.run,
        ChatInput(),
        id=workflow_id,
        task_queue=TASK_QUEUE,
    )

    print("=" * 70)
    print("  Wealth Management Assistant (Temporal + Strands)")
    print("=" * 70)
    print(f"Workflow ID: {workflow_id}")
    print("Type your message and press Enter. Type 'exit', 'quit', or 'end' to stop.\n")

    printed_status = 0
    history_len = 0

    async def drain_status():
        nonlocal printed_status
        statuses = await handle.query(WealthManagementWorkflow.get_status_updates)
        while printed_status < len(statuses):
            print(f"\n[status] {statuses[printed_status]}")
            printed_status += 1

    while True:
        await drain_status()

        try:
            user_input = await ainput("You: ")
        except (EOFError, KeyboardInterrupt):
            await handle.signal(WealthManagementWorkflow.end_workflow)
            print("\nGoodbye!")
            break

        if not user_input:
            continue
        if user_input.lower() in ("exit", "quit", "end"):
            await handle.signal(WealthManagementWorkflow.end_workflow)
            print("Goodbye!")
            break

        await handle.signal(
            WealthManagementWorkflow.process_user_message,
            ProcessUserMessageInput(user_input=user_input),
        )

        # Wait for the assistant's reply, handling approval prompts and status
        # updates that arrive while the turn is being processed.
        while True:
            await asyncio.sleep(0.5)
            await drain_status()

            pending = await handle.query(WealthManagementWorkflow.get_pending_approval)
            if pending is not None:
                ans = await ainput(f"\n[approval needed] {pending}\nApprove? (yes/no): ")
                response = "approve" if ans.lower() in ("y", "yes", "approve") else "deny"
                await handle.signal(WealthManagementWorkflow.approve, response)
                continue

            history = await handle.query(WealthManagementWorkflow.get_chat_history)
            if len(history) > history_len:
                print(f"\nAssistant: {_text(history[-1])}\n")
                history_len = len(history)
                break


if __name__ == "__main__":
    asyncio.run(main())
