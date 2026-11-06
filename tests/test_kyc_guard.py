"""The approve_kyc tool only approves KYC after the customer has seen and confirmed
their details for that account opening.

The first test checks the guard itself. The others run the real
``WealthManagementWorkflow`` with a scripted model (registered under the
production model name) that calls approve_kyc too early, and stub activities
(registered under the real activity names) that record which account openings
KYC was actually approved for.

Requires the Temporal test server (downloaded automatically on first run). No
GEMINI_API_KEY or Redis needed.
"""

import asyncio
import json
import uuid
from collections.abc import AsyncIterable
from typing import Any

import pytest
from strands.models import Model
from strands.types.content import Messages
from strands.types.streaming import StreamEvent
from temporalio import activity, workflow
from temporalio.contrib.strands import StrandsPlugin
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from common.user_message import ProcessUserMessageInput
from temporal_supervisor.model_config import LLM_TASK_QUEUE, OPEN_ACCOUNT_TASK_QUEUE
from temporal_supervisor.workflows.supervisor_workflow import (
    WealthManagementWorkflow,
    kyc_confirmed,
)

TASK_QUEUE = "test-kyc-guard"
CLIENT_ID = "123"
CAR = "OpenAccount-test-123-Car"
VACATION = "OpenAccount-test-123-Vacation"

# Account openings that KYC was approved for, in order, recorded worker-side.
APPROVED: list = []


@pytest.mark.parametrize(
    "shown_turns, expected",
    [
        ({}, False),  # details never shown for this opening
        ({VACATION: 2}, False),  # only shown for another opening
        ({CAR: 3}, False),  # shown in the current turn; the customer hasn't replied yet
        ({CAR: 2}, True),  # shown in an earlier turn; the customer has replied since
    ],
)
def test_kyc_is_not_approved_until_the_customer_confirms(shown_turns, expected):
    assert kyc_confirmed(shown_turns.get(CAR), 3) is expected


# --------------------------------------------------------------------------- #
# Scripted model: each model call takes the next step from SCRIPT. A step is a
# tool call, or ECHO, which replies with the text of the latest tool result so
# that what the open-account tools returned reaches the chat response.
# --------------------------------------------------------------------------- #
SCRIPT: list = []
ECHO = ("echo",)


def tool(name: str, **tool_input: Any) -> tuple:
    return ("tool", name, tool_input)


def delegate() -> list:
    """Supervisor -> investment agent -> open-account agent."""
    return [
        tool("investment_assistant", client_id=CLIENT_ID, request="relay"),
        tool("open_account_assistant", client_id=CLIENT_ID, request="relay"),
    ]


def relay() -> list:
    """Open-account agent, investment agent, then supervisor each pass the result up."""
    return [ECHO, ECHO, ECHO]


def _last_tool_result(messages: Messages) -> str:
    for m in reversed(messages):
        for block in m.get("content", []):
            if isinstance(block, dict) and "toolResult" in block:
                return "".join(c.get("text", "") for c in block["toolResult"]["content"])
    return ""


class ScriptedModel(Model):
    calls = 0

    def update_config(self, **_kw: Any) -> None:
        return None

    def get_config(self) -> dict:
        return {}

    def structured_output(self, *_a: Any, **_k: Any) -> Any:
        raise NotImplementedError

    async def stream(self, messages: Messages, *_a: Any, **_k: Any) -> AsyncIterable[StreamEvent]:
        step = SCRIPT.pop(0)
        ScriptedModel.calls += 1
        if step == ECHO:
            events = [
                {"messageStart": {"role": "assistant"}},
                {"contentBlockDelta": {"contentBlockIndex": 0,
                                       "delta": {"text": _last_tool_result(messages)}}},
                {"contentBlockStop": {"contentBlockIndex": 0}},
                {"messageStop": {"stopReason": "end_turn"}},
            ]
        else:
            _, name, tool_input = step
            events = [
                {"messageStart": {"role": "assistant"}},
                {"contentBlockStart": {"contentBlockIndex": 0, "start": {
                    "toolUse": {"name": name, "toolUseId": f"tu-{ScriptedModel.calls}"}}}},
                {"contentBlockDelta": {"contentBlockIndex": 0,
                                       "delta": {"toolUse": {"input": json.dumps(tool_input)}}}},
                {"contentBlockStop": {"contentBlockIndex": 0}},
                {"messageStop": {"stopReason": "tool_use"}},
            ]
        for event in events:
            yield event
        yield {"metadata": {"usage": {"inputTokens": 1, "outputTokens": 1, "totalTokens": 2},
                            "metrics": {"latencyMs": 1}}}


MODELS = {"gemini": lambda: ScriptedModel()}  # MODEL_NAME in model_config is "gemini"


# --------------------------------------------------------------------------- #
# Stub activities, under the real activity names.
# --------------------------------------------------------------------------- #
@activity.defn(name="get_current_client_info")
async def stub_get_current_client_info(workflow_id: str) -> dict:
    return {
        "client_id": CLIENT_ID, "first_name": "Sam", "last_name": "Doe", "address": "1 Main St",
        "phone": "555", "email": "s@example.com", "marital_status": "single",
    }


@activity.defn(name="approve_kyc")
async def stub_approve_kyc(workflow_id: str) -> str:
    APPROVED.append(workflow_id)
    return "approved"


@activity.defn(name="append_chat_interaction")
async def stub_append_chat_interaction(workflow_id: str, chat_interaction: Any) -> int:
    return 0


@activity.defn(name="append_status_update")
async def stub_append_status_update(workflow_id: str, status_update: Any) -> int:
    return 0


@activity.defn(name="delete_conversation")
async def stub_delete_conversation(workflow_id: str) -> bool:
    return True


@activity.defn(name="validate_new_account")
async def stub_validate_new_account(client_id: str, account_name: str, initial_amount: float) -> None:
    return None


@activity.defn(name="find_promotion")
async def stub_find_promotion(client_id: str) -> None:
    return None


@workflow.defn(name="OpenInvestmentAccountWorkflow")
class StubOpenAccountWorkflow:
    """Stands in for the open-account child: runs until it is told to finish."""

    def __init__(self) -> None:
        self.finished = False

    @workflow.run
    async def run(self, inputs: Any) -> None:
        await workflow.wait_condition(lambda: self.finished)

    @workflow.signal
    def finish(self) -> None:
        self.finished = True


STUB_ACTIVITIES = [
    stub_get_current_client_info, stub_approve_kyc, stub_validate_new_account, stub_find_promotion,
    stub_append_chat_interaction, stub_append_status_update, stub_delete_conversation,
]


@pytest.fixture(scope="module")
async def temporal_env():
    async with await WorkflowEnvironment.start_local(plugins=[StrandsPlugin(models=MODELS)]) as env:
        yield env


def _workers(env: WorkflowEnvironment):
    return Worker(
        env.client,
        task_queue=TASK_QUEUE,
        workflows=[WealthManagementWorkflow],
        activities=STUB_ACTIVITIES,
    ), Worker(env.client, task_queue=LLM_TASK_QUEUE)


async def _send(handle, text: str) -> str:
    """Send a customer message and return the reply to it."""
    before = len(await handle.query(WealthManagementWorkflow.get_chat_history))
    await handle.signal(WealthManagementWorkflow.process_user_message, ProcessUserMessageInput(user_input=text))
    for _ in range(160):
        history = await handle.query(WealthManagementWorkflow.get_chat_history)
        if len(history) > before:
            return history[-1].text_response
        await asyncio.sleep(0.25)
    raise AssertionError(f"No reply to {text!r}")


async def test_kyc_approved_only_after_the_customer_replies(temporal_env):
    APPROVED.clear()
    SCRIPT[:] = [
        # Customer asks to open Car. The model shows their details, then approves
        # KYC in the same turn without waiting for the customer to answer.
        *delegate(),
        tool("get_current_client_info", workflow_id=CAR),
        tool("approve_kyc", workflow_id=CAR),
        *relay(),
        # Customer confirms.
        *delegate(),
        tool("approve_kyc", workflow_id=CAR),
        *relay(),
        # Customer asks to open Vacation. The model treats the Car confirmation as
        # approval for this opening too.
        *delegate(),
        tool("get_current_client_info", workflow_id=VACATION),
        tool("approve_kyc", workflow_id=VACATION),
        *relay(),
        # Customer confirms.
        *delegate(),
        tool("approve_kyc", workflow_id=VACATION),
        *relay(),
    ]

    worker, llm_worker = _workers(temporal_env)
    async with worker, llm_worker:
        handle = await temporal_env.client.start_workflow(
            WealthManagementWorkflow.run,
            id=f"kyc-guard-test-{uuid.uuid4()}", task_queue=TASK_QUEUE,
        )

        reply = await _send(handle, "Please open an account called Car.")
        assert "KYC was not approved" in reply
        assert APPROVED == []

        reply = await _send(handle, "Yes, that's correct.")
        assert "KYC approved" in reply
        assert APPROVED == [CAR]

        reply = await _send(handle, "Now open one called Vacation.")
        assert "KYC was not approved" in reply
        assert APPROVED == [CAR]

        reply = await _send(handle, "Yes, still correct.")
        assert "KYC approved" in reply
        assert APPROVED == [CAR, VACATION]

        assert SCRIPT == []
        await handle.signal(WealthManagementWorkflow.end_workflow)


async def test_reopening_an_account_reuses_its_workflow_id(temporal_env):
    # The child workflow ID is OpenAccount-<chat ID>-<client ID>-<account name>, as in
    # the other ports. Opening the same name again fails while the first opening is
    # running, and starts a new run with the same ID once it has finished, without
    # carrying over the earlier KYC confirmation.
    APPROVED.clear()
    chat_id = f"kyc-guard-test-{uuid.uuid4()}"
    car = f"OpenAccount-{chat_id}-{CLIENT_ID}-Car"
    open_car = tool("open_new_investment_account", client_id=CLIENT_ID, account_name="Car", initial_amount=5000.0)
    SCRIPT[:] = [
        *delegate(), open_car, *relay(),  # opens Car
        *delegate(), open_car, *relay(),  # opens Car again while the first is running
        *delegate(), tool("get_current_client_info", workflow_id=car), *relay(),  # shows the details
        *delegate(), open_car, *relay(),  # reopens Car after the first finished
        *delegate(), tool("approve_kyc", workflow_id=car), *relay(),  # approves with no new details shown
    ]

    worker, llm_worker = _workers(temporal_env)
    child_worker = Worker(
        temporal_env.client,
        task_queue=OPEN_ACCOUNT_TASK_QUEUE,
        workflows=[StubOpenAccountWorkflow],
        workflow_runner=UnsandboxedWorkflowRunner(),
    )
    async with worker, llm_worker, child_worker:
        handle = await temporal_env.client.start_workflow(
            WealthManagementWorkflow.run, id=chat_id, task_queue=TASK_QUEUE,
        )

        reply = await _send(handle, "Open an account called Car with $5,000.")
        assert f"Workflow ID: {car}" in reply
        first_run = (await temporal_env.client.get_workflow_handle(car).describe()).run_id

        reply = await _send(handle, "Open Car with $5,000.")
        assert "already in progress" in reply and car in reply

        await _send(handle, "Show my details.")
        child = temporal_env.client.get_workflow_handle(car, run_id=first_run)
        await child.signal("finish")
        await child.result()

        reply = await _send(handle, "Open Car with $5,000 again.")
        assert f"Workflow ID: {car}" in reply
        assert (await temporal_env.client.get_workflow_handle(car).describe()).run_id != first_run

        reply = await _send(handle, "Yes, that's correct.")
        assert "KYC was not approved" in reply
        assert APPROVED == []

        assert SCRIPT == []
        await handle.signal(WealthManagementWorkflow.end_workflow)
