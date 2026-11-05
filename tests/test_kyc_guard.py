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
from temporalio import activity
from temporalio.contrib.strands import StrandsPlugin
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from common.user_message import ProcessUserMessageInput
from temporal_supervisor.model_config import LLM_TASK_QUEUE
from temporal_supervisor.workflows.supervisor_workflow import (
    WealthManagementWorkflow,
    kyc_confirmed,
)

TASK_QUEUE = "test-kyc-guard"
CLIENT_ID = "123"
CAR = "open-account-test-123-car"
VACATION = "open-account-test-123-vacation"

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


STUB_ACTIVITIES = [
    stub_get_current_client_info, stub_approve_kyc,
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
