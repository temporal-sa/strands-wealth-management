"""The open-account agent is told the current status of every account opening on
each turn, so it never reports a finished opening (for example, one that was
rolled back) as still pending from what it remembers.

Runs the real ``WealthManagementWorkflow`` with a scripted model (registered under
the production model name). The open-account agent's scripted step replies with
the request it received, which is what the agent sees at the start of its turn.
Status changes are sent with the same signal an open-account child workflow
uses to report its progress.

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

from common.account_context import UpdateAccountOpeningStateInput
from common.user_message import ProcessUserMessageInput
from temporal_supervisor.model_config import LLM_TASK_QUEUE
from temporal_supervisor.workflows.supervisor_workflow import WealthManagementWorkflow

TASK_QUEUE = "test-account-opening-status"
CLIENT_ID = "123"
VACATION = "OpenAccount-test-123-Vacation-1"
WAITING = "Waiting Compliance Review"
ROLLED_BACK = "Rolled back. The account was not opened: The custodian rejected the account registration."


# --------------------------------------------------------------------------- #
# Scripted model: supervisor and investment agent delegate; the open-account
# agent replies with the request it received; each agent then relays the reply.
# --------------------------------------------------------------------------- #
def _tool_result(message: dict) -> str | None:
    """The text of the tool result in a message, or None if it has none."""
    for block in message.get("content", []):
        if isinstance(block, dict) and "toolResult" in block:
            return "".join(c.get("text", "") for c in block["toolResult"]["content"])
    return None


def _user_text(message: dict) -> str:
    return "".join(block.get("text", "") for block in message.get("content", []))


class ScriptedModel(Model):
    calls = 0

    def update_config(self, **_kw: Any) -> None:
        return None

    def get_config(self) -> dict:
        return {}

    def structured_output(self, *_a: Any, **_k: Any) -> Any:
        raise NotImplementedError

    async def stream(self, messages: Messages, tool_specs=None, *_a: Any, **_k: Any) -> AsyncIterable[StreamEvent]:
        ScriptedModel.calls += 1
        names = {ts["name"] for ts in (tool_specs or [])}
        delegate_to = next((n for n in ("investment_assistant", "open_account_assistant") if n in names), None)

        text = _tool_result(messages[-1])
        if text is None and delegate_to is None:  # open-account agent, at the start of its turn
            text = _user_text(messages[-1])

        if text is not None:
            events = [
                {"messageStart": {"role": "assistant"}},
                {"contentBlockDelta": {"contentBlockIndex": 0, "delta": {"text": text}}},
                {"contentBlockStop": {"contentBlockIndex": 0}},
                {"messageStop": {"stopReason": "end_turn"}},
            ]
        else:
            tool_input = {"client_id": CLIENT_ID, "request": "Open Vacation with $15,000"}
            events = [
                {"messageStart": {"role": "assistant"}},
                {"contentBlockStart": {"contentBlockIndex": 0, "start": {
                    "toolUse": {"name": delegate_to, "toolUseId": f"tu-{ScriptedModel.calls}"}}}},
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


@activity.defn(name="append_chat_interaction")
async def stub_append_chat_interaction(workflow_id: str, chat_interaction: Any) -> int:
    return 0


@activity.defn(name="append_status_update")
async def stub_append_status_update(workflow_id: str, status_update: Any) -> int:
    return 0


@activity.defn(name="delete_conversation")
async def stub_delete_conversation(workflow_id: str) -> bool:
    return True


STUB_ACTIVITIES = [stub_append_chat_interaction, stub_append_status_update, stub_delete_conversation]


@pytest.fixture(scope="module")
async def temporal_env():
    async with await WorkflowEnvironment.start_local(plugins=[StrandsPlugin(models=MODELS)]) as env:
        yield env


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


async def _report_state(handle, state: str) -> None:
    await handle.signal(
        WealthManagementWorkflow.update_account_opening_state,
        UpdateAccountOpeningStateInput(account_name="Vacation", state=state, workflow_id=VACATION),
    )


async def test_open_account_agent_gets_the_current_status_each_turn(temporal_env):
    worker = Worker(
        temporal_env.client,
        task_queue=TASK_QUEUE,
        workflows=[WealthManagementWorkflow],
        activities=STUB_ACTIVITIES,
    )
    async with worker, Worker(temporal_env.client, task_queue=LLM_TASK_QUEUE):
        handle = await temporal_env.client.start_workflow(
            WealthManagementWorkflow.run,
            id=f"opening-status-test-{uuid.uuid4()}", task_queue=TASK_QUEUE,
        )

        # No openings yet: the request is passed through unchanged.
        reply = await _send(handle, "Open Vacation with $15,000")
        assert reply == f"The client ID is {CLIENT_ID}. Open Vacation with $15,000"

        await _report_state(handle, WAITING)
        reply = await _send(handle, "Open Vacation with $15,000")
        assert f'Vacation (workflow ID {VACATION}): "{WAITING}"' in reply

        # The opening was rolled back after the agent last saw it waiting for
        # compliance. The agent is told the new status, not the one it remembers.
        await _report_state(handle, ROLLED_BACK)
        reply = await _send(handle, "Open Vacation with $15,000")
        assert f'Vacation (workflow ID {VACATION}): "{ROLLED_BACK}"' in reply
        assert WAITING not in reply

        await handle.signal(WealthManagementWorkflow.end_workflow)
