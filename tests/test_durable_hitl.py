"""Regression test: durable human-in-the-loop across nested sub-agents.

Runs the real ``WealthManagementWorkflow`` in a local Temporal environment, but
with a deterministic scripted model (registered under the production model name)
and lightweight stub activities that record side effects instead of touching
Redis or the data files.

It guards the behavior proven while designing the supervisor -> sub-agent
refactor: an approval interrupt raised inside a specialized sub-agent
(delete_beneficiary's ApprovalHook) must propagate up through the supervisor to
the workflow's approval loop, and the human's decision must flow back down to the
same sub-agent.

  - approve -> the delete activity runs exactly once
  - deny    -> the delete activity never runs

Both assert that exactly one approval was surfaced via the workflow query.

Requires the Temporal test server (downloaded automatically on first run). No
GEMINI_API_KEY or Redis needed.
"""

import asyncio
import json
import uuid
from collections.abc import AsyncIterable
from datetime import timedelta
from typing import Any

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from temporalio.contrib.strands import StrandsPlugin

from strands.models import Model
from strands.types.content import Messages, SystemContentBlock
from strands.types.streaming import StreamEvent
from strands.types.tools import ToolChoice, ToolSpec

from common.user_message import ProcessUserMessageInput
from temporal_supervisor.workflows.supervisor_workflow import WealthManagementWorkflow, ChatInput

TASK_QUEUE = "test-durable-hitl"
CLIENT_ID = "C-1"
BENE_ID = "b1"

# Records real deletes worker-side (same process as the test).
DELETE_CALLS: list = []


# --------------------------------------------------------------------------- #
# Scripted model: drives supervisor -> beneficiary_assistant -> delete_beneficiary
# deterministically. One factory registered under the production model name so the
# real workflow's TemporalAgent(model=MODEL_NAME) picks it up. It decides what to
# emit from the tools it is offered + whether a tool result is already present, so
# it is replay-safe across the interrupt/resume re-run.
# --------------------------------------------------------------------------- #
def _has_tool_result(messages: Messages) -> bool:
    return any(
        isinstance(block, dict) and "toolResult" in block
        for m in messages
        for block in m.get("content", [])
    )


def _tool_use_events(name: str, tool_input: dict) -> list[StreamEvent]:
    return [
        {"messageStart": {"role": "assistant"}},
        {"contentBlockStart": {"contentBlockIndex": 0,
                               "start": {"toolUse": {"name": name, "toolUseId": f"tu-{name}"}}}},
        {"contentBlockDelta": {"contentBlockIndex": 0,
                               "delta": {"toolUse": {"input": json.dumps(tool_input)}}}},
        {"contentBlockStop": {"contentBlockIndex": 0}},
        {"messageStop": {"stopReason": "tool_use"}},
    ]


def _text_events(text: str) -> list[StreamEvent]:
    return [
        {"messageStart": {"role": "assistant"}},
        {"contentBlockDelta": {"contentBlockIndex": 0, "delta": {"text": text}}},
        {"contentBlockStop": {"contentBlockIndex": 0}},
        {"messageStop": {"stopReason": "end_turn"}},
    ]


class ScriptedModel(Model):
    def update_config(self, **_kw: Any) -> None:
        return None

    def get_config(self) -> dict:
        return {}

    def structured_output(self, *_a: Any, **_k: Any) -> Any:
        raise NotImplementedError

    async def stream(
        self,
        messages: Messages,
        tool_specs: list[ToolSpec] | None = None,
        system_prompt: str | None = None,
        *,
        tool_choice: ToolChoice | None = None,
        system_prompt_content: list[SystemContentBlock] | None = None,
        invocation_state: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> AsyncIterable[StreamEvent]:
        names = {ts["name"] for ts in (tool_specs or [])}

        if _has_tool_result(messages):
            events = _text_events("All done.")
        elif "beneficiary_assistant" in names:  # supervisor turn
            events = _tool_use_events(
                "beneficiary_assistant",
                {"client_id": CLIENT_ID, "request": f"delete beneficiary {BENE_ID}"},
            )
        elif "delete_beneficiary" in names:  # beneficiary sub-agent turn
            events = _tool_use_events(
                "delete_beneficiary", {"client_id": CLIENT_ID, "beneficiary_id": BENE_ID}
            )
        else:
            events = _text_events("All done.")

        for event in events:
            yield event
        yield {"metadata": {"usage": {"inputTokens": 1, "outputTokens": 1, "totalTokens": 2},
                            "metrics": {"latencyMs": 1}}}


MODELS = {"gemini": lambda: ScriptedModel()}  # MODEL_NAME in model_config is "gemini"


# --------------------------------------------------------------------------- #
# Stub activities: same @activity.defn names as the real ones (Temporal resolves
# by name), so the real workflow runs unchanged while we record side effects and
# avoid Redis / data-file writes.
# --------------------------------------------------------------------------- #
@activity.defn(name="list_beneficiaries")
async def stub_list_beneficiaries(client_id: str) -> list:
    return [{"id": BENE_ID, "first_name": "Sam", "last_name": "Doe", "relationship": "spouse"}]


@activity.defn(name="add_beneficiary")
async def stub_add_beneficiary(client_id: str, first_name: str, last_name: str, relationship: str) -> str:
    return "added"


@activity.defn(name="delete_beneficiary")
async def stub_delete_beneficiary(client_id: str, beneficiary_id: str) -> str:
    DELETE_CALLS.append((client_id, beneficiary_id))
    return f"Beneficiary {beneficiary_id} deleted successfully."


@activity.defn(name="list_investments")
async def stub_list_investments(client_id: str) -> list:
    return []


@activity.defn(name="close_investment")
async def stub_close_investment(client_id: str, investment_id: str) -> str:
    return "closed"


@activity.defn(name="append_chat_interaction")
async def stub_append_chat_interaction(workflow_id: str, chat_interaction: Any) -> int:
    return 0


@activity.defn(name="append_status_update")
async def stub_append_status_update(workflow_id: str, status_update: Any) -> int:
    return 0


@activity.defn(name="delete_conversation")
async def stub_delete_conversation(workflow_id: str) -> bool:
    return True


@activity.defn(name="describe_approval")
async def stub_describe_approval(tool_name: str, tool_input: dict) -> str:
    return f"Are you sure you want to run {tool_name}?"


STUB_ACTIVITIES = [
    stub_list_beneficiaries, stub_add_beneficiary, stub_delete_beneficiary,
    stub_list_investments, stub_close_investment,
    stub_append_chat_interaction, stub_append_status_update, stub_delete_conversation,
    stub_describe_approval,
]


async def _run_scenario(env: WorkflowEnvironment, human_response: str) -> dict:
    DELETE_CALLS.clear()
    client = env.client
    async with Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[WealthManagementWorkflow],
        activities=STUB_ACTIVITIES,
    ):
        handle = await client.start_workflow(
            WealthManagementWorkflow.run,
            ChatInput(),
            id=f"hitl-{human_response}-{uuid.uuid4()}",
            task_queue=TASK_QUEUE,
        )
        await handle.signal(
            WealthManagementWorkflow.process_user_message,
            ProcessUserMessageInput(user_input=f"Please delete beneficiary {BENE_ID} for client {CLIENT_ID}"),
        )

        approvals = 0
        for _ in range(160):
            pending = await handle.query(WealthManagementWorkflow.get_pending_approval)
            if pending is not None:
                approvals += 1
                await handle.signal(WealthManagementWorkflow.approve, human_response)
            history = await handle.query(WealthManagementWorkflow.get_chat_history)
            if history:
                break
            await asyncio.sleep(0.25)

        await handle.signal(WealthManagementWorkflow.end_workflow)
        return {"approvals": approvals, "deletes": list(DELETE_CALLS)}


@pytest.fixture(scope="module")
async def temporal_env():
    async with await WorkflowEnvironment.start_local(
        plugins=[StrandsPlugin(models=MODELS)]
    ) as env:
        yield env


async def test_approve_runs_delete(temporal_env):
    result = await _run_scenario(temporal_env, "approve")
    assert result["approvals"] == 1, "exactly one approval should surface to the workflow"
    assert result["deletes"] == [(CLIENT_ID, BENE_ID)], "delete must run once after approval"


async def test_deny_blocks_delete(temporal_env):
    result = await _run_scenario(temporal_env, "deny")
    assert result["approvals"] == 1, "exactly one approval should surface to the workflow"
    assert result["deletes"] == [], "delete must NOT run after denial"


if __name__ == "__main__":
    # Allow running standalone without pytest.
    async def _main():
        async with await WorkflowEnvironment.start_local(
            plugins=[StrandsPlugin(models=MODELS)]
        ) as env:
            ap = await _run_scenario(env, "approve")
            dn = await _run_scenario(env, "deny")
            print("APPROVE:", ap)
            print("DENY:   ", dn)
            ok = ap == {"approvals": 1, "deletes": [(CLIENT_ID, BENE_ID)]} and \
                dn == {"approvals": 1, "deletes": []}
            print("PASS" if ok else "FAIL")

    asyncio.run(_main())