"""Live test of the agent routing, against the real Gemini model.

A customer starts with the Beneficiary Agent, then asks about their investments.
The supervisor calls the specialized agents as tools ("agents as tools"), so in the
same turn it should call investment_assistant and the reply should list the
investment accounts, without asking the customer whether they want to be
transferred, announcing a transfer, or asking for the client ID again.

The model decides where to route, so this can't be checked with a scripted model.
It runs the real ``WealthManagementWorkflow`` with real Gemini calls and the real
beneficiary and investment activities (which only read data/*.json). Only the
Redis-backed event stream activities are replaced with in-memory stand-ins, and the
chaos flags are stubbed off (they would fail open without Redis anyway). Every
model call is recorded, with the agent that made it, so the assertions can check
what each agent said during the turn.

Not part of the default ``uv run pytest`` run, because it calls the paid Gemini API
(pytest only collects test_*.py files). Run it with:

    source ./setgeminikey.sh && uv run pytest tests/live_agent_handoffs.py -s

LIVE_TEST_RUNS=<n> repeats the conversation n times (default 1), since model output
varies. Skipped when GEMINI_API_KEY is not set. Requires the Temporal test server
(downloaded automatically on first run); no Redis needed.
"""

import asyncio
import os
import re
import uuid
from typing import Any, AsyncGenerator

import pytest
from temporalio import activity
from temporalio.contrib.strands import StrandsPlugin
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from common.agent_constants import (
    BENE_INSTRUCTIONS,
    OPEN_ACCOUNT_INSTRUCTIONS,
    TEMPORAL_INVEST_SUBAGENT_INSTRUCTIONS,
    TEMPORAL_SUPERVISOR_INSTRUCTIONS,
)
from common.chaos_manager import ChaosManager
from common.user_message import ProcessUserMessageInput
from temporal_supervisor.activities.beneficiaries import Beneficiaries
from temporal_supervisor.activities.investments import Investments
from temporal_supervisor.model_config import (
    LLM_TASK_QUEUE,
    MODEL_ID,
    MODEL_NAME,
    ChaosAwareGeminiModel,
)
from temporal_supervisor.workflows.supervisor_workflow import WealthManagementWorkflow

RUNS = max(1, int(os.getenv("LIVE_TEST_RUNS", "1")))
TASK_QUEUE = "agent-handoffs-live-test"
# Client 123's beneficiaries in data/beneficiaries.json and accounts in data/investments.json
CLIENT_123_BENEFICIARIES = ["John", "Jane", "Joan"]
CLIENT_123_ACCOUNTS = ["Checking", "Savings", "401K"]

pytestmark = pytest.mark.skipif(
    not (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")),
    reason="GEMINI_API_KEY is not set; run `source ./setgeminikey.sh` first",
)

# --------------------------------------------------------------------------- #
# Recording model: the real Gemini model, recording what each agent said.
# --------------------------------------------------------------------------- #
AGENTS_BY_PROMPT = {
    TEMPORAL_SUPERVISOR_INSTRUCTIONS: "Supervisor Agent",
    BENE_INSTRUCTIONS: "Beneficiary Agent",
    TEMPORAL_INVEST_SUBAGENT_INSTRUCTIONS: "Investment Agent",
    OPEN_ACCOUNT_INSTRUCTIONS: "Open Account Agent",
}

# One entry per model call: {"agent", "text", "tool_calls"}, in order.
MODEL_CALLS: list = []


def _agent_name(system_prompt: Any) -> str:
    return AGENTS_BY_PROMPT.get(system_prompt or "", "Unknown Agent")


class RecordingGeminiModel(ChaosAwareGeminiModel):
    async def stream(self, messages: Any, tool_specs: Any = None, system_prompt: Any = None,
                     *args: Any, **kwargs: Any) -> AsyncGenerator[Any, None]:
        call = {"agent": _agent_name(system_prompt), "text": "", "tool_calls": []}
        async for event in super().stream(messages, tool_specs, system_prompt, *args, **kwargs):
            start = event.get("contentBlockStart", {}).get("start", {})
            if "toolUse" in start:
                call["tool_calls"].append(start["toolUse"]["name"])
            call["text"] += event.get("contentBlockDelta", {}).get("delta", {}).get("text", "")
            yield event
        MODEL_CALLS.append(call)


def recording_gemini_factory() -> RecordingGeminiModel:
    return RecordingGeminiModel(
        client_args={"api_key": os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")},
        model_id=MODEL_ID,
        params={"temperature": 0.2},
    )


# --------------------------------------------------------------------------- #
# In-memory stand-ins for the Redis-backed event stream activities.
# --------------------------------------------------------------------------- #
@activity.defn(name="append_chat_interaction")
async def stub_append_chat_interaction(workflow_id: str, chat_interaction: Any) -> int:
    return 0


@activity.defn(name="append_status_update")
async def stub_append_status_update(workflow_id: str, status_update: Any) -> int:
    return 0


@activity.defn(name="delete_conversation")
async def stub_delete_conversation(workflow_id: str) -> bool:
    return True


ACTIVITIES = [
    Beneficiaries.list_beneficiaries,
    Beneficiaries.add_beneficiary,
    Beneficiaries.delete_beneficiary,
    Investments.list_investments,
    Investments.close_investment,
    stub_append_chat_interaction,
    stub_append_status_update,
    stub_delete_conversation,
]


@pytest.fixture(autouse=True)
def chaos_off(monkeypatch):
    """No chaos flags (without Redis they read as off anyway)."""

    async def _off(self, flag):
        return False

    monkeypatch.setattr(ChaosManager, "is_enabled", _off)


@pytest.fixture(scope="module")
async def temporal_env():
    plugin = StrandsPlugin(models={MODEL_NAME: recording_gemini_factory})
    async with await WorkflowEnvironment.start_local(plugins=[plugin]) as env:
        yield env


def _trace(calls: list) -> str:
    lines = []
    for c in calls:
        parts = []
        if c["text"].strip():
            parts.append(" ".join(c["text"].split()))
        if c["tool_calls"]:
            parts.append(f"(calls {', '.join(c['tool_calls'])})")
        lines.append(f"    [{c['agent']}]: {' '.join(parts) or '(no output)'}")
    return "\n".join(lines)


async def _send(handle, text: str) -> tuple[str, list]:
    """Send a customer message; return the reply and the model calls made for it."""
    before = len(await handle.query(WealthManagementWorkflow.get_chat_history))
    first_call = len(MODEL_CALLS)
    await handle.signal(WealthManagementWorkflow.process_user_message, ProcessUserMessageInput(user_input=text))
    for _ in range(720):  # up to 3 minutes
        history = await handle.query(WealthManagementWorkflow.get_chat_history)
        if len(history) > before:
            return history[-1].text_response, MODEL_CALLS[first_call:]
        await asyncio.sleep(0.25)
    raise AssertionError(f"No reply to {text!r}")


async def _run_conversation(env: WorkflowEnvironment, run: int) -> dict:
    """Run the 3-message conversation once. Returns the result; never raises on a failed check."""
    transcript: list = []
    failures: list = []
    chatter = False

    handle = await env.client.start_workflow(
        WealthManagementWorkflow.run,
        id=f"agent-handoffs-live-{run}-{uuid.uuid4()}",
        task_queue=TASK_QUEUE,
    )

    async def say(text: str) -> tuple[str, list]:
        reply, calls = await _send(handle, text)
        transcript.extend([f"Customer: {text}", f"Agent: {reply}", "  trace:", _trace(calls)])
        return reply, calls

    try:
        await say("Who are my beneficiaries?")

        reply, _ = await say("123")
        for name in CLIENT_123_BENEFICIARIES:
            if name not in reply:
                failures.append(f"beneficiary {name} not listed after '123'")

        reply, calls = await say("What investments do I have?")
        for account in CLIENT_123_ACCOUNTS:
            if account not in reply:
                failures.append(f"account {account} not listed")
        if re.search(r"transfer", reply, re.I):
            failures.append("the reply talked about a transfer")
            chatter = True
        if re.search(r"client id", reply, re.I):
            failures.append("asked for the client ID again")
        # What every agent said during the turn: no agent (the Beneficiary Agent in
        # particular) should talk about transferring the customer.
        for c in calls:
            if re.search(r"transfer", c["text"], re.I):
                chatter = True
                failures.append(f"the {c['agent']} talked about a transfer: {c['text'].strip()!r}")
        # Agents as tools: the supervisor must have called the investment tool, and
        # the Investment Agent must have run.
        if not any(c["agent"] == "Supervisor Agent" and "investment_assistant" in c["tool_calls"] for c in calls):
            failures.append("the supervisor did not call investment_assistant")
        if not any(c["agent"] == "Investment Agent" for c in calls):
            failures.append("the turn never reached the Investment Agent")
    finally:
        await handle.signal(WealthManagementWorkflow.end_workflow)
        await handle.result()

    text = f"\nRun {run} of {RUNS}:\n" + "\n".join(transcript)
    print(text)
    return {"run": run, "failures": failures, "chatter": chatter, "transcript": text}


async def test_investment_question_while_talking_to_the_beneficiary_agent(temporal_env):
    results = []
    async with Worker(
        temporal_env.client,
        task_queue=TASK_QUEUE,
        workflows=[WealthManagementWorkflow],
        activities=ACTIVITIES,
    ), Worker(temporal_env.client, task_queue=LLM_TASK_QUEUE):  # the model calls run here
        for run in range(1, RUNS + 1):
            results.append(await _run_conversation(temporal_env, run))

    failed = [r for r in results if r["failures"]]
    chatter = sum(r["chatter"] for r in results)
    print(f"\nLive agent handoffs: {RUNS - len(failed)}/{RUNS} runs passed, {chatter} with transfer chatter")
    assert not failed, "\n".join(
        f"Run {r['run']} failed: {'; '.join(r['failures'])}{r['transcript']}" for r in failed
    )
