"""Live test of opening an account, against the real Gemini model.

The customer opens a Vacation account, confirms their details, and compliance
approves it. The custodian then rejects the account, so the saga rolls it back.
The customer asks to open Vacation again. Checked:

  - No agent tells the customer the account is open before it is.
  - After the rollback, the new request starts a new account opening (with its
    own KYC) instead of being told the previous one is still waiting for
    compliance review.
  - Account-opening replies don't end with the "open, close, or list" next steps,
    which don't fit a reply that asks the customer to confirm their details.
  - Each opening's child workflow ID is OpenAccount-<chat ID>-<client ID>-<account
    name>, as in the other ports, so the reopened account reuses the ID.
  - Listing the investments afterwards puts each account on its own line.

The model decides what to say, so this can't be checked with a scripted model.
It runs the real ``WealthManagementWorkflow`` and ``OpenInvestmentAccountWorkflow``
with real Gemini calls and the real read-only activities (data/*.json). Stubbed:
the saga steps that would write the data files (the custodian always rejects),
the KYC activities (they signal the child through this test's Temporal client
instead of connecting to a server), the Redis-backed event stream, and the chaos
flags (off). Every model call is recorded, with the agent that made it.

Not part of the default ``uv run pytest`` run, because it calls the paid Gemini API
(pytest only collects test_*.py files). Run it with:

    source ./setgeminikey.sh && uv run pytest tests/live_account_opening.py -s

LIVE_TEST_RUNS=<n> repeats the conversation n times (default 1), since model output
varies. Skipped when GEMINI_API_KEY is not set. Requires the Temporal test server
(downloaded automatically on first run); no Redis needed.
"""

import asyncio
import os
import re
import uuid
from dataclasses import asdict
from typing import Any

import pytest
from temporalio import activity
from temporalio.contrib.strands import StrandsPlugin
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from common.chaos_manager import ChaosManager
from common.user_message import ProcessUserMessageInput
from live_agent_handoffs import (
    CLIENT_123_ACCOUNTS,
    MODEL_CALLS,
    _trace,
    listed_on_own_line,
    recording_gemini_factory,
    stub_append_chat_interaction,
    stub_append_status_update,
    stub_delete_conversation,
)
from temporal_supervisor.activities.clients import ClientActivities
from temporal_supervisor.activities.investments import Investments
from temporal_supervisor.model_config import LLM_TASK_QUEUE, MODEL_NAME, OPEN_ACCOUNT_TASK_QUEUE
from temporal_supervisor.workflows.open_account_workflow import OpenInvestmentAccountWorkflow
from temporal_supervisor.workflows.supervisor_workflow import WealthManagementWorkflow

RUNS = max(1, int(os.getenv("LIVE_TEST_RUNS", "1")))
# The supervisor starts the child on OPEN_ACCOUNT_TASK_QUEUE; run both there.
TASK_QUEUE = OPEN_ACCOUNT_TASK_QUEUE
CLIENT_FIRST_NAME = "Don"  # client 123 in data/clients.json

pytestmark = pytest.mark.skipif(
    not (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")),
    reason="GEMINI_API_KEY is not set; run `source ./setgeminikey.sh` first",
)

# Says the account is open or created ("has been opened", "I've created", "is now open"),
# but not "was not opened" or "opening started".
CLAIMS_OPEN = re.compile(
    r"\b(?:I'?ve|I have|has been|have been|was|is now|is)\s+(?:successfully\s+)?(?:opened|created|open)\b",
    re.I,
)
STALE_STATUS = re.compile(r"still (?:waiting|pending|in progress)|please wait", re.I)
NEXT_STEPS = re.compile(r"open, close, or list", re.I)

# Workflow IDs of the account openings KYC was approved for, in order.
KYC_APPROVED: list = []
ENV: dict = {}


# --------------------------------------------------------------------------- #
# KYC activities: the real ones connect to the configured Temporal server, so
# drive the child through the test environment's client instead.
# --------------------------------------------------------------------------- #
def _child(workflow_id: str):
    return ENV["client"].get_workflow_handle_for(OpenInvestmentAccountWorkflow.run, workflow_id)


@activity.defn(name="get_current_client_info")
async def stub_get_current_client_info(workflow_id: str) -> dict:
    client = await _child(workflow_id).execute_update(OpenInvestmentAccountWorkflow.get_client_details)
    return asdict(client)


@activity.defn(name="approve_kyc")
async def stub_approve_kyc(workflow_id: str) -> str:
    await _child(workflow_id).signal(OpenInvestmentAccountWorkflow.verify_kyc)
    KYC_APPROVED.append(workflow_id)
    return "KYC approved. The account is now waiting for compliance review."


@activity.defn(name="update_client_details")
async def stub_update_client_details(workflow_id: str, client_fields: dict) -> str:
    return await _child(workflow_id).execute_update(
        OpenInvestmentAccountWorkflow.update_client_details, args=[client_fields]
    )


# --------------------------------------------------------------------------- #
# Saga steps: record instead of writing data files; the custodian rejects.
# --------------------------------------------------------------------------- #
@activity.defn(name="open_investment")
async def stub_open_investment(new_account: Any) -> dict:
    return {"investment_id": "i-live-test", "name": "Vacation", "balance": 0.0}


@activity.defn(name="remove_investment")
async def stub_remove_investment(client_id: str, investment_id: str) -> bool:
    return True


@activity.defn(name="debit_linked_account")
async def stub_debit(client_id: str, amount: float, transfer_id: str) -> dict:
    return {}


@activity.defn(name="credit_linked_account")
async def stub_credit(client_id: str, amount: float, transfer_id: str) -> dict:
    return {}


@activity.defn(name="register_account")
async def stub_register(client_id: str, investment_id: str, amount: float) -> str:
    raise ApplicationError(
        "The custodian rejected the account registration", type="CustodianRejected", non_retryable=True
    )


ACTIVITIES = [
    Investments.list_investments,
    Investments.validate_new_account,
    Investments.find_promotion,
    ClientActivities.get_client,
    stub_get_current_client_info,
    stub_approve_kyc,
    stub_update_client_details,
    stub_open_investment,
    stub_remove_investment,
    stub_debit,
    stub_credit,
    stub_register,
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
        ENV["client"] = env.client
        yield env


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


async def _wait_for_status(handle, prefix: str) -> None:
    for _ in range(240):  # up to a minute
        if any(prefix in s for s in await handle.query(WealthManagementWorkflow.get_status_updates)):
            return
        await asyncio.sleep(0.25)
    raise AssertionError(f"No status update containing {prefix!r}")


def _open_account_tools(calls: list) -> list:
    return [t for c in calls if c["agent"] == "Open Account Agent" for t in c["tool_calls"]]


async def _run_conversation(env: WorkflowEnvironment, run: int) -> dict:
    """Run the conversation once. Returns the result; never raises on a failed check."""
    transcript: list = []
    failures: list = []
    KYC_APPROVED.clear()

    handle = await env.client.start_workflow(
        WealthManagementWorkflow.run,
        id=f"account-opening-live-{run}-{uuid.uuid4()}",
        task_queue=TASK_QUEUE,
    )

    async def say(text: str) -> tuple[str, list]:
        reply, calls = await _send(handle, text)
        transcript.extend([f"Customer: {text}", f"Agent: {reply}", "  trace:", _trace(calls)])
        if CLAIMS_OPEN.search(reply):
            failures.append(f"after {text!r}, the reply said the account is open: {reply!r}")
        if text not in ("123", "list investments") and NEXT_STEPS.search(reply):
            failures.append(f"after {text!r}, the reply ended with the next steps: {reply!r}")
        return reply, calls

    try:
        await say("123")

        reply, calls = await say("Open Vacation with $18,500")
        if "open_new_investment_account" not in _open_account_tools(calls):
            failures.append("the first request did not start an account opening")
        if CLIENT_FIRST_NAME not in reply:
            failures.append("the client details were not shown for the first opening")

        await say("Yes")
        if len(KYC_APPROVED) != 1:
            failures.append(f"KYC approved {len(KYC_APPROVED)} times after the first 'Yes', expected 1")
            return {"run": run, "failures": failures, "transcript": "\n".join(transcript)}

        # An employee approves compliance; the custodian rejects and the saga rolls back.
        await _child(KYC_APPROVED[0]).signal(OpenInvestmentAccountWorkflow.compliance_approved)
        await _wait_for_status(handle, "Rolled back")
        transcript.append("  (compliance approved; the custodian rejected; the opening was rolled back)")

        reply, calls = await say("Open Vacation with $15,000")
        if STALE_STATUS.search(reply):
            failures.append(f"the reply treated the rolled-back opening as pending: {reply!r}")
        if "open_new_investment_account" not in _open_account_tools(calls):
            failures.append("the second request did not start a new account opening")
        if CLIENT_FIRST_NAME not in reply:
            failures.append("the client details were not shown for the second opening")

        await say("Yes")
        # The model picks the account name's case, so compare case-insensitively.
        expected = [f"OpenAccount-{handle.id}-123-Vacation"] * 2
        if [w.lower() for w in KYC_APPROVED] != [w.lower() for w in expected]:
            failures.append(f"KYC approved for {KYC_APPROVED}, expected {expected}")

        reply, _ = await say("list investments")
        for account in CLIENT_123_ACCOUNTS:
            if not listed_on_own_line(account, reply):
                failures.append(f"account {account} is not listed on its own line")
    finally:
        await handle.signal(WealthManagementWorkflow.end_workflow)
        await handle.result()

    text = f"\nRun {run} of {RUNS}:\n" + "\n".join(transcript)
    print(text)
    return {"run": run, "failures": failures, "transcript": text}


async def test_open_account_after_a_rollback(temporal_env):
    results = []
    async with Worker(
        temporal_env.client,
        task_queue=TASK_QUEUE,
        workflows=[WealthManagementWorkflow, OpenInvestmentAccountWorkflow],
        activities=ACTIVITIES,
    ), Worker(temporal_env.client, task_queue=LLM_TASK_QUEUE):  # the model calls run here
        for run in range(1, RUNS + 1):
            results.append(await _run_conversation(temporal_env, run))

    failed = [r for r in results if r["failures"]]
    print(f"\nLive account opening: {RUNS - len(failed)}/{RUNS} runs passed")
    assert not failed, "\n".join(
        f"Run {r['run']} failed: {'; '.join(r['failures'])}{r['transcript']}" for r in failed
    )
