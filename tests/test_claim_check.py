"""Tests for the optional claim-check codec, its plugin, the codec server, and the
open-account task-queue split.

Redis is replaced with an in-memory stand-in, so no Redis is needed. The workflow
test starts the local Temporal test server (downloaded automatically on first run).
"""

import base64
import json
import uuid
from typing import Any

import pytest
from aiohttp.test_utils import TestClient, TestServer
from temporalio import activity
from temporalio.api.common.v1 import Payload
from temporalio.contrib.pydantic import PydanticPayloadConverter
from temporalio.contrib.strands import StrandsPlugin
from temporalio.contrib.strands._failure_converter import StrandsFailureConverter
from temporalio.converter import DataConverter
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from temporal_supervisor import client_helper
from temporal_supervisor.claim_check.claim_check_codec import ClaimCheckCodec
from temporal_supervisor.claim_check.claim_check_plugin import ClaimCheckPlugin
from temporal_supervisor.codec_server.codec_server import build_codec_server
from temporal_supervisor.run_worker import choose_workflows
from temporal_supervisor.workflows.open_account_workflow import (
    OpenInvestmentAccountInput,
    OpenInvestmentAccountWorkflow,
)
from temporal_supervisor.workflows.supervisor_workflow import WealthManagementWorkflow


class FakeRedis:
    """In-memory stand-in for redis.asyncio.Redis (only get and set)."""

    def __init__(self):
        self.data: dict = {}

    async def set(self, key: str, value: bytes) -> None:
        self.data[key] = value

    async def get(self, key: str):
        return self.data.get(key)


def _json_payload(value: Any) -> Payload:
    return Payload(metadata={"encoding": b"json/plain"}, data=json.dumps(value).encode())


@pytest.fixture
def redis() -> FakeRedis:
    return FakeRedis()


@pytest.fixture
def codec(redis) -> ClaimCheckCodec:
    return ClaimCheckCodec(redis_client=redis)


async def test_codec_stores_payloads_and_sends_only_their_id(codec, redis):
    original = _json_payload({"client_id": "123"})
    [encoded] = await codec.encode([original])

    # The same metadata as the Java codec (ClaimCheckCodec.java).
    assert encoded.metadata["encoding"] == b"claim-checked"
    assert encoded.metadata["temporal.io/claim-check-codec"] == b"v1"
    claim_id = encoded.data.decode()
    uuid.UUID(claim_id)
    # The serialized Payload protobuf, which the Java codec reads back with Payload.parseFrom.
    assert redis.data[claim_id] == original.SerializeToString()

    assert await codec.decode([encoded]) == [original]


async def test_codec_passes_through_payloads_that_were_not_claim_checked(codec):
    plain = _json_payload("hello")
    assert await codec.decode([plain]) == [plain]


async def test_codec_fails_clearly_when_the_payload_is_missing(codec, redis):
    [encoded] = await codec.encode([_json_payload(1)])
    redis.data.clear()
    with pytest.raises(ValueError, match="not found in Redis"):
        await codec.decode([encoded])


def test_clients_get_the_plugin_only_when_claim_check_is_on(monkeypatch):
    monkeypatch.delenv("USE_CLAIM_CHECK", raising=False)
    assert [type(p) for p in client_helper.client_plugins()] == [StrandsPlugin]

    monkeypatch.setenv("USE_CLAIM_CHECK", "false")
    assert [type(p) for p in client_helper.client_plugins()] == [StrandsPlugin]

    monkeypatch.setenv("USE_CLAIM_CHECK", "true")
    # After the StrandsPlugin, so the codec is added to its data converter.
    assert [type(p) for p in client_helper.client_plugins()] == [StrandsPlugin, ClaimCheckPlugin]


def test_plugin_adds_the_codec_to_the_strands_data_converter(codec):
    config: dict = {"data_converter": DataConverter.default}
    for plugin in [StrandsPlugin(models={}), ClaimCheckPlugin(codec)]:
        config = plugin.configure_client(config)

    converter = config["data_converter"]
    assert converter.payload_codec is codec
    # The StrandsPlugin's pydantic payload converter and failure converter are kept.
    assert converter.payload_converter_class is PydanticPayloadConverter
    assert converter.failure_converter_class is StrandsFailureConverter


# --- The open-account workflow with the codec on: payloads in history are claim-check IDs ---

TASK_QUEUE = "test-claim-check"


@activity.defn(name="get_client")
async def stub_get_client(client_id: str) -> dict:
    return {
        "client_id": client_id, "first_name": "Sam", "last_name": "Doe", "address": "1 Main St",
        "phone": "555", "email": "s@example.com", "marital_status": "single",
    }


@activity.defn(name="open_investment")
async def stub_open_investment(new_account: Any) -> dict:
    return {"investment_id": "i-test", "name": "Test", "balance": 500.0}


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
    raise ApplicationError("The custodian rejected the account registration", non_retryable=True)


async def test_workflow_payloads_are_claim_checked_end_to_end(codec, redis):
    async with await WorkflowEnvironment.start_local(
        plugins=[StrandsPlugin(models={}), ClaimCheckPlugin(codec)]
    ) as env:
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[OpenInvestmentAccountWorkflow],
            activities=[
                stub_get_client, stub_open_investment, stub_remove_investment,
                stub_debit, stub_credit, stub_register,
            ],
        ):
            handle = await env.client.start_workflow(
                OpenInvestmentAccountWorkflow.run,
                OpenInvestmentAccountInput(client_id="C-1", account_name="Test", initial_amount=500.0),
                id=f"claim-check-test-{uuid.uuid4()}",
                task_queue=TASK_QUEUE,
            )
            await handle.signal(OpenInvestmentAccountWorkflow.verify_kyc)
            await handle.signal(OpenInvestmentAccountWorkflow.compliance_approved)
            result = await handle.result()

        # Decoded through Redis, and the activity's failure message still reaches the saga.
        assert not result.account_created
        assert result.message == "The account was not opened: The custodian rejected the account registration"

        history = await handle.fetch_history()
        started = history.events[0].workflow_execution_started_event_attributes
        [input_payload] = started.input.payloads
        assert input_payload.metadata["encoding"] == b"claim-checked"
        stored = Payload.FromString(redis.data[input_payload.data.decode()])
        assert json.loads(stored.data)["account_name"] == "Test"


# --- Codec server ---

async def test_codec_server_decodes_for_the_temporal_ui(codec):
    [encoded] = await codec.encode([_json_payload({"state": "Complete"})])
    body = {"payloads": [{
        "metadata": {k: base64.b64encode(v).decode() for k, v in encoded.metadata.items()},
        "data": base64.b64encode(encoded.data).decode(),
    }]}
    async with TestClient(TestServer(build_codec_server(codec))) as client:
        resp = await client.post("/decode", json=body, headers={"Origin": "http://localhost:8233"})
        assert resp.status == 200
        assert resp.headers["Access-Control-Allow-Origin"] == "http://localhost:8233"
        decoded = (await resp.json())["payloads"][0]
        assert json.loads(base64.b64decode(decoded["data"])) == {"state": "Complete"}

        # Only the local UI at exactly http://localhost:8233 (and Temporal Cloud) may call it.
        resp = await client.options("/decode", headers={"Origin": "http://127.0.0.1:8233"})
        assert "Access-Control-Allow-Origin" not in resp.headers


# --- Task-queue split for the Java open-account worker ---

def test_worker_registers_the_open_account_workflow_only_on_a_shared_queue():
    assert choose_workflows("main", "main") == [WealthManagementWorkflow, OpenInvestmentAccountWorkflow]
    # Split queues: the Java worker runs the open-account workflow.
    assert choose_workflows("main", "Open-Account-Java") == [WealthManagementWorkflow]
