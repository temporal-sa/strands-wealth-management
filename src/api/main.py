import os
import sys
from contextlib import asynccontextmanager
from typing import Optional, AsyncGenerator

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import TemporalError
from temporalio.service import RPCError

from common.event_stream_manager import EventStreamManager
from common.user_message import ProcessUserMessageInput
from temporal_supervisor.client_helper import connect_client
from temporal_supervisor.model_config import TASK_QUEUE
from temporal_supervisor.workflows.supervisor_workflow import WealthManagementWorkflow

load_dotenv()

temporal_client: Optional[Client] = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    print("API starting up...")
    global temporal_client
    temporal_client = await connect_client()
    yield
    print("API shutting down...")


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"message": "Strands Agents + Temporal Wealth Management API"}


@app.post("/start-workflow")
async def start_workflow(workflow_id: str):
    try:
        await temporal_client.start_workflow(
            WealthManagementWorkflow.run,
            args=[],
            id=workflow_id,
            task_queue=TASK_QUEUE,
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        )
        return {"message": "Workflow started."}
    except Exception as e:
        return {"message": f"An error occurred starting the workflow: {e}"}


@app.post("/send-prompt")
async def send_prompt(workflow_id: str, prompt: str):
    message = ProcessUserMessageInput(user_input=prompt)
    try:
        handle = temporal_client.get_workflow_handle(workflow_id=workflow_id)
        await handle.signal(WealthManagementWorkflow.process_user_message, args=[message])
        return {"response": "Message sent"}
    except RPCError as e:
        return {"response": f"Error: {e}"}


@app.get("/get-chat-history")
async def get_chat_history(workflow_id: str, from_index: int = Query(0)):
    try:
        manager = EventStreamManager()
        try:
            return await manager.get_events_from_index(workflow_id=workflow_id, from_index=from_index)
        finally:
            await manager.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@app.get("/pending-approval")
async def pending_approval(workflow_id: str):
    """Returns the reason text if the agent is paused awaiting delete/close approval."""
    try:
        handle = temporal_client.get_workflow_handle(workflow_id=workflow_id)
        reason = await handle.query(WealthManagementWorkflow.get_pending_approval)
        return {"pending": reason}
    except TemporalError:
        return {"pending": None}


@app.post("/approve")
async def approve(workflow_id: str, response: str):
    """Answer a pending delete/close approval. response is 'approve' or 'deny'."""
    try:
        handle = temporal_client.get_workflow_handle(workflow_id=workflow_id)
        await handle.signal(WealthManagementWorkflow.approve, response)
        return {"message": f"Approval response '{response}' sent."}
    except TemporalError as e:
        return {"message": f"Error: {e}"}


@app.post("/end-chat")
async def end_chat(workflow_id: str):
    try:
        handle = temporal_client.get_workflow_handle(workflow_id=workflow_id)
        await handle.signal(WealthManagementWorkflow.end_workflow)
        return {"message": "End chat signal sent."}
    except TemporalError:
        return {}
