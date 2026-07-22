import asyncio
from dataclasses import dataclass, field
from datetime import timedelta
from typing import List, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.workflow import ParentClosePolicy

from common.user_message import ProcessUserMessageInput, ChatInteraction
from common.status_update import StatusUpdate
from common.account_context import UpdateAccountOpeningStateInput
from common.agent_constants import (
    BENE_INSTRUCTIONS,
    OPEN_ACCOUNT_INSTRUCTIONS,
    TEMPORAL_INVEST_SUBAGENT_INSTRUCTIONS,
    TEMPORAL_SUPERVISOR_INSTRUCTIONS,
)
from temporal_supervisor.model_config import MODEL_NAME
from temporal_supervisor.activities.event_stream_activities import EventStreamActivities

with workflow.unsafe.imports_passed_through():
    from strands import tool
    from strands.hooks import HookProvider, HookRegistry, BeforeToolCallEvent
    from strands.types.tools import ToolContext
    from temporalio.contrib.strands import TemporalAgent
    from temporalio.contrib.strands.workflow import activity_as_tool
    from temporal_supervisor.activities.beneficiaries import Beneficiaries
    from temporal_supervisor.activities.investments import Investments
    from temporal_supervisor.activities.open_account import OpenAccount
    from temporal_supervisor.activities.approval import ApprovalActivities
    from temporal_supervisor.workflows.open_account_workflow import (
        OpenInvestmentAccountWorkflow,
        OpenInvestmentAccountInput,
    )

ACTIVITY_TIMEOUT = timedelta(seconds=30)

# Tools that mutate state and therefore require human approval before running.
APPROVAL_REQUIRED_TOOLS = {"delete_beneficiary", "close_investment"}


# ---------------------------------------------------------------------------
# Human-approval gate: pause the agent before a destructive tool call. Registered
# on the specialized sub-agents (which own delete_beneficiary / close_investment),
# so the interrupt is raised inside a sub-agent and re-propagated to the
# supervisor by _run_subagent below. The hook runs in workflow context, so it must
# be deterministic (no I/O).
# ---------------------------------------------------------------------------
class ApprovalHook(HookProvider):
    def register_hooks(self, registry: HookRegistry, **kwargs: object) -> None:
        registry.add_callback(BeforeToolCallEvent, self._gate)

    def _gate(self, event: BeforeToolCallEvent) -> None:
        name = event.tool_use["name"]
        if name not in APPROVAL_REQUIRED_TOOLS:
            return
        # Structured reason (no I/O here — this runs in workflow context). The
        # friendly, name-resolved prompt is produced later by an activity; see
        # WealthManagementWorkflow._describe_approval.
        approval = event.interrupt(
            "approval",
            reason={"tool": name, "input": dict(event.tool_use["input"])},
        )
        if approval != "approve":
            event.cancel_tool = "The customer denied the request, so it was not performed."


# ---------------------------------------------------------------------------
# In-workflow tools (run deterministically in the workflow sandbox).
# ---------------------------------------------------------------------------
@tool
async def open_new_investment_account(client_id: str, account_name: str, initial_amount: float) -> str:
    """Begin opening a new investment account via a durable child workflow.

    Args:
        client_id: The customer's client ID.
        account_name: The name for the new investment account.
        initial_amount: The initial deposit amount.

    Returns:
        The child workflow ID, which the other open-account tools require.
    """
    account_input = OpenInvestmentAccountInput(
        client_id=client_id, account_name=account_name, initial_amount=initial_amount
    )
    child_workflow_id = f"open-account-{workflow.info().workflow_id}-{account_name}-{workflow.uuid4()}"
    await workflow.start_child_workflow(
        OpenInvestmentAccountWorkflow.run,
        account_input,
        id=child_workflow_id,
        parent_close_policy=ParentClosePolicy.ABANDON,
    )
    return child_workflow_id


@tool
async def update_client_details(
    workflow_id: str,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
    address: Optional[str] = None,
    phone: Optional[str] = None,
    email: Optional[str] = None,
    marital_status: Optional[str] = None,
) -> str:
    """Update one or more client fields during account opening (also completes KYC).

    Args:
        workflow_id: The open-account child workflow ID from open_new_investment_account.
        first_name: New first name (omit to leave unchanged).
        last_name: New last name (omit to leave unchanged).
        address: New address (omit to leave unchanged).
        phone: New phone number (omit to leave unchanged).
        email: New email address (omit to leave unchanged).
        marital_status: New marital status (omit to leave unchanged).
    """
    fields = {
        k: v
        for k, v in {
            "first_name": first_name,
            "last_name": last_name,
            "address": address,
            "phone": phone,
            "email": email,
            "marital_status": marital_status,
        }.items()
        if v is not None
    }
    return await workflow.execute_activity(
        OpenAccount.update_client_details,
        args=[workflow_id, fields],
        start_to_close_timeout=ACTIVITY_TIMEOUT,
    )


@dataclass
class ChatInput:
    """Carried across continue-as-new so the conversation survives history resets."""
    messages: list = field(default_factory=list)
    history: List[ChatInteraction] = field(default_factory=list)
    # The persistent open-account agent's own message history (see
    # _make_open_account_assistant); carried so a multi-turn account-opening flow
    # survives a continue-as-new mid-flight.
    open_account_messages: list = field(default_factory=list)


@workflow.defn
class WealthManagementWorkflow:
    def __init__(self):
        self.wf_id: Optional[str] = None
        self.pending_chat_messages: asyncio.Queue = asyncio.Queue()
        self.pending_status_updates: asyncio.Queue = asyncio.Queue()
        self.chat_history: List[ChatInteraction] = []
        self.status_updates: List[str] = []
        self.end_workflow_flag = False
        self._pending_approval: Optional[str] = None
        self._approval_response: Optional[str] = None
        self._agent: Optional[TemporalAgent] = None
        # Sub-agents persisted across an interrupt/resume, keyed by the supervisor
        # tool_use id that spawned them. See _run_subagent. Only ever holds an
        # entry mid-turn (while an approval is pending); empty between turns, so it
        # does not need to be carried across continue-as-new.
        self._subagents: dict = {}
        # Long-lived sub-agents that must own multi-turn state (the open-account
        # agent owns its child-workflow ID), keyed by a stable name rather than by
        # tool_use id so the same agent is reused across supervisor turns. Their
        # message history IS carried across continue-as-new; see run().
        self._persistent_subagents: dict = {}
        self._persistent_seed_messages: dict = {}
        self.local_to_close_timeout = timedelta(seconds=10)
        self.retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=1),
            backoff_coefficient=2,
            maximum_interval=timedelta(seconds=30),
        )

    # ----- Specialized sub-agents (the "agents as tools" pattern) -----
    async def _run_subagent(
        self,
        *,
        tool_context: "ToolContext",
        client_id: str,
        request: str,
        system_prompt: str,
        tools: list,
        persistent_key: Optional[str] = None,
    ) -> str:
        """Build/reuse a specialized sub-agent and run it, propagating approvals.

        A sub-agent gates its destructive tool with ApprovalHook, which raises an
        interrupt from *inside* the sub-agent. That interrupt does not cross the
        tool boundary on its own — the sub-agent's invoke returns normally with
        stop_reason == "interrupt". So we re-raise it on the supervisor via
        ``tool_context.interrupt`` (which surfaces to the workflow's approval loop)
        and, on resume, feed the human's decision back down to the *same*
        sub-agent. The in-flight sub-agent is persisted in ``self._subagents``
        across the raise/resume; the supervisor's tool_use id is a stable,
        deterministic key.

        When ``persistent_key`` is given, the sub-agent is also long-lived: it is
        built once and reused across supervisor turns (from
        ``self._persistent_subagents``) so it can own multi-turn state such as the
        open-account child-workflow ID. Ordinary sub-agents are rebuilt per call.
        """
        key = tool_context.tool_use["toolUseId"]
        if key not in self._subagents:
            if persistent_key is not None and persistent_key in self._persistent_subagents:
                # A later turn of a multi-turn flow: reuse the same agent so its
                # message history (and the child-workflow ID it holds) carries over.
                sub = self._persistent_subagents[persistent_key]
            else:
                seed = self._persistent_seed_messages.pop(persistent_key, []) if persistent_key else []
                sub = TemporalAgent(
                    model=MODEL_NAME,
                    start_to_close_timeout=timedelta(seconds=90),
                    retry_policy=RetryPolicy(maximum_attempts=3),
                    system_prompt=system_prompt,
                    tools=tools,
                    hooks=[ApprovalHook()],
                    messages=seed,
                )
                if persistent_key is not None:
                    self._persistent_subagents[persistent_key] = sub
            sub_result = await sub.invoke_async(f"The client ID is {client_id}. {request}")
            self._subagents[key] = [sub, sub_result]
        else:
            sub, sub_result = self._subagents[key]

        while sub_result.stop_reason == "interrupt":
            responses = []
            for interrupt in sub_result.interrupts or []:
                # Raises on the first pass (propagates to the supervisor -> workflow
                # approval loop); returns the human's response on resume. A distinct
                # name per sub-interrupt keeps multiple gated calls in one turn from
                # colliding on the supervisor's interrupt state.
                answer = tool_context.interrupt(f"subagent:{interrupt.id}", reason=interrupt.reason)
                responses.append(
                    {"interruptResponse": {"interruptId": interrupt.id, "response": answer}}
                )
            sub_result = await sub.invoke_async(responses)
            self._subagents[key][1] = sub_result

        self._subagents.pop(key, None)
        return str(sub_result)

    def _make_beneficiary_assistant(self):
        @tool(context=True)
        async def beneficiary_assistant(client_id: str, request: str, tool_context: ToolContext) -> str:
            """Handle a beneficiary-related request (list, add, or delete beneficiaries).

            Args:
                client_id: The customer's client ID.
                request: A natural-language description of what the customer wants done with their beneficiaries.

            Returns:
                The beneficiary agent's response.
            """
            return await self._run_subagent(
                tool_context=tool_context,
                client_id=client_id,
                request=request,
                system_prompt=BENE_INSTRUCTIONS,
                tools=[
                    activity_as_tool(Beneficiaries.list_beneficiaries, start_to_close_timeout=ACTIVITY_TIMEOUT),
                    activity_as_tool(Beneficiaries.add_beneficiary, start_to_close_timeout=ACTIVITY_TIMEOUT),
                    activity_as_tool(Beneficiaries.delete_beneficiary, start_to_close_timeout=ACTIVITY_TIMEOUT),
                ],
            )

        return beneficiary_assistant

    def _make_investment_assistant(self):
        @tool(context=True)
        async def investment_assistant(client_id: str, request: str, tool_context: ToolContext) -> str:
            """Handle an investment-account request to list or close investment accounts.

            (Opening a new account is handled by the supervisor, not this tool.)

            Args:
                client_id: The customer's client ID.
                request: A natural-language description of what the customer wants done with their investment accounts.

            Returns:
                The investment agent's response.
            """
            return await self._run_subagent(
                tool_context=tool_context,
                client_id=client_id,
                request=request,
                system_prompt=TEMPORAL_INVEST_SUBAGENT_INSTRUCTIONS,
                tools=[
                    activity_as_tool(Investments.list_investments, start_to_close_timeout=ACTIVITY_TIMEOUT),
                    activity_as_tool(Investments.close_investment, start_to_close_timeout=ACTIVITY_TIMEOUT),
                ],
            )

        return investment_assistant

    def _make_open_account_assistant(self):
        @tool(context=True)
        async def open_account_assistant(client_id: str, request: str, tool_context: ToolContext) -> str:
            """Open a new investment account (a multi-step, durable KYC + compliance flow).

            This delegates to a persistent open-account agent that drives the durable
            child workflow across several customer turns, so relay whatever the customer
            said (the account name and initial amount, a confirmation that their details
            are correct, or which details to change) and relay the agent's reply back.

            Args:
                client_id: The customer's client ID.
                request: A natural-language description of what the customer wants done
                    with respect to opening the account.

            Returns:
                The open-account agent's response.
            """
            return await self._run_subagent(
                tool_context=tool_context,
                client_id=client_id,
                request=request,
                system_prompt=OPEN_ACCOUNT_INSTRUCTIONS,
                tools=[
                    open_new_investment_account,
                    activity_as_tool(OpenAccount.get_current_client_info, start_to_close_timeout=ACTIVITY_TIMEOUT),
                    activity_as_tool(OpenAccount.approve_kyc, start_to_close_timeout=ACTIVITY_TIMEOUT),
                    update_client_details,
                ],
                persistent_key="open_account",
            )

        return open_account_assistant

    @workflow.run
    async def run(self, chat_input: ChatInput = ChatInput()) -> None:
        self.wf_id = workflow.info().workflow_id
        self.chat_history = list(chat_input.history)
        if chat_input.open_account_messages:
            self._persistent_seed_messages["open_account"] = list(chat_input.open_account_messages)

        if workflow.info().continued_run_id is None:
            await workflow.execute_local_activity(
                EventStreamActivities.delete_conversation,
                args=[self.wf_id],
                start_to_close_timeout=self.local_to_close_timeout,
                retry_policy=self.retry_policy,
            )

        # The supervisor is a pure orchestrator: it delegates beneficiary,
        # investment, and open-account requests to specialized sub-agents (see
        # _make_*_assistant). The open-account agent is persistent and drives the
        # durable child workflow itself. The approval gate lives on the sub-agents.
        self._agent = TemporalAgent(
            model=MODEL_NAME,
            start_to_close_timeout=timedelta(seconds=90),
            retry_policy=RetryPolicy(maximum_attempts=3),
            system_prompt=TEMPORAL_SUPERVISOR_INSTRUCTIONS,
            tools=[
                self._make_beneficiary_assistant(),
                self._make_investment_assistant(),
                self._make_open_account_assistant(),
            ],
            messages=list(chat_input.messages),
        )

        while True:
            await workflow.wait_condition(
                lambda: (
                    not self.pending_chat_messages.empty()
                    or not self.pending_status_updates.empty()
                    or self.end_workflow_flag
                )
            )

            if self.end_workflow_flag:
                return

            while not self.pending_status_updates.empty():
                await self._process_status_update(self.pending_status_updates.get_nowait())

            if not self.pending_chat_messages.empty():
                await self._process_chat_message(self.pending_chat_messages.get_nowait())

            if workflow.info().is_continue_as_new_suggested():
                await workflow.wait_condition(workflow.all_handlers_finished)
                open_account_agent = self._persistent_subagents.get("open_account")
                workflow.continue_as_new(
                    ChatInput(
                        messages=self._agent.messages,
                        history=self.chat_history,
                        open_account_messages=(
                            open_account_agent.messages if open_account_agent else []
                        ),
                    )
                )

    async def _process_chat_message(self, message: str) -> None:
        chat = ChatInteraction(user_prompt=message, text_response="")

        result = await self._agent.invoke_async(message)
        while result.stop_reason == "interrupt":
            interrupts = list(result.interrupts or [])
            raw_reason = interrupts[0].reason if interrupts else None
            # The confirmation is surfaced inline in the chat via the
            # get_pending_approval query; it is intentionally not pushed to the
            # status stream so it does not also appear in the top status bar.
            self._pending_approval = await self._describe_approval(raw_reason)
            await workflow.wait_condition(lambda: self._approval_response is not None)
            response = self._approval_response
            self._approval_response = None
            self._pending_approval = None
            responses = [
                {"interruptResponse": {"interruptId": i.id, "response": response}} for i in interrupts
            ]
            result = await self._agent.invoke_async(responses)

        chat.text_response = str(result).strip()
        self.chat_history.append(chat)

        await workflow.execute_local_activity(
            EventStreamActivities.append_chat_interaction,
            args=[self.wf_id, chat],
            start_to_close_timeout=self.local_to_close_timeout,
            retry_policy=self.retry_policy,
        )

    async def _describe_approval(self, reason) -> str:
        """Turn an approval interrupt's structured reason into a friendly prompt.

        The reason is ``{"tool": ..., "input": ...}`` set by ApprovalHook. The
        name lookup reads the data files, so it runs as an activity rather than in
        workflow context.
        """
        if isinstance(reason, dict) and "tool" in reason:
            return await workflow.execute_activity(
                ApprovalActivities.describe_approval,
                args=[reason["tool"], reason.get("input", {})],
                start_to_close_timeout=ACTIVITY_TIMEOUT,
            )
        return str(reason) if reason is not None else "Approve this action?"

    async def _process_status_update(self, status_message: str) -> None:
        await self._append_status(status_message)

    async def _append_status(self, status_message: str) -> None:
        self.status_updates.append(status_message)
        await workflow.execute_local_activity(
            EventStreamActivities.append_status_update,
            args=[self.wf_id, StatusUpdate(status=status_message)],
            start_to_close_timeout=self.local_to_close_timeout,
            retry_policy=self.retry_policy,
        )

    # ----- Queries -----
    @workflow.query
    def get_chat_history(self) -> List[ChatInteraction]:
        return self.chat_history

    @workflow.query
    def get_status_updates(self) -> List[str]:
        return self.status_updates

    @workflow.query
    def get_pending_approval(self) -> Optional[str]:
        return self._pending_approval

    # ----- Signals -----
    @workflow.signal
    async def process_user_message(self, message_input: ProcessUserMessageInput) -> None:
        await self.pending_chat_messages.put(message_input.user_input)

    @workflow.signal
    def approve(self, response: str) -> None:
        self._approval_response = response

    @workflow.signal
    def end_workflow(self) -> None:
        self.end_workflow_flag = True

    @workflow.signal
    async def update_account_opening_state(self, state_input: UpdateAccountOpeningStateInput) -> None:
        # Surface a simple account-opening status. Compliance is approved out-of-band
        # by an employee via the run_send_compliance_approval CLI (using the child
        # workflow ID from the Temporal UI).
        await self.pending_status_updates.put(
            f"New '{state_input.account_name}' account status: {state_input.state}"
        )
