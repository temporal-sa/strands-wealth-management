import asyncio
from datetime import timedelta
from typing import List, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, WorkflowAlreadyStartedError
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
from temporal_supervisor.activities.event_stream_activities import EventStreamActivities
from temporal_supervisor.retry_policies import LLM_RETRY_POLICY, TOOL_RETRY_POLICY

with workflow.unsafe.imports_passed_through():
    from strands import tool
    from strands.hooks import HookProvider, HookRegistry, BeforeToolCallEvent
    from strands.types.tools import ToolContext
    from temporalio.contrib.strands import TemporalAgent
    from temporalio.contrib.strands.workflow import activity_as_tool
    from temporal_supervisor.model_config import LLM_TASK_QUEUE, MODEL_NAME, OPEN_ACCOUNT_TASK_QUEUE
    from temporal_supervisor.activities.beneficiaries import Beneficiaries
    from temporal_supervisor.activities.investments import Investments
    from temporal_supervisor.activities.open_account import OpenAccount
    from temporal_supervisor.activities.approval import ApprovalActivities
    from temporal_supervisor.workflows.open_account_workflow import (
        OpenInvestmentAccountWorkflow,
        OpenInvestmentAccountInput,
    )

ACTIVITY_TIMEOUT = timedelta(seconds=30)
MODEL_TIMEOUT = timedelta(seconds=90)


def _activity_tool(activity_fn):
    """Expose an activity to an agent as a tool, retrying until its system recovers."""
    return activity_as_tool(activity_fn, start_to_close_timeout=ACTIVITY_TIMEOUT, retry_policy=TOOL_RETRY_POLICY)


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
KYC_NOT_APPROVED = (
    "KYC was not approved. First show the customer their current details for this account "
    "opening (get_current_client_info) and ask whether they are correct. Approve KYC only "
    "after the customer confirms in their next message."
)


def kyc_confirmed(shown_turn: Optional[int], turn: int) -> bool:
    """Whether an account opening's details were shown in an earlier customer turn.

    The model sometimes treats a confirmation for an earlier account opening as
    approval for a new one, or approves in the same turn it shows the details.
    KYC may only be approved once this opening's details have been shown and the
    customer has replied since.
    """
    return shown_turn is not None and shown_turn < turn


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
        retry_policy=TOOL_RETRY_POLICY,
    )


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
        # entry mid-turn (while an approval is pending); empty between turns.
        self._subagents: dict = {}
        # Long-lived sub-agents that must own multi-turn state (the open-account
        # agent owns its child-workflow ID), keyed by a stable name rather than by
        # tool_use id so the same agent is reused across supervisor turns.
        self._persistent_subagents: dict = {}
        # Latest state and account name reported by each open-account child
        # workflow (see update_account_opening_state), so the open-account agent
        # gets the real status instead of relying on what it remembers.
        self._account_opening_states: dict = {}
        self._account_opening_names: dict = {}
        # Number of the customer message being processed, and the message in which
        # each account opening's client details were shown, keyed by its workflow
        # ID. Used to check that the customer confirmed their details before KYC is
        # approved (see kyc_confirmed).
        self._turn = 0
        self._kyc_details_shown_turn: dict = {}
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
                sub = TemporalAgent(
                    model=MODEL_NAME,
                    task_queue=LLM_TASK_QUEUE,
                    start_to_close_timeout=MODEL_TIMEOUT,
                    retry_policy=LLM_RETRY_POLICY,
                    system_prompt=system_prompt,
                    tools=tools,
                    hooks=[ApprovalHook()],
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
                    _activity_tool(Beneficiaries.list_beneficiaries),
                    _activity_tool(Beneficiaries.add_beneficiary),
                    _activity_tool(Beneficiaries.delete_beneficiary),
                ],
            )

        return beneficiary_assistant

    def _make_investment_assistant(self):
        @tool(context=True)
        async def investment_assistant(client_id: str, request: str, tool_context: ToolContext) -> str:
            """Handle any investment-account request: list, close, or open an account.

            Listing and closing are handled directly; opening a new account is delegated
            onward to the persistent open-account agent (open_account_assistant), which
            drives the durable KYC + compliance flow.

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
                    _activity_tool(Investments.list_investments),
                    _activity_tool(Investments.close_investment),
                    # Nested delegate: the investment agent owns account-opening and
                    # hands it to the dedicated open-account agent.
                    self._make_open_account_assistant(),
                ],
            )

        return investment_assistant

    def _make_open_account_assistant(self):
        @tool
        async def open_new_investment_account(client_id: str, account_name: str, initial_amount: float) -> str:
            """Begin opening a new investment account via a durable child workflow.

            Args:
                client_id: The customer's client ID.
                account_name: The name for the new investment account.
                initial_amount: The initial deposit amount.

            Returns:
                The child workflow ID, which the other open-account tools require, or
                the reason the account cannot be opened. Starting the opening does not
                open the account: it still needs KYC and a compliance review.
            """
            # Business rules are checked up front. A violation is a non-retryable error:
            # it fails on the first attempt, and the reason goes back to the agent.
            try:
                await workflow.execute_activity(
                    Investments.validate_new_account,
                    args=[client_id, account_name, initial_amount],
                    start_to_close_timeout=ACTIVITY_TIMEOUT,
                    retry_policy=TOOL_RETRY_POLICY,
                )
            except ActivityError as e:
                # Strands only shows the model str(error), which for an ActivityError is
                # "Activity task failed"; pass the business reason through instead.
                if isinstance(e.__cause__, ApplicationError):
                    return f"The account cannot be opened: {e.__cause__.message}"
                raise

            # Read in an activity so the result is recorded in history and replays the same.
            promo_code = await workflow.execute_activity(
                Investments.find_promotion,
                client_id,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=TOOL_RETRY_POLICY,
            )
            account_input = OpenInvestmentAccountInput(
                client_id=client_id,
                account_name=account_name,
                initial_amount=initial_amount,
                promo_code=promo_code,
            )
            # Same format as the other ports. The ID is reused when the customer opens
            # an account with the same name again (e.g. after a rollback); the child
            # puts its run ID in the transfer IDs so the new opening moves money again.
            child_workflow_id = (
                f"OpenAccount-{workflow.info().workflow_id}-{client_id}-{account_name}"
            )
            # Forget what the previous opening with this ID reported, so a reopened
            # account is not shown as rolled back or KYC-confirmed. Put it back if
            # that opening is still running.
            previous = (
                self._account_opening_states.pop(child_workflow_id, None),
                self._account_opening_names.pop(child_workflow_id, None),
                self._kyc_details_shown_turn.pop(child_workflow_id, None),
            )
            # TERMINATE matches the other ports: ending the conversation stops its pending
            # account openings.
            try:
                await workflow.start_child_workflow(
                    OpenInvestmentAccountWorkflow.run,
                    account_input,
                    id=child_workflow_id,
                    parent_close_policy=ParentClosePolicy.TERMINATE,
                    task_queue=OPEN_ACCOUNT_TASK_QUEUE,
                )
            except WorkflowAlreadyStartedError:
                # Like the reference, starting a second opening with the same name
                # fails while the first is running, and the error goes back to the agent.
                for store, value in zip(
                    (self._account_opening_states, self._account_opening_names, self._kyc_details_shown_turn),
                    previous,
                ):
                    if value is not None:
                        store.setdefault(child_workflow_id, value)
                return (
                    f"The account cannot be opened: an opening of an account named {account_name!r} "
                    f"is already in progress (workflow ID {child_workflow_id}). Continue that one, "
                    f"or choose a different account name."
                )
            # A bare ID reads like success, and the model told customers their account
            # was open. Say plainly that it is not.
            return (
                f"Account opening started. The account is NOT open yet: the customer must confirm "
                f"their details (KYC), then it needs a compliance review. Workflow ID: {child_workflow_id}"
            )

        @tool
        async def get_account_opening_status(workflow_id: str) -> str:
            """Get the current status of an account opening.

            Args:
                workflow_id: The open-account child workflow ID returned by open_new_investment_account.

            Returns:
                The latest status the account opening reported. "Complete" means the account
                was opened; a status starting with "Rolled back" means it was not opened.
                Either way, that account opening is finished.
            """
            state = self._account_opening_states.get(workflow_id)
            if state is None:
                return f"No account opening with workflow ID {workflow_id} was found in this conversation."
            return state

        @tool
        async def get_current_client_info(workflow_id: str) -> dict:
            """Retrieve the customer's current details for an account opening, to show them for KYC.

            Args:
                workflow_id: The open-account child workflow ID returned by open_new_investment_account.
            """
            client = await workflow.execute_activity(
                OpenAccount.get_current_client_info,
                workflow_id,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=TOOL_RETRY_POLICY,
            )
            self._kyc_details_shown_turn[workflow_id] = self._turn
            return client

        @tool
        async def approve_kyc(workflow_id: str) -> str:
            """Approve KYC for an account opening, after the customer confirmed their details are correct.

            Args:
                workflow_id: The open-account child workflow ID returned by open_new_investment_account.
            """
            if not kyc_confirmed(self._kyc_details_shown_turn.get(workflow_id), self._turn):
                return KYC_NOT_APPROVED
            await workflow.execute_activity(
                OpenAccount.approve_kyc,
                workflow_id,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=TOOL_RETRY_POLICY,
            )
            return "KYC approved. The account is now waiting for compliance review."

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
            # The agent is persistent, so its history holds whatever status it last
            # saw. Give it the current status on every turn so a finished opening
            # (for example, one that was rolled back) is never reported as pending.
            return await self._run_subagent(
                tool_context=tool_context,
                client_id=client_id,
                request=f"{self._account_openings_summary()}{request}",
                system_prompt=OPEN_ACCOUNT_INSTRUCTIONS,
                tools=[
                    open_new_investment_account,
                    get_account_opening_status,
                    get_current_client_info,
                    approve_kyc,
                    update_client_details,
                ],
                persistent_key="open_account",
            )

        return open_account_assistant

    def _account_openings_summary(self) -> str:
        """The current status of each account opening in this conversation, as a
        sentence to put before the customer's request (empty when there are none)."""
        if not self._account_opening_states:
            return ""
        openings = "; ".join(
            f'{self._account_opening_names[workflow_id]} (workflow ID {workflow_id}): "{state}"'
            for workflow_id, state in self._account_opening_states.items()
        )
        return f"Current status of account openings in this conversation: {openings}. "

    @workflow.run
    async def run(self) -> None:
        self.wf_id = workflow.info().workflow_id

        await workflow.execute_local_activity(
            EventStreamActivities.delete_conversation,
            args=[self.wf_id],
            start_to_close_timeout=self.local_to_close_timeout,
            retry_policy=self.retry_policy,
        )

        # The supervisor is a pure orchestrator: it delegates beneficiary and
        # investment requests to specialized sub-agents (see _make_*_assistant).
        # Opening an account is nested one level deeper — the investment agent
        # owns it and delegates onward to the persistent open-account agent, which
        # drives the durable child workflow. The approval gate lives on the sub-agents.
        self._agent = TemporalAgent(
            model=MODEL_NAME,
            task_queue=LLM_TASK_QUEUE,
            start_to_close_timeout=MODEL_TIMEOUT,
            retry_policy=LLM_RETRY_POLICY,
            system_prompt=TEMPORAL_SUPERVISOR_INSTRUCTIONS,
            tools=[
                self._make_beneficiary_assistant(),
                self._make_investment_assistant(),
            ],
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

            # TODO: Continue-As-New.
            # This demo keeps the whole conversation in one workflow run. A production system
            # would call continue-as-new when workflow.info().is_continue_as_new_suggested()
            # returns true, because Temporal limits a run's event history (warnings at 10K
            # events / 10 MB, terminated at 50K events / 50 MB) and every LLM call and tool
            # call adds events, so a long-running chat would eventually hit those limits.
            # To do it safely:
            #   - Wait for workflow.all_handlers_finished() and drain the pending message and
            #     status queues first, so no customer message or status update is lost.
            #   - Carry the conversation state into the new run: the message history, the
            #     active agent, the client ID, the account-opening states, names, and
            #     count, and the KYC guard's turn tracking.
            #   - Open-account child workflows are started with ParentClosePolicy.TERMINATE,
            #     which also applies when the parent continues-as-new, so an opening waiting
            #     for KYC or compliance would be terminated. Use ParentClosePolicy.ABANDON and
            #     have the end_workflow handler cancel/terminate any openings still running,
            #     or defer continue-as-new until no opening is in progress.

    async def _process_chat_message(self, message: str) -> None:
        self._turn += 1
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
        self._account_opening_states[state_input.workflow_id] = state_input.state
        self._account_opening_names[state_input.workflow_id] = state_input.account_name
        # Surface a simple account-opening status. Compliance is approved out-of-band
        # by an employee via the run_send_compliance_approval CLI (using the child
        # workflow ID from the Temporal UI).
        await self.pending_status_updates.put(
            f"New {state_input.account_name} account status changed: {state_input.state}"
        )
