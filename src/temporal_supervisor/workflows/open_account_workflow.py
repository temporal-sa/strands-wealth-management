from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from common.investment_manager import InvestmentAccount
    from common.account_context import UpdateAccountOpeningStateInput
    from temporal_supervisor.activities.clients import ClientActivities, WealthManagementClient
    from temporal_supervisor.activities.custodian import Custodian
    from temporal_supervisor.activities.funding import Funding
    from temporal_supervisor.activities.investments import Investments
    from temporal_supervisor.retry_policies import TOOL_RETRY_POLICY


@dataclass
class OpenInvestmentAccountInput:
    client_id: str
    account_name: str
    initial_amount: float
    promo_code: Optional[str] = None


@dataclass
class OpenInvestmentAccountOutput:
    account_created: bool = False
    message: Optional[str] = None


@workflow.defn
class OpenInvestmentAccountWorkflow:
    """Opens an investment account, gating on KYC and a human compliance approval.

    Demonstrates a child workflow with:
      - an Update to fetch/update client details (KYC),
      - Signals for KYC verification and compliance approval,
      - a saga that creates, funds, and registers the account, undoing the
        completed steps if a later one fails,
      - state pushed back to the parent supervisor workflow as it progresses.
    """

    def __init__(self):
        self.client: Optional[WealthManagementClient] = None
        self.inputs: Optional[OpenInvestmentAccountInput] = None
        self.initialized = False
        self.kyc_verified = False
        self.compliance_reviewed = False
        self.current_state = "Initializing"
        # Bounds a single attempt only, so an outage is retried for as long as it lasts.
        self.start_to_close_timeout = timedelta(seconds=30)

    @workflow.run
    async def run(self, inputs: OpenInvestmentAccountInput) -> OpenInvestmentAccountOutput:
        workflow.logger.info(f"OpenInvestmentAccountWorkflow started: {inputs}")
        self.inputs = inputs

        self.client = await self._execute(ClientActivities.get_client, inputs.client_id)
        self.initialized = True

        await self._set_state("Waiting KYC")
        await workflow.wait_condition(lambda: self.kyc_verified)

        await self._set_state("Waiting Compliance Review")
        await workflow.wait_condition(lambda: self.compliance_reviewed)

        await self._set_state("Compliance review approved. Opening the account.")
        return await self._open_account(inputs)

    async def _open_account(self, inputs: OpenInvestmentAccountInput) -> OpenInvestmentAccountOutput:
        """Saga: create, fund, and register the account.

        Each completed step records how to undo it. If a later step fails with a
        non-retryable error, the recorded compensations run in reverse order.
        Transient failures never get here: activities retry until they succeed.
        """
        amount = inputs.initial_amount
        bonus = self._welcome_bonus(inputs) if inputs.promo_code else 0.0
        transfer_id = f"{workflow.info().workflow_id}-initial-deposit"
        compensations = []  # (description, activity, args)
        try:
            await self._set_state("Creating the account record.")
            account = await self._execute(
                Investments.open_investment,
                InvestmentAccount(inputs.client_id, inputs.account_name, amount + bonus),
            )
            investment_id = account["investment_id"]
            compensations.append(
                ("removing the account record", Investments.remove_investment, [inputs.client_id, investment_id])
            )

            await self._set_state(f"Transferring ${amount:,.2f} from the linked bank account.")
            await self._execute(Funding.debit_linked_account, inputs.client_id, amount, transfer_id)
            compensations.append(
                (
                    f"refunding ${amount:,.2f} to the linked bank account",
                    Funding.credit_linked_account,
                    [inputs.client_id, amount, f"{transfer_id}-refund"],
                )
            )

            await self._set_state("Registering the account with the custodian.")
            await self._execute(Custodian.register_account, inputs.client_id, investment_id, amount)
        except ActivityError as e:
            cause = e.__cause__
            reason = cause.message if isinstance(cause, ApplicationError) else str(cause or e)
            await self._set_state(f"Failed: {reason}. Rolling back.")
            for description, activity_fn, args in reversed(compensations):
                await self._set_state(f"Rolling back: {description}.")
                await self._execute(activity_fn, *args)
            await self._set_state(f"Rolled back. The account was not opened: {reason}.")
            return OpenInvestmentAccountOutput(
                account_created=False, message=f"The account was not opened: {reason}"
            )

        await self._set_state("Complete")
        return OpenInvestmentAccountOutput(account_created=True, message="investment account created")

    @staticmethod
    def _welcome_bonus(inputs: OpenInvestmentAccountInput) -> float:
        """Welcome-bonus promotion: 1% of the initial deposit, up to $500."""
        # Deliberate bug for the "Bug in workflow code" demo; the fix is
        # inputs.initial_amount. See README "Failure scenarios".
        # return min(inputs.initial_amount * 0.01, 500.0)
        return min(inputs.initial_deposit * 0.01, 500.0)

    async def _execute(self, activity_fn, *args):
        return await workflow.execute_activity(
            activity_fn,
            args=list(args),
            start_to_close_timeout=self.start_to_close_timeout,
            retry_policy=TOOL_RETRY_POLICY,
        )

    @workflow.query
    def get_current_state(self) -> str:
        return self.current_state

    @workflow.update
    async def get_client_details(self) -> WealthManagementClient:
        await workflow.wait_condition(lambda: self.initialized)
        return self.client

    @workflow.update
    async def update_client_details(self, client_fields: dict) -> str:
        result = await self._execute(ClientActivities.update_client, self.inputs.client_id, client_fields)
        self.kyc_verified = True
        return result

    @workflow.signal
    async def verify_kyc(self) -> None:
        await workflow.wait_condition(lambda: self.initialized)
        self.kyc_verified = True

    @workflow.signal
    async def compliance_approved(self) -> None:
        await workflow.wait_condition(lambda: self.initialized and self.kyc_verified)
        self.compliance_reviewed = True

    async def _set_state(self, state: str) -> None:
        self.current_state = state
        info = workflow.info()
        if info.parent is not None:
            parent_handle = workflow.get_external_workflow_handle(info.parent.workflow_id)
            await parent_handle.signal(
                "update_account_opening_state",
                UpdateAccountOpeningStateInput(
                    account_name=self.inputs.account_name,
                    state=state,
                    workflow_id=info.workflow_id,
                ),
            )
