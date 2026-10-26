from typing import Optional

from temporalio import activity, workflow
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from common.investment_manager import InvestmentManager, InvestmentAccount
    from common.chaos_manager import ChaosFlag
    from temporal_supervisor.chaos import core_banking_api, is_enabled, maybe_slow_down

MIN_INITIAL_DEPOSIT = 100.0
MAX_INITIAL_DEPOSIT = 1_000_000.0
WELCOME_BONUS_PROMO = "WELCOME-BONUS"


class Investments:
    @staticmethod
    @activity.defn
    async def list_investments(client_id: str) -> list:
        """List all investment accounts for a given client."""
        activity.logger.info(f"list_investments: {client_id}")
        await core_banking_api()
        return InvestmentManager().list_investment_accounts(client_id)

    @staticmethod
    @activity.defn
    async def validate_new_account(client_id: str, account_name: str, initial_amount: float) -> None:
        """Check the business rules for opening an account. Raises a non-retryable error if one fails."""
        activity.logger.info(f"validate_new_account: {client_id}, {account_name}, {initial_amount}")
        if initial_amount < MIN_INITIAL_DEPOSIT:
            raise ApplicationError(
                f"The initial deposit must be at least ${MIN_INITIAL_DEPOSIT:,.2f}",
                type="DepositBelowMinimum",
                non_retryable=True,
            )
        if initial_amount > MAX_INITIAL_DEPOSIT:
            raise ApplicationError(
                f"Initial deposits over ${MAX_INITIAL_DEPOSIT:,.2f} cannot be opened online "
                "and must be arranged with a wealth advisor",
                type="DepositLimitExceeded",
                non_retryable=True,
            )

    @staticmethod
    @activity.defn
    async def find_promotion(client_id: str) -> Optional[str]:
        """The promo code for a new account, if a promotion is running.

        The chaos panel's "Bug in workflow code" switch runs the welcome-bonus
        promotion, whose code path in OpenInvestmentAccountWorkflow has a bug.
        """
        activity.logger.info(f"find_promotion: {client_id}")
        return WELCOME_BONUS_PROMO if await is_enabled(ChaosFlag.WORKFLOW_BUG) else None

    @staticmethod
    @activity.defn
    async def open_investment(new_account: InvestmentAccount) -> dict:
        """Create a new investment account. Called by the open-account child workflow."""
        activity.logger.info(
            f"open_investment: {new_account.client_id}, name={new_account.name}, balance={new_account.balance}"
        )
        await maybe_slow_down()
        await core_banking_api()
        return InvestmentManager().add_investment_account(new_account)

    @staticmethod
    @activity.defn
    async def remove_investment(client_id: str, investment_id: str) -> bool:
        """Remove an investment account record (saga compensation for open_investment)."""
        activity.logger.info(f"remove_investment: {client_id}, {investment_id}")
        await maybe_slow_down()
        await core_banking_api()
        InvestmentManager().delete_investment_account(client_id, investment_id)
        # Already gone counts as removed, so a retried compensation succeeds.
        return True

    @staticmethod
    @activity.defn
    async def close_investment(client_id: str, investment_id: str) -> str:
        """Close an investment account for a client."""
        activity.logger.info(f"close_investment: {client_id}, {investment_id}")
        await core_banking_api()
        manager = InvestmentManager()
        name = manager.account_name(client_id, investment_id)
        success = manager.delete_investment_account(client_id, investment_id)
        if success:
            return f"{name} closed successfully." if name else f"Investment account {investment_id} closed successfully."
        return f"Investment account {investment_id} not found or could not be closed."
