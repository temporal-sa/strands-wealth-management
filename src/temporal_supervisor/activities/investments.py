from temporalio import activity, workflow

with workflow.unsafe.imports_passed_through():
    from common.investment_manager import InvestmentManager, InvestmentAccount


class Investments:
    @staticmethod
    @activity.defn
    async def list_investments(client_id: str) -> list:
        """List all investment accounts for a given client."""
        activity.logger.info(f"list_investments: {client_id}")
        return InvestmentManager().list_investment_accounts(client_id)

    @staticmethod
    @activity.defn
    async def open_investment(new_account: InvestmentAccount) -> dict:
        """Create a new investment account. Called by the open-account child workflow."""
        activity.logger.info(
            f"open_investment: {new_account.client_id}, name={new_account.name}, balance={new_account.balance}"
        )
        return InvestmentManager().add_investment_account(new_account)

    @staticmethod
    @activity.defn
    async def close_investment(client_id: str, investment_id: str) -> str:
        """Close an investment account for a client."""
        activity.logger.info(f"close_investment: {client_id}, {investment_id}")
        manager = InvestmentManager()
        name = manager.account_name(client_id, investment_id)
        success = manager.delete_investment_account(client_id, investment_id)
        if success:
            return f"{name} closed successfully." if name else f"Investment account {investment_id} closed successfully."
        return f"Investment account {investment_id} not found or could not be closed."
