"""Strands @tool functions backing the wealth management agents.

Each tool is a thin wrapper over the file-backed managers in ``common``.
Strands derives the tool schema from the type hints and docstring, so the
docstrings here are part of the contract the model sees.
"""

from strands import tool

from common.beneficiaries_manager import BeneficiariesManager
from common.investment_manager import InvestmentManager, InvestmentAccount


@tool
def list_beneficiaries(client_id: str) -> list:
    """List all beneficiaries for a given client.

    Args:
        client_id: The unique identifier of the client.

    Returns:
        A list of beneficiary records for the client.
    """
    return BeneficiariesManager().list_beneficiaries(client_id)


@tool
def add_beneficiary(client_id: str, first_name: str, last_name: str, relationship: str) -> str:
    """Add a new beneficiary for a client.

    Args:
        client_id: The unique identifier of the client.
        first_name: The beneficiary's first name.
        last_name: The beneficiary's last name.
        relationship: The beneficiary's relationship to the client (e.g. spouse, son, daughter).

    Returns:
        A confirmation message.
    """
    BeneficiariesManager().add_beneficiary(client_id, first_name, last_name, relationship)
    return f"Beneficiary {first_name} {last_name} added successfully."


@tool
def delete_beneficiary(client_id: str, beneficiary_id: str) -> str:
    """Delete a beneficiary for a client.

    Args:
        client_id: The unique identifier of the client.
        beneficiary_id: The unique identifier of the beneficiary to delete.

    Returns:
        A confirmation message.
    """
    manager = BeneficiariesManager()
    name = manager.display_name(client_id, beneficiary_id)
    manager.delete_beneficiary(client_id, beneficiary_id)
    return f"{name} deleted successfully." if name else f"Beneficiary {beneficiary_id} deleted successfully."


@tool
def list_investments(client_id: str) -> list:
    """List all investment accounts for a given client.

    Args:
        client_id: The unique identifier of the client.

    Returns:
        A list of investment account records for the client.
    """
    return InvestmentManager().list_investment_accounts(client_id)


@tool
def open_investment(client_id: str, account_name: str, initial_balance: float) -> dict:
    """Open a new investment account for a client.

    Args:
        client_id: The unique identifier of the client.
        account_name: The name for the new investment account.
        initial_balance: The opening balance for the account (must be non-negative).

    Returns:
        The newly created investment account record, or an error dict.
    """
    account = InvestmentAccount(client_id=client_id, name=account_name, balance=initial_balance)
    result = InvestmentManager().add_investment_account(account)
    if result is None:
        return {"error": "Failed to create investment account. Check that the balance is non-negative."}
    return result


@tool
def close_investment(client_id: str, investment_id: str) -> str:
    """Close an investment account for a client.

    Args:
        client_id: The unique identifier of the client.
        investment_id: The unique identifier of the investment account to close.

    Returns:
        A confirmation message.
    """
    manager = InvestmentManager()
    name = manager.account_name(client_id, investment_id)
    success = manager.delete_investment_account(client_id, investment_id)
    if success:
        return f"{name} closed successfully." if name else f"Investment account {investment_id} closed successfully."
    return f"Investment account {investment_id} not found or could not be closed."
