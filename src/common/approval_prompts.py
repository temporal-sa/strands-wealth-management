"""Friendly, per-tool confirmation prompts for destructive actions.

Shared by both versions so the wording matches:
  - the Strands CLI uses this directly inside its intervention (see
    ``strands_supervisor.approval``), and
  - the Temporal version calls it from an activity (the approval hook runs in
    workflow context and must stay deterministic, so the name lookup — which
    reads the data files — happens off the workflow).

Resolves the target's name from the file-backed managers so the customer sees
who/what they are about to affect. Returns ``None`` for any non-destructive tool
or when the lookup comes up empty, signalling the caller to fall back to a
generic message.
"""

from common.beneficiaries_manager import BeneficiariesManager
from common.investment_manager import InvestmentManager


def friendly_confirmation(tool_name: str, tool_input: dict) -> str | None:
    """A natural-language confirmation for a destructive tool, or None to fall back."""
    if tool_name == "delete_beneficiary":
        name = BeneficiariesManager().display_name(
            tool_input.get("client_id"), tool_input.get("beneficiary_id")
        )
        target = f"beneficiary {name}" if name else "this beneficiary"
        return f"Are you sure you want to delete {target}?"
    if tool_name == "close_investment":
        name = InvestmentManager().account_name(
            tool_input.get("client_id"), tool_input.get("investment_id")
        )
        target = f"investment account '{name}'" if name else "this investment account"
        return f"Are you sure you want to close {target}?"
    return None