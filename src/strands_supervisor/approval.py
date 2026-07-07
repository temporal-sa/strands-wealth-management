"""Human-approval gate with friendly, per-tool confirmation prompts.

The vended ``HumanInTheLoop`` intervention asks with a generic message:

    Tool "delete_beneficiary" requires human approval. Input: {"client_id": ...} (y/n):

``ConfirmDestructive`` subclasses it to ask a natural question instead, resolving
the target's name from the file-backed managers so the customer sees who/what
they are about to affect:

    Are you sure you want to delete beneficiary Jake Doe? (y/n):

The prompt is the only thing overridden; the parent's approval/deny/trust logic
is reused unchanged. Falls back to the default phrasing for any other tool or if
the name lookup comes up empty.
"""

import inspect
import json
from typing import Any

from strands.interventions.actions import Confirm, Deny, InterventionAction, Proceed
from strands.hooks.events import BeforeToolCallEvent
from strands.vended_interventions.hitl import HumanInTheLoop

from common.approval_prompts import friendly_confirmation


class ConfirmDestructive(HumanInTheLoop):
    """HumanInTheLoop that asks a friendly, named confirmation question."""

    # A distinct class name is required: HumanInTheLoop.name is a fixed class
    # attribute, so at most one handler of a given name registers per agent.
    name = "strands:confirm-destructive"

    async def before_tool_call(
        self, event: BeforeToolCallEvent, **kwargs: Any
    ) -> InterventionAction:
        tool_name = event.tool_use["name"]
        if not self._requires_approval(event):
            return Proceed()

        prompt = friendly_confirmation(tool_name, event.tool_use["input"]) or (
            f'Tool "{tool_name}" requires human approval. '
            f'Input: {json.dumps(event.tool_use["input"])}'
        )
        is_negated = f"!{tool_name}" in self._allowed_tools

        # No inline ask configured: defer to interrupt/resume (unused in the CLI,
        # kept for parity with the parent's contract).
        if self._ask is None:
            def evaluate(response: Any) -> bool:
                if not is_negated and self._enable_trust and self._evaluate_trust(response):
                    self._trust_tool(event, tool_name)
                    return True
                return self._evaluate(response)

            return Confirm(prompt=prompt, evaluate=evaluate)

        # Inline mode (ask="stdio"): collect the response now.
        response = self._ask(prompt)
        if inspect.isawaitable(response):
            response = await response
        if response is None:
            return Deny(reason=f'Tool "{tool_name}" denied: approval callback returned no response.')
        if not is_negated and self._enable_trust and self._evaluate_trust(response):
            self._trust_tool(event, tool_name)
            return Proceed()
        return Confirm(prompt=prompt, response=response, evaluate=self._evaluate)