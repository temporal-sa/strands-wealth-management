"""Off-topic questions are declined through the supervisor's prompt, not guardrail code.

The specialists are called as tools, so every customer message reaches the
supervisor first; both supervisors (Temporal and standalone) carry the decline
block right after their first line.
"""

import pathlib
import re

from common.agent_constants import (
    BENE_INSTRUCTIONS,
    INVEST_INSTRUCTIONS,
    OPEN_ACCOUNT_INSTRUCTIONS,
    SUPERVISOR_INSTRUCTIONS,
    TEMPORAL_INVEST_SUBAGENT_INSTRUCTIONS,
    TEMPORAL_SUPERVISOR_INSTRUCTIONS,
)

DECLINE_BLOCK = """You only help with wealth management: the customer's beneficiaries and investment accounts.
If the customer asks about anything else (for example general knowledge, weather, news, or
other financial products), politely decline, say that you can only help with beneficiaries
and investment accounts, and do not answer the question. Then ask how you can help with
their beneficiaries or investment accounts."""

SRC = pathlib.Path(__file__).resolve().parents[1] / "src"


def test_supervisors_decline_off_topic_questions():
    for instructions in (SUPERVISOR_INSTRUCTIONS, TEMPORAL_SUPERVISOR_INSTRUCTIONS):
        first_line, rest = instructions.split("\n", 1)
        assert first_line == "You are a helpful wealth management assistant."
        assert rest.startswith(DECLINE_BLOCK)


def test_specialists_hand_off_topic_requests_back():
    for instructions in (
        BENE_INSTRUCTIONS,
        INVEST_INSTRUCTIONS,
        TEMPORAL_INVEST_SUBAGENT_INSTRUCTIONS,
        OPEN_ACCOUNT_INSTRUCTIONS,
    ):
        assert "If the request is not related to this routine, do not answer it." in instructions


def test_no_guardrail_code_remains():
    pattern = re.compile(r"guardrail|tripwire|ROUTING_INSTRUCTIONS|disable_guardrails", re.IGNORECASE)
    offenders = [
        str(path.relative_to(SRC))
        for path in SRC.rglob("*.py")
        if pattern.search(path.read_text())
    ]
    assert offenders == []
