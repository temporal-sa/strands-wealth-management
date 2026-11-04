# Wealth Management Agent Example using Strands Agents

Demonstrates a multi-agent wealth management assistant built with
[Strands Agents](https://strandsagents.com/). A **Supervisor** agent delegates
to specialized **Beneficiary** and **Investment** agents using the Strands
["agents as tools"](https://strandsagents.com/latest/documentation/docs/user-guide/concepts/multi-agent/agents-as-tools/)
pattern.

The Temporal version of this example is located [here](../temporal_supervisor/README.md).

Scenarios currently implemented include:

* Add Beneficiary, List Beneficiaries, Delete Beneficiary
* Open Investment Account, List Investments, Close Investment Account
* Questions that aren't about beneficiaries or investment accounts are politely
  declined by the supervisor agent (handled in its prompt)

Before deleting a beneficiary or closing an account, the agent asks for
confirmation in the terminal (for example
`Are you sure you want to delete beneficiary John Doe? (y/n):`); answer `y` to
proceed. Opening an account is a single direct tool call (`open_investment`): no
KYC, compliance approval, or saga, which only the Temporal version has.

## How it works

* `tools.py` — the six `@tool` functions (backed by the file-based managers in `common/`).
* `agents.py` — the Gemini model factory, the two specialist agents exposed as
  tools (`beneficiary_assistant`, `investment_assistant`), and `build_supervisor()`.
* `approval.py` — `ConfirmDestructive`, the y/n confirmation before
  `delete_beneficiary` / `close_investment`.
* `main.py` — the interactive CLI loop.

The supervisor collects the client ID once, then forwards it on every delegation.

## Prerequisites

* [uv](https://docs.astral.sh/uv/) and Python 3.10 or later
* A [Gemini API key](https://aistudio.google.com/api-keys)

## Set up the Python environment

Run this in the project root (up two levels from this folder):

```bash
uv sync
```

## Set up your Gemini API key

Run this in the project root:

```bash
cp setgeminikey.example setgeminikey.sh
chmod +x setgeminikey.sh
# edit setgeminikey.sh and paste in your key
```

## Running the agent

From the project root:

```bash
source ./setgeminikey.sh
uv run strands-supervisor
# or, equivalently:
uv run python src/strands_supervisor/main.py
```

### Example session

```
============================================================
  Wealth Management Assistant (Strands)
============================================================
Type your message and press Enter. Type 'exit', 'quit', or 'end' to stop.

You: Who are my beneficiaries?

Assistant: I can help with that. What is your client ID?

You: 123

Assistant: Your current beneficiaries are:
- John Doe (son)
- Jane Doe (daughter)
- Joan Doe (spouse)

You: What investments do I have?

Assistant: Your investment accounts are:
- Checking: $1000.00
- Savings: $2312.08
- 401K: $11070.89

You: end
Goodbye!
```

You can also add/delete beneficiaries and open/close investment accounts.
Try client ID `123` or `234`. These changes are written to the JSON files in
`data/`; restore them from the project root with `git checkout data/`.
