# Wealth Management Agent Example using Strands Agents

Demonstrates how to build a multi-agent wealth management assistant with
[Strands Agents](https://strandsagents.com/) using a supervisor that delegates to
specialized sub-agents (the Strands ["agents as tools"](https://strandsagents.com/latest/documentation/docs/user-guide/concepts/multi-agent/agents-as-tools/) pattern).

It ships in two flavors, mirroring the
[ADK Wealth Management demo](https://github.com/temporal-sa/adk-wealth-management):

- **Strands only** (command-line) — [`src/strands_supervisor`](src/strands_supervisor/README.md)
- **Temporal + Strands** (durable execution, with a **React frontend + FastAPI backend**) —
  [`src/temporal_supervisor`](src/temporal_supervisor/README.md), which wraps the agentic flow with
  [Temporal](https://temporal.io) via the
  [`StrandsPlugin`](https://docs.temporal.io/develop/python/integrations/strands-agents).
  The web app lives in [`src/frontend`](src/frontend) and the API in [`src/api`](src/api).

Scenarios currently implemented include:

* **Add Beneficiary** — add a new beneficiary to your account
* **List Beneficiaries** — show beneficiaries and their relationship to the account owner
* **Delete Beneficiary** — delete a beneficiary from your account
* **Open Investment Account** — open a new investment account (using a **child workflow** in the Temporal version)
* **List Investments** — show accounts and their current balances
* **Close Investment Account** — close an investment account

## Architecture

A **Supervisor** agent talks to the customer and routes each request to a specialist:

```
                    ┌─────────────────────┐
       customer ──▶ │   Supervisor Agent  │
                    └──────────┬──────────┘
                      delegates │ (with client_id)
        ┌──────────────────────┼──────────────────────┐
        ▼                      ▼                       ▼
┌───────────────────┐ ┌───────────────────┐ ┌──────────────────────┐
│ Beneficiary Agent │ │  Investment Agent │ │  Open Account Agent   │
│  list / add /     │ │  list / close     │ │  open a new account   │
│  delete           │ │  (open *)         │ │  (Temporal only)      │
└───────────────────┘ └───────────────────┘ └──────────────────────┘
```

In the **Temporal** version, opening an investment account is delegated to a
dedicated **Open Account Agent** that drives an `OpenInvestmentAccountWorkflow`
child workflow, gating on KYC and a human compliance-approval signal before
creating the account.

In the **Strands-only** (CLI) version there is no separate Open Account Agent:
opening (`*`) is a single synchronous tool inside the Investment Agent, so that
agent does list / open / close.

## Prerequisites

* [uv](https://docs.astral.sh/uv/) — Python package and project manager
* [Google / Gemini API key](https://aistudio.google.com/api-keys) — to access Gemini models
* [Temporal CLI](https://docs.temporal.io/cli#install) — local Temporal service (Temporal version only)
* [Redis](https://redis.io/downloads/) — stores conversation history and status updates (Temporal version only)
* [Node.js](https://nodejs.org/) 18+ and npm — for the React frontend (Temporal version only)

## Set up the Python environment

```bash
uv sync
```

## Set up your Gemini API key

```bash
cp setgeminikey.example setgeminikey.sh
chmod +x setgeminikey.sh
```

Edit `setgeminikey.sh` and paste in your key:

```bash
export GEMINI_API_KEY="Your API Key Goes Here"
```

## Getting started

* Strands-only CLI version → [src/strands_supervisor/README.md](src/strands_supervisor/README.md)
* Temporal + Strands version → [src/temporal_supervisor/README.md](src/temporal_supervisor/README.md)
