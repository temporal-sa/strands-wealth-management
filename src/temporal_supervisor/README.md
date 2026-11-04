# Wealth Management Agent Example using Temporal + Strands

Demonstrates how to make the [Strands Agents](https://strandsagents.com/) wealth
management assistant **durable** by running it inside a
[Temporal](https://temporal.io) Workflow with the
[`StrandsPlugin`](https://docs.temporal.io/develop/python/integrations/strands-agents),
and driving it from a **React frontend** through a **FastAPI** backend. The plugin routes every model invocation and tool call through
Temporal Activities, so each step is recorded in Workflow history and survives
crashes, restarts, and infrastructure failures.

The vanilla Strands (CLI) version of this example is located [here](../strands_supervisor/README.md).

## Application architecture

![](../../images/application-architecture.png)

The React frontend uses adaptive polling (2s while awaiting the assistant, 5s
otherwise) to read new events from Redis via the API, giving real-time status
updates as the open-account child workflow progresses.

## What this demonstrates

| Temporal feature | Where |
|---|---|
| **Durable model + tool calls** | `TemporalAgent` in `workflows/supervisor_workflow.py` |
| **Agents as tools (nested)** | The supervisor delegates to `beneficiary_assistant` / `investment_assistant`; the investment agent delegates opening onward to `open_account_assistant` (`_make_*_assistant`) |
| **Tools as Activities** | `activities/` wrapped with `activity_as_tool` |
| **Child workflow** | The open-account agent starts `OpenInvestmentAccountWorkflow` |
| **Human approval gate** | `ApprovalHook` interrupts before `delete_beneficiary` / `close_investment`; the workflow waits for an `approve` Signal |
| **Human compliance gate** | The open-account child blocks on a `compliance_approved` Signal, sent with `sendcomplianceapproval.sh` |
| **Updates** | KYC reads and updates client details through the child workflow's `get_client_details` / `update_client_details` Updates |
| **Conversation history in Redis** | `EventStreamActivities` persist chat + status events the UI polls |
| **Retries, non-retryable errors, saga** | See [Failure scenarios](#failure-scenarios-chaos-panel) |
| **Task queue rate limiting** | Model calls on their own task queue, optionally capped with `LLM_MAX_CALLS_PER_SECOND` (`run_worker.py`) |

The chat workflow keeps the whole conversation in one run; a production version
would need Continue-as-New for long conversations (see the TODO in
`workflows/supervisor_workflow.py`).

### A note on the agent topology

Strands has no agent-to-agent handoff primitive, so this version uses the Strands
["agents as tools"](https://strandsagents.com/latest/documentation/docs/user-guide/concepts/multi-agent/agents-as-tools/)
pattern, nested two levels deep: a supervisor `TemporalAgent` delegates each request
to a specialized sub-agent — itself a `TemporalAgent` — exposed to it as a tool
(`beneficiary_assistant`, `investment_assistant`). The **investment agent** in turn
owns account-opening and delegates it onward to a third sub-agent,
`open_account_assistant` (see `_make_*_assistant` in
`workflows/supervisor_workflow.py`). The `StrandsPlugin` routes every model
invocation and tool call through Activities, so each sub-agent's work is durably
recorded in the workflow history. Delegation is one-directional (caller → specialist);
a specialist never calls back up.

The beneficiary and investment agents are rebuilt per request, but the
**open-account agent is persistent**: it is built once and reused across customer
turns so it can hold
the open-account child-workflow ID while it walks the customer through KYC and waits
for compliance review — regardless of which agent invokes it, since it is keyed on
the workflow, not the caller. Opening an account is therefore owned by this dedicated
agent, which drives the durable `OpenInvestmentAccountWorkflow` child workflow, and
is reached via the investment agent rather than from the supervisor directly.

The CLI-only
[`strands_supervisor`](../strands_supervisor/README.md) version uses the same
"agents as tools" topology without the Temporal durability layer (there, opening an
account is a single synchronous tool inside the investment agent rather than a child
workflow).

## Prerequisites

* [uv](https://docs.astral.sh/uv/) and Python 3.10 or later
* [Node.js](https://nodejs.org/) 18+ and npm (for the React frontend)
* A [Gemini API key](https://aistudio.google.com/api-keys)
* [Temporal CLI](https://docs.temporal.io/cli#install)
* [Redis](https://redis.io/downloads/)

## Setup

From the project root:

```bash
uv sync
cp setgeminikey.example setgeminikey.sh   # then edit and paste your key
```

The worker, API, and chat scripts `cd` to the project root and source
`setgeminikey.sh` if it exists, so they can be run from any directory.

Start Redis and a local Temporal dev server in separate terminals:

```bash
redis-server
```

```bash
temporal server start-dev
```

Redis location can be overridden with `REDIS_HOST` / `REDIS_PORT` (see
[Environment variables](#environment-variables)).

## Run the demo (local)

**1. Start the worker** (terminal 1, from the project root):

```bash
src/temporal_supervisor/startlocalworker.sh
```

**2. Start the API** (terminal 2, from the project root):

```bash
src/api/startlocalapi.sh        # uvicorn on http://127.0.0.1:8000
```

**3. Start the React frontend** (terminal 3, from the project root):

```bash
cd src/frontend
npm install      # first time only
npm start        # opens http://localhost:3000
```

Click **Start Chat**, then try client ID `123` or `234`. Questions that aren't
about beneficiaries or investment accounts are politely declined by the supervisor
agent (handled in its prompt).

### Human approval (delete / close)

When you ask to delete a beneficiary or close an investment account, the
sub-agent's `ApprovalHook` interrupts before the tool runs and the workflow
pauses. The bot asks for confirmation inline in the chat (for example "Are you
sure you want to delete beneficiary John Doe?"). Answer by typing: a reply that
starts with *yes*, *y*, *ok*, *sure*, *approve*, *confirm*, *proceed* (and a few
similar words) approves; anything else denies. The UI sends your answer to
`POST /approve` (`response=approve` or `deny`), which signals the workflow, and
the agent either runs the tool or tells you it wasn't done.

### How the Open Account functionality works

1. You ask to open an account. The supervisor routes to `investment_assistant`,
   which hands the whole flow to the persistent `open_account_assistant`.
2. Its `open_new_investment_account` tool checks the business rules
   (`validate_new_account`: at least $100, at most $1,000,000), looks up any
   promotion (`find_promotion`), and starts the `OpenInvestmentAccountWorkflow`
   child workflow. The child loads the client and waits for KYC.
3. **KYC:** the agent shows your current details (`get_current_client_info`, an
   Update on the child) and asks whether they are correct. It can approve KYC
   (`approve_kyc`, a `verify_kyc` Signal) only after you confirm in a later
   message; if details need changing, `update_client_details` (an Update) saves
   them and also completes KYC.
4. The child moves to **Waiting Compliance Review** and waits for the
   `compliance_approved` Signal, sent by an employee with the script below.
5. **Saga:** the child creates the account record, transfers the deposit from the
   linked bank account, and registers the account with the custodian. If a step
   fails non-retryably, the completed steps are undone in reverse order (refund,
   then remove the account) and the status ends with "Rolled back".
6. Each state change is signalled to the chat workflow
   (`update_account_opening_state`), shown in the UI status area as
   `New <account> account status changed: <state>`, and read by the agent with its
   `get_account_opening_status` tool, so it reports the real status instead of
   what it remembers. "Complete" means the account was opened.

The **Bug in workflow code** chaos switch adds a welcome-bonus promo code that
pauses the child after compliance approval; see
[Bug in the workflow code](#bug-in-the-workflow-code).

### Approving compliance

Compliance approval is sent from a terminal (there is no button in the UI). The
script `cd`s to the project root, so it can be run from any directory:

```bash
# local Temporal server
src/temporal_supervisor/localsendcomplianceapproval.sh <child-workflow-id>
# Temporal Cloud (sources setcloudenv.sh from the project root)
src/temporal_supervisor/sendcomplianceapproval.sh <child-workflow-id>
```

The status text does not include the child workflow ID. Its format is
`open-account-<chat workflow ID>-<account-name-slug>-<uuid>`, for example
`open-account-strands-temporal-agent-k3j9x2m1q-retirement-<uuid>`. To find it, open
the [Temporal UI](http://localhost:8233), look for the **Running**
`OpenInvestmentAccountWorkflow` (a child of your chat workflow, whose ID starts
with `strands-temporal-agent-`) and copy its Workflow ID. You can also list them
with the Temporal CLI:

```bash
temporal workflow list --query "WorkflowType='OpenInvestmentAccountWorkflow' AND ExecutionStatus='Running'"
```

The signal waits until KYC has been approved, so send it after you have confirmed
your details.

### Resetting the demo data

Opening and closing accounts and editing beneficiaries change the JSON files in
`data/` (the saga also changes linked bank account balances in
`data/funding_accounts.json`). Restore them from the project root with
`git checkout data/`.

## Failure scenarios (chaos panel)

Click **Chaos** in the chat header to open the chaos panel. Its switches inject
failures at runtime: activities and the model read the flags from Redis on every
attempt, so you can turn a failure on and off mid-conversation without restarting
anything. The panel also lists each injected failure as it happens (activity and
attempt number). Open the [Temporal UI](http://localhost:8233) alongside it to show
the pending activity, its attempt count, and the last failure. **Reset all** turns
every flag off.

| Scenario | How to show it | What Temporal does |
|---|---|---|
| **Tool / API down** | Turn on *Core banking API down*, then ask to list your investments (or open an account, so it fails after compliance approval). Turn it off. | The activity retries (1s backoff, capped at 5s) for as long as the outage lasts. The agent waits mid-turn and answers once the API is back. Nothing is lost or re-asked. |
| **LLM down** | Turn on *Gemini API down* and send a message. Turn it off. | The model call is an activity, so it retries the same way (backoff capped at 10s). The conversation picks up exactly where it was. |
| **LLM rate limited (reactive)** | Turn on *Gemini rate limited (429)* and send a message. | The model raises an `ApplicationError` with `next_retry_delay` (15s, like a `Retry-After` header), which overrides the retry policy's backoff. Real Gemini 429s are handled the same way (`ChaosAwareGeminiModel` in `model_config.py`). |
| **LLM rate limited (proactive)** | Start the worker with `LLM_MAX_CALLS_PER_SECOND=0.2 src/temporal_supervisor/startlocalworker.sh`, then chat in several browser tabs at once. | Model calls run on their own task queue (`wealth-management-strands-llm` by default; set with `TEMPORAL_TASK_QUEUE_LLM`). The Temporal service dispatches at most that many per second across *all* workers, so calls queue up instead of hitting the provider's limit. Tools on the main queue aren't slowed down. |
| **Business rule (non-retryable)** | Ask to open an account with $2,000,000 (or less than $100). | `validate_new_account` raises a non-retryable `ApplicationError`: one attempt, no retries. The reason goes back to the agent, which explains it to the customer. Compare it with *Tool / API down* in the Temporal UI. |
| **Saga** | Turn on *Custodian rejects new accounts* (and *Slow account-opening steps* so you can watch), then open an account through compliance approval. Without flags: as client `123`, open an account with $300,000 (the linked bank account has $250,000). | After compliance approval the child workflow creates the account record, transfers the deposit from the linked bank account, and registers with the custodian. When a step fails non-retryably, the completed steps are undone in reverse order (refund, then remove the account). Compensations are activities too, so they are retried until they succeed. Kill the worker mid-rollback and the rollback still finishes. Transfers are idempotent, so a retried debit or refund never moves money twice. |
| **Bug in workflow code** | Turn on *Bug in workflow code*, then open an account through compliance approval. See below. | The workflow pauses at the failing line, keeping all its state, until a worker with fixed code picks it up. |

### Bug in the workflow code

Turn on **Bug in workflow code**, then open an account and approve KYC and
compliance. While the switch is on, `find_promotion` gives new account openings a
`WELCOME-BONUS` promo code, which is passed to `OpenInvestmentAccountWorkflow` as
part of its input. The welcome-bonus code path (`_welcome_bonus` in
`workflows/open_account_workflow.py`) has a real bug: it reads
`inputs.initial_deposit`, a field that doesn't exist. Openings without the promo
code never reach it.

Right after compliance approval, the workflow raises `AttributeError:
'OpenInvestmentAccountInput' object has no attribute 'initial_deposit'`. In the
Python SDK an ordinary exception fails the *workflow task*, not the workflow, so
the workflow stays **Running**, paused at that point with the compliance approval
intact. The Temporal UI shows the failing workflow task and its stack trace.
Other conversations, and openings without the promo code, keep working.

Turning the switch off doesn't help: the promo code is part of the workflow's
input, so every retry hits the same line. The fix is code. Change
`inputs.initial_deposit` to `inputs.initial_amount` and restart the worker. The
workflow resumes on the server's next workflow-task retry and opens the account
with the bonus.

Workflow-task retries back off over time, up to 10 minutes. If the fix is
deployed within a couple of minutes, the workflow resumes within about a minute.

After the demo, re-arm the bug:

```bash
git checkout src/temporal_supervisor/workflows/open_account_workflow.py
```

### Optional: CLI client

A command-line client is included for quick testing without the frontend/API (the
worker must be running):

```bash
src/temporal_supervisor/startchat.sh
```

It starts a new chat workflow (ID `wealth-management-<uuid>`, printed at start)
and prints status updates as `[status] ...`. When a delete or close needs
approval it prints `[approval needed] <question>` and asks `Approve? (yes/no)`;
`y`, `yes`, or `approve` approves and anything else denies. Type `exit`, `quit`,
or `end` to stop.

## API endpoints

`src/api/main.py` (FastAPI, started by `src/api/startlocalapi.sh` or
`src/api/startcloudapi.sh` on http://127.0.0.1:8000). Needs Temporal, Redis, and the
worker running. CORS allows the frontend on port 3000 (`localhost` and `127.0.0.1`).
All parameters are query parameters.

| Method | Path | Parameters | What it does |
|---|---|---|---|
| GET | `/` | | Health message |
| POST | `/start-workflow` | `workflow_id` | Starts a `WealthManagementWorkflow` |
| POST | `/send-prompt` | `workflow_id`, `prompt` | Signals a customer message |
| GET | `/get-chat-history` | `workflow_id`, `from_index` (default 0) | Chat and status events from Redis |
| GET | `/pending-approval` | `workflow_id` | The pending delete/close confirmation question, or `null` |
| POST | `/approve` | `workflow_id`, `response` (`approve` or `deny`) | Answers the pending confirmation |
| POST | `/end-chat` | `workflow_id` | Signals the workflow to end |
| GET | `/chaos` | | Chaos flags and recent injected failures |
| POST | `/chaos` | `flag`, `enabled` | Turns one chaos flag on or off |
| POST | `/chaos/reset` | | Turns every flag off and clears the failure log |

## Environment variables

| Variable | Default | Used for |
|---|---|---|
| `GEMINI_API_KEY` (or `GOOGLE_API_KEY`) | | Gemini API key (worker) |
| `TEMPORAL_ADDRESS` | `localhost:7233` | Temporal service address |
| `TEMPORAL_NAMESPACE` | `default` | Temporal namespace |
| `TEMPORAL_TLS_CLIENT_CERT_PATH` / `TEMPORAL_TLS_CLIENT_KEY_PATH` | | mTLS cert and key for Temporal Cloud (both must be set) |
| `TEMPORAL_TASK_QUEUE` | `wealth-management-strands` | Main task queue (workflows and tools). Set the same value for the worker, API, and CLI client |
| `TEMPORAL_TASK_QUEUE_LLM` | `<TEMPORAL_TASK_QUEUE>-llm` | Task queue for model calls. The older name `TEMPORAL_LLM_TASK_QUEUE` is still accepted |
| `LLM_MAX_CALLS_PER_SECOND` | unset (no limit) | Max model calls per second dispatched from the LLM queue, across all workers |
| `TEMPORAL_TASK_QUEUE_OPEN_ACCOUNT` | `<TEMPORAL_TASK_QUEUE>` | Task queue the open-account child workflow is started on. If you change it, also run a worker on that queue, e.g. `TEMPORAL_TASK_QUEUE=<queue> src/temporal_supervisor/startlocalworker.sh` |
| `REDIS_HOST` / `REDIS_PORT` | `localhost` / `6379` | Redis for chat history, status updates, and chaos flags |

The scripts pass the environment through, so you can prefix any of them, e.g.
`LLM_MAX_CALLS_PER_SECOND=0.2 src/temporal_supervisor/startlocalworker.sh`.

## Tests

From the project root:

```bash
uv run pytest
```

The tests cover the durable approval gate across sub-agents, the KYC guard, the
failure scenarios (saga rollback, retries, the workflow bug), and the off-topic
prompt. They use a scripted model and stub activities, so no Gemini key or Redis is
needed; the Temporal test server is downloaded automatically on first run.

## Run on Temporal Cloud

Redis still runs locally. From the project root:

```bash
cp src/temporal_supervisor/setcloudenv.example setcloudenv.sh   # edit with your namespace + mTLS cert paths
# terminal 1 (worker):
source ./setcloudenv.sh && source ./setgeminikey.sh && uv run python src/temporal_supervisor/run_worker.py
# terminal 2 (API; the script sources setcloudenv.sh itself):
src/api/startcloudapi.sh
# terminal 3 (frontend):
cd src/frontend && npm install && npm start
```

`connect_client` (`client_helper.py`) reads `TEMPORAL_ADDRESS`,
`TEMPORAL_NAMESPACE`, and the `TEMPORAL_TLS_CLIENT_CERT_PATH` /
`TEMPORAL_TLS_CLIENT_KEY_PATH` env vars.

Send the compliance approval with the cloud script, which sources `setcloudenv.sh`
itself:

```bash
src/temporal_supervisor/sendcomplianceapproval.sh <child-workflow-id>
```

`startchat.sh` does not source `setcloudenv.sh`; to use the CLI chat client against
Temporal Cloud, run `source ./setcloudenv.sh` in your shell first.

Find the child workflow ID in your namespace in the Temporal Cloud UI, as described
in [Approving compliance](#approving-compliance).
