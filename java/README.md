# Java Open Investment Account Worker

An optional Java (Spring Boot) implementation of `OpenInvestmentAccountWorkflow`, the child workflow
the open-account agent starts to open a new investment account. It is a drop-in replacement for the
Python version in [`src/temporal_supervisor/workflows/open_account_workflow.py`](../src/temporal_supervisor/workflows/open_account_workflow.py):
same workflow type, query (`get_current_state`), updates (`get_client_details`, `update_client_details`)
and signals (`verify_kyc`, `compliance_approved`), same states and messages, same saga (create the
account record, debit the linked bank account, register with the custodian, with compensations in
reverse order), the same transfer IDs (`<workflow ID>-initial-deposit`, refunds `...-refund`), and the
same `update_account_opening_state` signals to the parent chat workflow (`account_name`, `state`,
`workflow_id`). Without a parent (for example when started by `run_open_account_workflow.py`), it
signals nothing.

The workflow has no business logic of its own. It calls the Python activities by name (`get_client`,
`update_client`, `open_investment`, `remove_investment`, `debit_linked_account`,
`credit_linked_account`, `register_account`) on the Python worker's task queue (`TEMPORAL_TASK_QUEUE`,
default `wealth-management-strands`), so the Python worker must be running too, and the chaos panel's
flags apply unchanged. The Java worker registers no activities. Payloads are plain JSON with the
Python field names (snake_case), which the Python side reads and writes with the StrandsPlugin's
pydantic data converter.

As in the Python version, the `update_client_details` update saves the customer's changes (through the
Python `update_client` activity) and completes KYC, and `get_client_details` waits until the client
has been loaded.

## Prerequisites

* [Java JDK 25](https://adoptium.net/) as the default `java` - the Gradle wrapper (`./gradlew`, Gradle
  9.8.2) is included, so no separate Gradle install is needed
* Everything the Python demo needs (Temporal CLI, Redis, uv, Gemini API key). See the
  [Temporal README](../src/temporal_supervisor/README.md).

## Running locally

Start Temporal (`temporal server start-dev`) and Redis (`redis-server`).

The open-account task queue name is passed as the first argument to both workers. Start the Python
worker with it, so the supervisor starts the child workflow on that queue (and the Python worker
doesn't register the Python version of the workflow, but still runs all the activities). From the
project root:

```bash
src/temporal_supervisor/startlocalworker.sh Open-Account-Java
```

Then start the Java worker with the same name:

```bash
java/startlocalworker.sh Open-Account-Java
```

The script can be run from any directory. It sets `TEMPORAL_TASK_QUEUE_OPEN_ACCOUNT` from the
argument, sources `setclaimcheck.sh` from the project root if it exists, and runs
`./gradlew runWorker`. It connects to `127.0.0.1:7233`, namespace `default`
(`src/main/resources/application.yaml`); `TEMPORAL_ADDRESS` and `TEMPORAL_NAMESPACE` override them, as
for the Python components. When claim check is enabled, the Java worker uses the Redis at `REDIS_HOST`
/ `REDIS_PORT` (default `localhost:6379`).

Start the API and the frontend as described in the [Temporal README](../src/temporal_supervisor/README.md),
and send the compliance approval with `src/temporal_supervisor/localsendcomplianceapproval.sh` as usual.

To exercise the Java workflow without the chat or the LLM, run the three open-account scenarios
against it (with the Java worker running):

```bash
src/temporal_supervisor/startlocalopenaccount.sh Open-Account-Java
```

See [Testing the open-account workflow on its own](../src/temporal_supervisor/README.md#testing-the-open-account-workflow-on-its-own).

## Running with Temporal Cloud

Create `setcloudenv.sh` in the project root (copy `src/temporal_supervisor/setcloudenv.example`)
as described in [Run on Temporal Cloud](../src/temporal_supervisor/README.md#run-on-temporal-cloud).
Start the Python worker with the queue name, then the Java worker with the same name:

```bash
src/temporal_supervisor/startcloudworker.sh Open-Account-Java
```

```bash
java/startcloudworker.sh Open-Account-Java
```

The Java cloud script can be run from any directory. It sources `setclaimcheck.sh` (if it exists) and
`setcloudenv.sh` from the project root, maps `TEMPORAL_TLS_CLIENT_CERT_PATH` to the
`TEMPORAL_CERT_PATH` the Java worker reads, uses the Spring profile `tc`
(`src/main/resources/application-tc.yaml`), and runs `./gradlew runWorker`.

Java's TLS needs the client key as an unencrypted PKCS8 key (`-----BEGIN PRIVATE KEY-----`). If
`TEMPORAL_TLS_CLIENT_KEY_PATH` points to a different format, the script stops and prints the command
to convert it, for example:

```bash
openssl pkcs8 -topk8 -nocrypt -in /path/to/key.key -out /path/to/key.pkcs8.key
```

Then add the converted key to `setcloudenv.sh` (the Python components keep using
`TEMPORAL_TLS_CLIENT_KEY_PATH`):

```bash
export TEMPORAL_KEY_PKCS8_PATH="/path/to/key.pkcs8.key"
```

## Claim check

With `USE_CLAIM_CHECK=true` (from `setclaimcheck.sh`, see [Claim check](../src/temporal_supervisor/README.md#claim-check-optional)),
the worker's client stores every payload in Redis and sends only its ID to Temporal
(`claimcheck/ClaimCheckCodec.java`). The metadata (`encoding: claim-checked`,
`temporal.io/claim-check-codec: v1`) and the stored bytes (the serialized `Payload` protobuf) are the
same as the Python codec's, so the Python and Java workers read each other's payloads and the Python
codec server decodes both. Turn it on for every component or none.

## Bug in the workflow code (chaos panel)

The chaos panel's **Bug in workflow code** switch gives new account openings the promo code
`WELCOME-BONUS`. In `src/main/java/io/temporal/demo/wealthmanagement/workflow/OpenInvestmentAccountWorkflowImpl.java`,
`BONUS_RATES` is keyed by `"WELCOME_BONUS"` (underscore), so `welcomeBonus` throws a
`NullPointerException` right after compliance approval. The exception fails the workflow task, not the
workflow, so the workflow stays **Running**, paused at that line. To fix it, use the commented-out
`BONUS_RATES` line above it and restart the Java worker; the workflow resumes on the next
workflow-task retry. Re-arm the bug afterwards, from the project root:

```bash
git checkout java/src/main/java/io/temporal/demo/wealthmanagement/workflow/OpenInvestmentAccountWorkflowImpl.java
```

See [Failure scenarios](../src/temporal_supervisor/README.md#failure-scenarios-chaos-panel) for the other scenarios.

## Tests

The tests use the Temporal Java SDK's in-memory test environment with stub activities (and an
in-memory stand-in for Redis), so no Temporal server, Redis or Gemini key is needed:

```bash
cd java
./gradlew test       # or ./gradlew clean build
```

* `OpenInvestmentAccountWorkflowTest` - the saga and its rollback order, undoing only completed steps,
  transient failures retried, transfer IDs (a new opening after a rollback gets a new one),
  `update_client_details` completing KYC, every state signalled to a parent (and nothing without one),
  and the workflow bug pausing the workflow
* `PayloadJsonTest` - the Java classes read and write the JSON the Python side does
* `ClaimCheckCodecTest` - the claim check codec
