# Wealth Management Frontend (Temporal + Strands)

A small React (Create React App) UI for the
[Temporal version](../temporal_supervisor/README.md) of the demo. It talks to the
FastAPI backend in [`src/api`](../api) (endpoints are listed in the
[Temporal README](../temporal_supervisor/README.md#api-endpoints)) and uses adaptive
polling (2s while waiting for the assistant, 5s otherwise) to show conversation
history and real-time status updates from Redis.

- **Start Chat** starts a new chat workflow with a random ID
  (`strands-temporal-agent-<random>`, prefix set in `src/App.js`), so every chat is
  a separate workflow. **End Chat** ends it.
- **Delete / close confirmations** appear inline in the chat as a question from
  the bot. Answer by typing: a reply starting with *yes*, *y*, *ok*, *sure*,
  *approve*, *confirm*, *proceed* (or similar) approves, anything else denies. The
  answer is sent to `POST /approve` instead of as a chat message.
- **Status area** at the top shows the latest account-opening status. Compliance
  approval is not done in the UI; use the script described in
  [Approving compliance](../temporal_supervisor/README.md#approving-compliance).
- **Chaos** (header button) opens the chaos panel for injecting failures; see
  [Failure scenarios](../temporal_supervisor/README.md#failure-scenarios-chaos-panel).

## Prerequisites

- [Node.js](https://nodejs.org/) 18+ and npm
- Temporal, Redis, the worker, and the API running (see
  [../temporal_supervisor/README.md](../temporal_supervisor/README.md))

## Run

From the project root:

```bash
cd src/frontend
npm install   # first time only
npm start     # http://localhost:3000
```

The API base URL is configured in [`src/config.js`](src/config.js)
(`http://127.0.0.1:8000` by default). The API's CORS settings allow the frontend on
port 3000 (`localhost` or `127.0.0.1`).

## Tests

```bash
cd src/frontend
npm test
```

`npm test` starts Jest in watch mode; press `q` to quit (or run `CI=true npm test` to run once).
The smoke test (`src/App.test.js`) renders the app and checks that the chat starts with a
**Start Chat** button and a disabled message box; it needs no backend.
