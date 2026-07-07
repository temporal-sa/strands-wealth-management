# Wealth Management Frontend (Temporal + Strands)

A small React (Create React App) UI for the
[Temporal version](../temporal_supervisor/README.md) of the demo. It talks to the
[FastAPI backend](../api) and uses adaptive polling to show conversation history
and real-time status updates from Redis.

It also surfaces the two human-in-the-loop gates:

- **Approve / Deny** when the agent wants to delete a beneficiary or close an account.
- **Approve Compliance** when an open-account child workflow is waiting for compliance review.

## Prerequisites

- [Node.js](https://nodejs.org/) 18+ and npm
- The Temporal worker and the API running (see [../temporal_supervisor/README.md](../temporal_supervisor/README.md))

## Run

```bash
npm install   # first time only
npm start     # http://localhost:3000
```

The API base URL is configured in [`src/config.js`](src/config.js)
(`http://127.0.0.1:8000` by default).
