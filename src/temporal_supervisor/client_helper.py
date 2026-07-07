"""Connects a Temporal client with the StrandsPlugin attached.

The plugin must be on the *client* (not just the worker) so that the plugin's
failure converter is installed — this is what lets activity-tool interrupts and
structured output cross the activity boundary. Workers built from this client
inherit the plugin automatically.
"""

import asyncio
import os

from temporalio.client import Client, TLSConfig
from temporalio.contrib.strands import StrandsPlugin

from temporal_supervisor.model_config import MODELS


def _tls_config():
    """Build a TLSConfig from env vars for Temporal Cloud, or None for local."""
    cert_path = os.getenv("TEMPORAL_TLS_CLIENT_CERT_PATH")
    key_path = os.getenv("TEMPORAL_TLS_CLIENT_KEY_PATH")
    if cert_path and key_path:
        with open(cert_path, "rb") as f:
            client_cert = f.read()
        with open(key_path, "rb") as f:
            client_key = f.read()
        return TLSConfig(client_cert=client_cert, client_private_key=client_key)
    return None


async def connect_client(max_attempts: int = 5) -> Client:
    address = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
    namespace = os.getenv("TEMPORAL_NAMESPACE", "default")
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return await Client.connect(
                address,
                namespace=namespace,
                tls=_tls_config() or False,
                plugins=[StrandsPlugin(models=MODELS)],
            )
        except RuntimeError as exc:
            # Tolerate transient transport resets during startup.
            last_exc = exc
            if attempt < max_attempts:
                await asyncio.sleep(0.5 * attempt)
    raise last_exc  # type: ignore[misc]
