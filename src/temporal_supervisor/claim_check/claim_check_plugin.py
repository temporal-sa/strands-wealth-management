"""Temporal plugin that adds the claim-check codec to a client's data converter.

It keeps the rest of the data converter as it is: list it *after* the
StrandsPlugin so the codec is added to the plugin's pydantic payload converter
and failure converter rather than replacing them (plugins configure the client
in list order). ``client_helper.client_plugins()`` adds it only when
USE_CLAIM_CHECK=true.
"""

import dataclasses
import os
from typing import Optional

from temporalio.converter import DataConverter
from temporalio.plugin import SimplePlugin

from common.redis_config import RedisConfig
from temporal_supervisor.claim_check.claim_check_codec import ClaimCheckCodec


def use_claim_check() -> bool:
    """Whether USE_CLAIM_CHECK is set to a true value (true / 1 / yes / on)."""
    return os.getenv("USE_CLAIM_CHECK", "false").strip().lower() in ("true", "1", "yes", "on")


class ClaimCheckPlugin(SimplePlugin):
    def __init__(self, codec: Optional[ClaimCheckCodec] = None):
        self.codec = codec or ClaimCheckCodec(RedisConfig())
        super().__init__("wealth_management.ClaimCheckPlugin", data_converter=self._add_codec)

    def _add_codec(self, converter: Optional[DataConverter]) -> DataConverter:
        return dataclasses.replace(converter or DataConverter.default, payload_codec=self.codec)
