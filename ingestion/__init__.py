"""Ingestion: move the simulator's files into Snowflake RAW tables.

RAW is a landing zone, not a clean layer. Records arrive exactly as the source
sent them, stored whole in a VARIANT column, with only load metadata added
alongside. Nothing is cleaned, typed, deduplicated or rejected here. If RAW is
edited, the evidence of what actually arrived is gone.
"""

from .sql import ENTITIES

__all__ = ["ENTITIES"]
