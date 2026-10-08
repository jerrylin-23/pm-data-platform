import base64
import json
import re
from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

I64_MAX = (1 << 63) - 1
I64_MIN = -(1 << 63)
DECIMAL = re.compile(r"([+-]?)(\d*)(?:\.(\d+))?\Z")


def scaled(value: str | int, decimals: int) -> int:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError("decimal_string_required")
    match = DECIMAL.fullmatch(str(value))
    if not match or not any(match.groups()[1:]):
        raise ValueError("invalid_decimal")
    sign, whole, fraction = match.groups()
    fraction = (fraction or "").rstrip("0")
    if len(fraction) > decimals:
        raise ValueError("unsupported_precision")
    result = int(whole or "0") * 10**decimals + int(fraction.ljust(decimals, "0") or "0")
    result = -result if sign == "-" else result
    if not I64_MIN <= result <= I64_MAX:
        raise ValueError("integer_overflow")
    return result


def decimal_text(value: int, decimals: int) -> str:
    sign = "-" if value < 0 else ""
    value = abs(value)
    return f"{sign}{value // 10**decimals}.{value % 10**decimals:0{decimals}d}"


def iso_ns(ns: int) -> str:
    seconds, nano = divmod(ns, 1_000_000_000)
    return datetime.fromtimestamp(seconds, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + f".{nano:09d}Z"


def parse_ns(value: str) -> int:
    if value.isdigit():
        return int(value)
    match = re.fullmatch(r"(.+?)(?:\.(\d{1,9}))?Z", value)
    if not match:
        raise ValueError("use_UTC_ISO_timestamp_or_integer_nanoseconds")
    seconds, fraction = match.groups()
    dt = datetime.fromisoformat(seconds).replace(tzinfo=timezone.utc)
    return int(dt.timestamp()) * 1_000_000_000 + int((fraction or "").ljust(9, "0"))


def canonical(value: dict | list) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class Level(StrictModel):
    side: Literal["bid", "ask"]
    price_e4: int = Field(ge=0, le=10_000)
    size_e6: int = Field(ge=I64_MIN, le=I64_MAX)
    operation: Literal["SET_SIZE", "ADD_SIZE"] = "SET_SIZE"

    @model_validator(mode="after")
    def check_size(self):
        if self.operation == "SET_SIZE" and self.size_e6 < 0:
            raise ValueError("negative_size")
        return self


class Event(StrictModel):
    schema_version: Literal[1] = 1
    venue: str
    data_origin: Literal["live", "synthetic"] = "live"
    market_id: str
    instrument_id: str
    outcome: Literal["yes", "no"] = "yes"
    event_kind: Literal["snapshot", "delta", "trade", "status", "control"]
    source_topic: str
    source_partition: int = Field(ge=0)
    source_offset: int = Field(ge=0)
    subevent_index: int = Field(ge=0)
    connection_epoch: int = Field(ge=0)
    connection_counter: int = Field(ge=0)
    subscription_id: str = ""
    venue_seq: int | None = None
    venue_ts_ns: int | None = None
    recv_ts_ns: int = Field(ge=0)
    continuity: Literal["checked", "unverified", "broken"] = "unverified"
    levels: list[Level] = Field(default_factory=list)
    trade_price_e4: int | None = Field(default=None, ge=0, le=10_000)
    trade_size_e6: int | None = Field(default=None, ge=0, le=I64_MAX)
    status: str | None = None
    expected_bid_e4: int | None = Field(default=None, ge=0, le=10_000)
    expected_ask_e4: int | None = Field(default=None, ge=0, le=10_000)

    @property
    def key(self) -> str:
        return f"{self.venue}:{self.instrument_id}"

    @property
    def event_id(self) -> str:
        return f"{self.source_topic}:{self.source_partition}:{self.source_offset}:{self.subevent_index}"

    @model_validator(mode="after")
    def shape(self):
        if self.event_kind != "snapshot" and self.event_kind != "delta" and self.levels:
            raise ValueError("levels_only_on_book_events")
        if self.event_kind == "snapshot" and any(level.operation != "SET_SIZE" for level in self.levels):
            raise ValueError("snapshot_requires_set")
        if self.event_kind == "trade" and self.trade_price_e4 is None:
            raise ValueError("trade_price_required")
        if self.event_kind in ("control", "status") and self.status is None:
            raise ValueError("status_required")
        return self


class RawRecord(StrictModel):
    venue: Literal["polymarket", "kalshi"]
    data_origin: Literal["live", "synthetic"] = "live"
    topic: str
    partition: int = Field(ge=0)
    offset: int = Field(ge=0)
    recv_ts_ns: int = Field(ge=0)
    connection_epoch: int = Field(ge=0)
    connection_id: str
    counter: int = Field(ge=0)
    payload_b64: str
    instruments: dict[str, dict] = Field(default_factory=dict)

    @property
    def payload(self) -> bytes:
        return base64.b64decode(self.payload_b64, validate=True)

    @classmethod
    def capture(cls, payload: bytes, **fields):
        return cls(payload_b64=base64.b64encode(payload).decode("ascii"), **fields)
