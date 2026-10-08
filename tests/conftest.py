import pytest

from pmplatform.model import Event, Level


@pytest.fixture
def event():
    def make(kind="snapshot", offset=0, epoch=1, levels=None, **extra):
        return Event(venue="test", market_id="m", instrument_id="i", event_kind=kind,
                     source_topic="raw.test", source_partition=0, source_offset=offset,
                     subevent_index=0, connection_epoch=epoch, connection_counter=offset,
                     recv_ts_ns=1_000_000_000 + offset * 1_000_000,
                     levels=levels if levels is not None else [
                         Level(side="bid", price_e4=4000, size_e6=100_000_000),
                         Level(side="ask", price_e4=6000, size_e6=200_000_000)], **extra)
    return make
