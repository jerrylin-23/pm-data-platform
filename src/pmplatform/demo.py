from .gaps import Pair
from .model import RawRecord, canonical, decimal_text

DEMO_PAIR = Pair("Sample rate decision, synthetic data", "polymarket:demo-yes", "kalshi:DEMO-RATE",
                 fee_e4=50, threshold_e4=100)


def frame_pair(index, recv_ts_ns, epoch=1):
    movement = ((index % 20) - 10) * 10
    poly_bid, poly_ask = 4800 + movement, 5100 + movement
    kal_bid, kal_ask = 5500 - movement, 5700 - movement
    poly = {"event_type": "book", "asset_id": "demo-yes", "timestamp": str(recv_ts_ns // 1_000_000),
            "bids": [{"price": decimal_text(poly_bid - i * 100, 4), "size": str(30 + i * 13)} for i in range(10)],
            "asks": [{"price": decimal_text(poly_ask + i * 100, 4), "size": str(25 + i * 11)} for i in range(10)]}
    kalshi = {"type": "orderbook_snapshot", "sid": 1, "seq": index + 1,
              "msg": {"market_ticker": "DEMO-RATE", "ts_ms": recv_ts_ns // 1_000_000,
                      "yes_dollars_fp": [[decimal_text(kal_bid - i * 100, 4), str(40 + i * 17)] for i in range(10)],
                      "no_dollars_fp": [[decimal_text(10_000 - kal_ask - i * 100, 4), str(35 + i * 19)] for i in range(10)]}}
    for offset, venue, instrument, message in ((index * 2, "polymarket", "demo-yes", poly),
                                              (index * 2 + 1, "kalshi", "DEMO-RATE", kalshi)):
        yield RawRecord.capture(canonical(message), venue=venue, data_origin="synthetic", topic=f"raw.{venue}",
                                partition=0, offset=offset, recv_ts_ns=recv_ts_ns, connection_epoch=epoch,
                                connection_id=f"demo-{venue}-{epoch}", counter=index,
                                instruments={instrument: {"market_id": "demo-rate", "outcome": "yes"}})


def fixture(seconds=600):
    start = 1_791_374_400_000_000_000
    for index in range(seconds):
        yield from frame_pair(index, start + index * 1_000_000_000)
