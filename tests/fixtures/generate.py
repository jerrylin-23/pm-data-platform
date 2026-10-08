"""Create synthetic protocol fixtures. Live capture is kept as a separate file."""
import gzip
import hashlib
from pathlib import Path

from pmplatform.demo import fixture
from pmplatform.model import RawRecord, canonical
from pmplatform.replay import normalized_raw, replay


def save(path, records):
    encoded = b''.join(canonical(raw.model_dump(mode='json')) + b'\n' for raw in records)
    path.write_bytes(gzip.compress(encoded, mtime=0))
    result = replay(list(normalized_raw(records)))
    return {'file': path.name, 'raw_frames': len(records), 'data_origin': records[0].data_origin,
            'duration_ns': max(r.recv_ts_ns for r in records) - min(r.recv_ts_ns for r in records),
            'compressed_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            'book_state_sha256': result.sha256, 'violations': len(result.violations)}


if __name__ == '__main__':
    root = Path(__file__).parent
    records = list(fixture(600))
    results = [save(root / f'{venue}-synthetic-10min.jsonl.gz', [r for r in records if r.venue == venue])
               for venue in ('polymarket', 'kalshi')]
    # A generated Kalshi delta fixture has signed quantities and periodic snapshots.
    changes = []
    for index in range(600):
        ts = 1791374400000000000 + index * 1000000000
        if index % 60 == 0:
            kind = 'orderbook_snapshot'
            data = {'market_ticker': 'TEST-MARKET', 'yes_dollars_fp': [['0.4', '100.00']],
                    'no_dollars_fp': [['0.3', '100.00']]}
        else:
            kind = 'orderbook_delta'
            data = {'market_ticker': 'TEST-MARKET', 'side': 'yes', 'price_dollars': '0.4',
                    'delta_fp': '1.00' if index % 2 else '-1.00'}
        data['ts_ms'] = ts // 1000000
        changes.append(RawRecord.capture(canonical({'type': kind, 'sid': 1, 'seq': index + 1, 'msg': data}),
                                         venue='kalshi', data_origin='synthetic', topic='raw.kalshi', partition=0,
                                         offset=index, recv_ts_ns=ts, connection_id='fixture', connection_epoch=1,
                                         counter=index, instruments={'TEST-MARKET': {'market_id': 'TEST-MARKET'}}))
    results.append(save(root / 'kalshi-deltas-synthetic-10min.jsonl.gz', changes))
    (root / 'synthetic-manifest.json').write_bytes(canonical({'schema_version': 1, 'fixtures': results}) + b'\n')
