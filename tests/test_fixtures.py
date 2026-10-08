import gzip
import hashlib
import json
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from pmplatform.model import RawRecord
from pmplatform.replay import normalized_raw, replay

ROOT = Path(__file__).parent / 'fixtures'
FIXTURES = [entry for name in ('synthetic-manifest.json', 'live-manifest.json')
            for entry in json.loads((ROOT / name).read_text())['fixtures']]


def load(name):
    return [RawRecord.model_validate_json(line) for line in gzip.decompress((ROOT / name).read_bytes()).splitlines()]


@pytest.mark.parametrize('entry', FIXTURES, ids=lambda entry: entry['file'])
def test_golden_replay(entry):
    path = ROOT / entry['file']
    assert hashlib.sha256(path.read_bytes()).hexdigest() == entry['compressed_sha256']
    events = list(normalized_raw(load(entry['file'])))
    first, second = replay(events), replay(events)
    assert first.output == second.output
    assert first.sha256 == entry['book_state_sha256']
    assert len(first.violations) == entry['violations']


@given(st.integers(min_value=1, max_value=58))
@settings(max_examples=58)
def test_dropped_delta_always_invalidates_until_snapshot(drop):
    records = load('kalshi-deltas-synthetic-10min.jsonl.gz')[:61]
    events = list(normalized_raw([raw for i, raw in enumerate(records) if i != drop]))
    result = replay(events)
    assert any(v['reason'] == 'sequence_gap' for v in result.violations)
    # A delta after the missing message cannot modify the last accepted level.
    held = next(s for s in reversed(result.states) if s['version'] == drop)
    affected = [s for s in result.states if drop < int(s['event_id'].split(':')[-2]) < 60]
    assert affected
    assert all(s['status'] == 'stale' and s['bids'] == held['bids'] for s in affected)
    assert result.states[-1]['status'] == 'live'


def test_parser_failure_uses_same_control_in_raw_replay():
    raw = load('polymarket-synthetic-10min.jsonl.gz')[0]
    malformed = RawRecord.capture(b'{broken', **{k: v for k, v in raw.model_dump().items()
                                                if k not in ('payload_b64', 'schema_version')})
    result = replay(list(normalized_raw([malformed])))
    assert result.states[0]['status'] == 'stale'
    assert result.violations[0]['reason'] == 'parser_error'
