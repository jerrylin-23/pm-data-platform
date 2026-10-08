import json
from pathlib import Path

import httpx
import pytest

from pmplatform.resolver import Resolver, parse_url, poly_markets
from pmplatform.subscriptions import Subscriptions

FORMAT_CASES = json.loads((Path(__file__).parent / "fixtures/polymarket-formats.json").read_text())["cases"]


@pytest.mark.parametrize("url", ["http://polymarket.com/event/a", "https://evil.com/event/a",
                                "https://polymarket.com.evil.com/event/a", "https://polymarket.com@evil.com/event/a",
                                "https://user@polymarket.com/event/a", "https://polymarket.com:123/event/a",
                                "https://polymarket.com/event/%2Fetc%2Fpasswd", "file:///etc/passwd"])
def test_reject_untrusted_urls(url):
    with pytest.raises(ValueError):
        parse_url(url)


def test_resolver_uses_official_api_and_rejects_closed():
    requests = []
    def handler(request):
        requests.append(str(request.url))
        return httpx.Response(200, json={"id": "1", "slug": "test", "active": True, "closed": False,
                                         "clobTokenIds": '["123", "456"]', "outcomes": '["Yes", "No"]'})
    resolver = Resolver(httpx.Client(transport=httpx.MockTransport(handler)))
    markets = resolver.resolve("https://polymarket.com/market/test")
    assert len(markets) == 2
    assert requests == ["https://gamma-api.polymarket.com/markets/slug/test"]


def test_standard_kalshi_event_url():
    assert parse_url("https://kalshi.com/markets/kxfeddecision/fed-decision/kxfeddecision-26oct") == (
        "kalshi", "event", "KXFEDDECISION-26OCT")


def test_polymarket_link_can_select_one_event_choice():
    assert parse_url("https://polymarket.com/event/tweet-count/tweet-count-220-239") == (
        "polymarket", "market", "tweet-count-220-239")


@pytest.mark.parametrize("labels", [["220-239", "240-259"], ["Lakers", "Celtics"],
                                    ["Below 2%", "At least 2%"]])
def test_event_choices_keep_parent_and_native_books(labels):
    event = {"id": "event-1", "slug": "choices", "title": "Which result?", "markets": [
        {"id": str(i), "slug": f"choice-{i}", "question": f"Will {label} win?",
         "groupItemTitle": label, "clobTokenIds": [str(100 + i * 2), str(101 + i * 2)],
         "outcomes": ["Yes", "No"]} for i, label in enumerate(labels)
    ]}
    event["markets"].append({"id": "closed", "closed": True})
    resolver = Resolver(httpx.Client(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json=event))))
    markets = resolver.resolve("https://polymarket.com/event/choices")
    assert len(markets) == 4
    assert {m["parent_event_id"] for m in markets} == {"event-1"}
    assert {m["event_title"] for m in markets} == {"Which result?"}
    assert {m["event_url"] for m in markets} == {"https://polymarket.com/event/choices"}
    assert [m["market_label"] for m in markets] == [labels[0], labels[0], labels[1], labels[1]]
    assert [m["outcome"] for m in markets] == ["yes", "no", "yes", "no"]


def test_named_binary_sides_map_to_native_direction():
    market = {"id": "1", "slug": "up-down", "clobTokenIds": ["123", "456"],
              "outcomes": ["Up", "Down"], "events": [{"id": "e", "title": "Price direction", "slug": "price"}]}
    resolver = Resolver(httpx.Client(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json=market))))
    markets = resolver.resolve("https://polymarket.com/market/up-down")
    assert [(m["instrument_id"], m["outcome"]) for m in markets] == [("123", "yes"), ("456", "no")]
    assert all(m["outcome_labels"] == {"yes": "Up", "no": "Down"} for m in markets)
    assert all(m["parent_event_id"] == "e" for m in markets)


def test_search_keeps_event_groups():
    event = {"id": "e", "slug": "choices", "title": "Event title", "markets": [
        {"id": "1", "slug": "choice", "groupItemTitle": "Candidate A",
         "clobTokenIds": ["123", "456"], "outcomes": ["No", "Yes"]}
    ]}
    resolver = Resolver(httpx.Client(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json={"events": [event]}))))
    markets = resolver.search("choices", "polymarket")["markets"]
    assert all(m["event_title"] == "Event title" and m["market_label"] == "Candidate A" for m in markets)
    assert all(m["source_url"] == "https://polymarket.com/event/choices" for m in markets)
    assert [m["outcome"] for m in markets] == ["no", "yes"]


def test_kalshi_event_keeps_choice_names():
    event = {"event_ticker": "E-26", "title": "Which team wins?", "markets": [
        {"ticker": "E-26-A", "status": "open", "title": "Team A wins?", "yes_sub_title": "Team A"},
        {"ticker": "E-26-B", "status": "open", "title": "Team B wins?", "yes_sub_title": "Team B"},
    ]}
    resolver = Resolver(httpx.Client(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json={"event": event}))))
    markets = resolver.resolve("https://kalshi.com/events/e-26")
    assert {m["parent_event_id"] for m in markets} == {"E-26"}
    assert {m["event_title"] for m in markets} == {"Which team wins?"}
    assert [m["market_label"] for m in markets] == ["Team A", "Team B"]


@pytest.mark.parametrize("case", FORMAT_CASES, ids=lambda case: case["format"])
def test_captured_polymarket_formats(case):
    event = case["event"]
    resolver = Resolver(httpx.Client(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json=event))))
    markets = resolver.resolve(f"https://polymarket.com/event/{event['slug']}")
    assert len(markets) == case["expected_open_markets"] * 2
    assert len({m["market_id"] for m in markets}) == case["expected_open_markets"]
    assert all(m["parent_event_id"] == str(event["id"]) for m in markets)
    originals = {str(m["id"]): m for m in event["markets"]}
    for record in markets:
        original = originals[record["market_id"]]
        assets, labels = json.loads(original["clobTokenIds"]), json.loads(original["outcomes"])
        assert record["outcome_labels"][record["outcome"]] == labels[assets.index(record["instrument_id"])]
        assert record["market_type"] == (original.get("sportsMarketType") or "markets")


def test_search_keeps_all_pairs_in_large_event():
    case = next(case for case in FORMAT_CASES if case["format"] == "named_choices")
    resolver = Resolver(httpx.Client(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json={"events": [case["event"]]}))))
    markets = resolver.search("nominee", "polymarket")["markets"]
    assert len(markets) == 106
    assert all(len([r for r in markets if r["market_id"] == m]) == 2 for m in {r["market_id"] for r in markets})


def test_sports_types_and_repeated_labels_use_venue_data():
    event = {"id": "e", "slug": "sports", "title": "Teams", "markets": [
        {"id": str(i), "slug": f"choice-{i}", "question": f"Team {i} total above 3?",
         "groupItemTitle": "O/U 3", "groupItemThreshold": 0, "sportsMarketType": "new_venue_market_type",
         "clobTokenIds": [str(100 + i * 2), str(101 + i * 2)], "outcomes": ["Over", "Under"]}
        for i in range(2)
    ]}
    markets = poly_markets(event["markets"], event=event)
    assert {m["market_type"] for m in markets} == {"new_venue_market_type"}
    assert {m["market_order"] for m in markets} == {0}
    assert {m["market_label"] for m in markets} == {"Team 0 total above 3?", "Team 1 total above 3?"}


@pytest.mark.parametrize("assets, labels", [(["123", "bad"], ["Yes", "No"]),
                                          (["123", "123"], ["Yes", "No"]),
                                          (["123", "456"], ["", "No"])])
def test_invalid_pair_is_not_returned_as_one_native_outcome(assets, labels):
    assert poly_markets([{"id": "1", "slug": "bad-pair", "clobTokenIds": assets, "outcomes": labels}]) == []


def test_subscription_limit_persistence_and_outbox(tmp_path):
    path = tmp_path / "subs.sqlite"
    store = Subscriptions(path, maximum=1)
    market = {"venue": "test", "market_id": "a", "instrument_id": "a-yes"}
    store.update([market, {**market, "instrument_id": "a-no"}])
    with pytest.raises(ValueError, match="limit"):
        store.update([{**market, "market_id": "b", "instrument_id": "b"}])
    assert len(store.list()) == 2
    store.close()
    store = Subscriptions(path, maximum=1)
    assert len(store.list()) == 2
    class Sink:
        def __init__(self):
            self.records = []
        def send(self, *args):
            self.records.append(args)
    sink = Sink()
    assert store.publish(sink) == 2
    assert store.publish(sink) == 0
    store.remove("a")
    assert store.list() == []
    store.close()


def test_retracking_replaces_old_instrument_ids_and_publishes_removal(tmp_path):
    store = Subscriptions(tmp_path / "subs.sqlite", maximum=1)
    base = {"venue": "polymarket", "market_id": "m"}
    store.update([{**base, "instrument_id": "old-yes"}, {**base, "instrument_id": "old-no"}])
    records = store.update([{**base, "instrument_id": "new-yes"}, {**base, "instrument_id": "new-no"}])
    assert {m["instrument_id"] for m in store.list()} == {"new-yes", "new-no"}
    assert len(records) == 2 and all(m["action"] == "track" for m in records)
    outbox = [json.loads(row[0]) for row in store.db.execute("SELECT payload FROM outbox ORDER BY id")]
    assert [(m["instrument_id"], m["action"]) for m in outbox[-4:]] == [
        ("old-no", "untrack"), ("old-yes", "untrack"), ("new-yes", "track"), ("new-no", "track")]
    store.close()
