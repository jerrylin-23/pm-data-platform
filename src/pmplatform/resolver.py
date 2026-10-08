import json
import re
from urllib.parse import unquote, urlsplit

import httpx

POLY_API = "https://gamma-api.polymarket.com"
KALSHI_API = "https://external-api.kalshi.com/trade-api/v2"
SLUG = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,199}\Z")
ASSET = re.compile(r"\d{1,100}\Z")


def parse_url(url):
    parsed = urlsplit(url.strip())
    if (parsed.scheme != "https" or parsed.hostname not in
            ("polymarket.com", "www.polymarket.com", "kalshi.com", "www.kalshi.com")
            or parsed.username or parsed.password or parsed.port not in (None, 443)):
        raise ValueError("Use an HTTPS link from polymarket.com or kalshi.com.")
    parts = [unquote(p) for p in parsed.path.split("/") if p]
    if len(parts) < 2:
        raise ValueError("Paste a market or event link.")
    venue = "polymarket" if "polymarket" in parsed.hostname else "kalshi"
    if venue == "polymarket":
        if parts[0] not in ("event", "market") or not SLUG.fullmatch(parts[1]):
            raise ValueError("Invalid Polymarket event or market slug.")
        if parts[0] == "event" and len(parts) == 3:
            if not SLUG.fullmatch(parts[2]):
                raise ValueError("Invalid Polymarket market slug.")
            return venue, "market", parts[2]
        return venue, parts[0], parts[1]
    if parts[0] not in ("markets", "events"):
        raise ValueError("Invalid Kalshi market link.")
    # Standard Kalshi links end in an event ticker; the earlier segments are presentation slugs.
    ticker = parts[-1].upper()
    if not SLUG.fullmatch(ticker):
        raise ValueError("Invalid Kalshi ticker.")
    return venue, "event" if parts[0] == "events" or len(parts) >= 3 else "market", ticker


def decode_array(value):
    return json.loads(value) if isinstance(value, str) else value or []


def poly_markets(markets, source_url=None, event=None):
    result, label_counts = [], {}
    for order, m in enumerate(markets):
        if m.get("closed") or not m.get("active", True) or m.get("enableOrderBook") is False:
            continue
        outcomes = decode_array(m.get("outcomes", '["Yes","No"]'))
        assets = decode_array(m.get("clobTokenIds")) or decode_array(m.get("positionIds"))
        prices = decode_array(m.get("outcomePrices"))
        if len(outcomes) != 2 or len(assets) != 2:
            continue
        labels = [str(label).strip() for label in outcomes]
        if any(not label for label in labels) or len(set(assets)) != 2 or not all(ASSET.fullmatch(str(a)) for a in assets):
            continue
        sides = [label.lower() for label in labels]
        if set(sides) != {"yes", "no"}:
            sides = ["yes", "no"]
        parent = event or next(iter(m.get("events") or []), {})
        event_id = str(parent.get("id", m["id"]))
        title = m.get("question", m.get("slug", str(m["id"])))
        market_type = m.get("sportsMarketType") or "markets"
        market_label = m.get("groupItemTitle") or title
        label_key = (event_id, market_type, market_label)
        label_counts[label_key] = label_counts.get(label_key, 0) + 1
        event_url = (f"https://polymarket.com/event/{parent['slug']}" if parent.get("slug") else
                     source_url or f"https://polymarket.com/market/{m['slug']}")
        for i, asset in enumerate(assets):
            result.append({"venue": "polymarket", "market_id": str(m["id"]), "instrument_id": str(asset),
                           "outcome": sides[i], "outcome_labels": dict(zip(sides, labels)), "title": title,
                           "parent_event_id": event_id, "event_title": parent.get("title", title),
                           "event_url": event_url, "market_label": market_label, "market_type": market_type,
                           "market_order": m["groupItemThreshold"] if m.get("groupItemThreshold") is not None else order,
                           "price": str(prices[i]) if i < len(prices) else None,
                           "volume_24h": str(m.get("volume24hr", "0")),
                           "source_url": source_url or event_url,
                           "tick_size": str(m.get("orderPriceMinTickSize", "0.01"))})
    for market in result:
        if label_counts[(market["parent_event_id"], market["market_type"], market["market_label"])] > 1:
            market["market_label"] = market["title"]
    return result


def kalshi_markets(markets, source_url=None, event=None):
    result = []
    for m in markets:
        if m.get("status") not in ("open", "active") or m.get("result") not in (None, ""):
            continue
        if not SLUG.fullmatch(m["ticker"]):
            raise ValueError("Invalid ticker in venue response.")
        parent = event or {}
        title = m.get("title", m["ticker"])
        event_id = parent.get("event_ticker", m.get("event_ticker", m["ticker"]))
        result.append({"venue": "kalshi", "market_id": m["ticker"], "instrument_id": m["ticker"],
                       "outcome": "yes", "outcome_labels": {"yes": "Yes", "no": "No"}, "title": title,
                       "parent_event_id": event_id, "event_title": parent.get("title", event_id),
                       "event_url": source_url or f"https://kalshi.com/events/{event_id}",
                       "market_label": m.get("yes_sub_title") or m.get("subtitle") or title,
                       "price": m.get("yes_bid_dollars"), "volume_24h": m.get("volume_24h_fp", "0"),
                       "source_url": source_url or f"https://kalshi.com/events/{event_id}",
                       "price_ranges": m.get("price_ranges", [])})
    return result


class Resolver:
    def __init__(self, client=None):
        self.client = client or httpx.Client(timeout=20, follow_redirects=False)

    def get(self, base, path, params=None):
        # Only constant API bases are used. No user URL is requested.
        response = self.client.get(base + path, params=params)
        response.raise_for_status()
        return response.json()

    def resolve(self, url):
        venue, kind, identifier = parse_url(url)
        if venue == "polymarket":
            resource = "events" if kind == "event" else "markets"
            data = self.get(POLY_API, f"/{resource}/slug/{identifier}")
            markets = data.get("markets", []) if kind == "event" else [data]
            result = poly_markets(markets, url, data if kind == "event" else None)
        elif kind == "event":
            data = self.get(KALSHI_API, f"/events/{identifier}", {"with_nested_markets": "true"})
            result = kalshi_markets(data.get("event", {}).get("markets", data.get("markets", [])), url,
                                    data.get("event"))
        else:
            try:
                data = self.get(KALSHI_API, f"/markets/{identifier}")
                result = kalshi_markets([data["market"]], url)
            except httpx.HTTPStatusError as error:
                if error.response.status_code != 404:
                    raise
                data = self.get(KALSHI_API, f"/events/{identifier}", {"with_nested_markets": "true"})
                result = kalshi_markets(data.get("event", {}).get("markets", data.get("markets", [])), url,
                                        data.get("event"))
        if not result:
            raise ValueError("This link has no open markets with order book data.")
        return result

    def search(self, keyword, venue=None):
        keyword = keyword.strip()
        if not keyword or len(keyword) > 120:
            raise ValueError("Enter a search term of 1 to 120 characters.")
        results, errors = [], []
        if venue in (None, "polymarket"):
            try:
                data = self.get(POLY_API, "/public-search", {"q": keyword, "events_status": "active",
                                                          "limit_per_type": 10})
                for event in data.get("events", []):
                    markets = event.get("markets")
                    if not markets:
                        event = self.get(POLY_API, f"/events/{event['id']}")
                        markets = event.get("markets", [])
                    results.extend(poly_markets(markets, event=event))
            except (httpx.HTTPError, ValueError, KeyError) as error:
                errors.append({"venue": "polymarket", "error": type(error).__name__})
        if venue in (None, "kalshi"):
            try:
                cursor = None
                for _ in range(5):
                    params = {"status": "open", "limit": 200, "with_nested_markets": "true"}
                    if cursor:
                        params["cursor"] = cursor
                    data = self.get(KALSHI_API, "/events", params)
                    for event in data.get("events", []):
                        markets = event.get("markets", [])
                        event_matches = keyword.lower() in (event.get("title", "") + " " + event["event_ticker"]).lower()
                        matches = markets if event_matches else [m for m in markets if keyword.lower() in
                                                                 (m.get("title", "") + " " + m["ticker"]).lower()]
                        results.extend(kalshi_markets(matches, event=event))
                    cursor = data.get("cursor")
                    if not cursor:
                        break
            except (httpx.HTTPError, ValueError, KeyError) as error:
                errors.append({"venue": "kalshi", "error": type(error).__name__})
        unique = {f"{r['venue']}:{r['instrument_id']}": r for r in results}
        return {"markets": list(unique.values()), "errors": errors,
                "note": "Kalshi search filters the first 1,000 open events. Use a direct link for other events."}
