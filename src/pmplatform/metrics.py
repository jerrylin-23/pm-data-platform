from prometheus_client import Counter, Gauge, Histogram, start_http_server

MESSAGES = Counter("pm_messages_total", "Accepted source messages", ["venue"])
VIOLATIONS = Counter("pm_dq_violations_total", "Data quality failures", ["reason"])
RESYNCS = Counter("pm_resync_total", "Snapshot recovery requests", ["venue"])
BOOK_GAPS = Counter("pm_book_gaps_total", "Books invalidated by sequence failures", ["venue"])
LAG = Gauge("pm_consumer_lag", "Broker high watermark less committed input offset", ["group", "topic", "partition"])
ALERT_LATENCY = Histogram("pm_alert_latency_seconds", "Local receive to alert publication",
                          buckets=(.001, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5, 10))


def serve_metrics(port):
    if port:
        start_http_server(port, addr="0.0.0.0")
