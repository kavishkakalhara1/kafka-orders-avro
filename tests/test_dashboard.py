import json
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from threading import Thread
from unittest.mock import Mock

import pytest

from common.config import Settings
from dashboard.server import Dashboard, handler_for, producer_options


@pytest.mark.parametrize("data", [
    {"mode": "invalid"}, {"count": -1}, {"count": True}, {"count": 1001},
    {"rate": float("nan")}, {"rate": 0}, {"failures": 11},
])
def test_producer_options_reject_invalid_input(data):
    with pytest.raises(ValueError):
        producer_options(data)


def test_dashboard_isolates_topics_and_reads_real_aggregates(monkeypatch):
    monkeypatch.setenv("DASHBOARD_TOPIC_PREFIX", "test.demo")
    dashboard = Dashboard(Settings())
    assert dashboard.settings.orders_topic == "test.demo"
    assert dashboard.settings.consumer_group != Settings().consumer_group
    averages = {"global": {"count": 2, "sum": 30, "average": 15}, "products": {}}
    dashboard.ingest("consumer", {"event": "averages_updated", "averages": averages})
    state = dashboard.snapshot()
    assert state["averages"] == averages
    assert state["trend"] == [15]
    assert state["counts"]["averages_updated"] == 1
    assert state["workers"]["consumer"]["running"] is False


def test_dashboard_preserves_corrupt_record_and_metadata():
    dashboard = Dashboard(Settings())
    codec, message = Mock(), Mock()
    codec.decode.side_effect = ValueError("bad avro")
    message.value.return_value = b"bad payload"
    message.key.return_value = b"1001"
    message.topic.return_value = dashboard.settings.dlq_topic
    message.partition.return_value = 1
    message.offset.return_value = 3
    message.headers.return_value = [("failure-reason", b"invalid")]
    message.timestamp.return_value = (1, 1000)
    dashboard.record_message(codec, message)
    record = dashboard.snapshot()["records"][0]
    assert record["payload"] == "bad payload"
    assert record["base64"] == "YmFkIHBheWxvYWQ="
    assert record["headers"]["failure-reason"] == "invalid"


def test_dashboard_cannot_launch_before_services_ready():
    dashboard = Dashboard(Settings())
    with pytest.raises(ValueError, match="ready"):
        dashboard.launch("consumer")


def test_dashboard_reports_committed_offsets_and_lag():
    dashboard = Dashboard(Settings())
    consumer = Mock()
    topic = Mock()
    topic.partitions = {0: Mock(), 1: Mock()}
    consumer.list_topics.return_value.topics = {dashboard.settings.orders_topic: topic}
    consumer.committed.return_value = [Mock(partition=0, offset=8), Mock(partition=1, offset=-1001)]
    consumer.get_watermark_offsets.side_effect = [(0, 10), (3, 5)]
    dashboard.read_offsets(consumer)
    assert dashboard.snapshot()["offsets"] == [
        {"partition": 0, "committed": 8, "end": 10, "lag": 2},
        {"partition": 1, "committed": None, "end": 5, "lag": 2},
    ]


@pytest.fixture
def dashboard_http():
    dashboard = Dashboard(Settings())
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_for(dashboard))
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    connection = HTTPConnection("127.0.0.1", server.server_port, timeout=5)
    try:
        yield dashboard, connection
    finally:
        connection.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_export_is_filtered_json_attachment(dashboard_http):
    dashboard, connection = dashboard_http
    dashboard.records.extend([
        {"topic": dashboard.settings.orders_topic, "key": "normal"},
        {"topic": dashboard.settings.dlq_topic, "key": "failed", "payload": "bad bytes"},
    ])
    connection.request("GET", "/api/export?topic=dlq&search=failed")
    response = connection.getresponse()
    assert response.status == 200
    assert response.getheader("Content-Disposition") == 'attachment; filename="orderflow-dlq.json"'
    assert json.loads(response.read()) == [dashboard.records[-1]]


def test_cross_origin_commands_are_rejected(dashboard_http):
    dashboard, connection = dashboard_http
    dashboard.launch = Mock()
    connection.request("POST", "/api/produce", json.dumps({"count": 1}), {
        "Content-Type": "application/json", "Origin": "https://untrusted.example",
    })
    response = connection.getresponse()
    assert response.status == 403
    response.read()
    dashboard.launch.assert_not_called()