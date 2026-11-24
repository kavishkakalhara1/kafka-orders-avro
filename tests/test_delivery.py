from threading import Event
from unittest.mock import Mock

import pytest

from common.config import Settings
from consumer.errors import InvalidOrder, TransientFailure
from consumer.headers import header_value, retry_count
from consumer.main import run_consumer
from consumer.retry import FailureRouter


def message(headers=None):
    record = Mock()
    record.topic.return_value = "orders"
    record.partition.return_value = 2
    record.offset.return_value = 42
    record.key.return_value = b"1001"
    record.value.return_value = b"original-avro-bytes"
    record.headers.return_value = headers or []
    record.error.return_value = None
    return record


@pytest.mark.parametrize(("error", "attempt", "destination"), [
    (TransientFailure("timeout"), 0, "orders.retry"),
    (TransientFailure("timeout"), 2, "orders.retry"),
    (TransientFailure("timeout"), 3, "orders.dlq"),
    (InvalidOrder("negative"), 0, "orders.dlq"),
])
def test_routing_preserves_bytes_and_origin(monkeypatch, error, attempt, destination):
    publish = Mock()
    monkeypatch.setattr("consumer.retry.publish_confirmed", publish)
    record = message([("retry-count", str(attempt).encode())])
    FailureRouter(Settings(), Mock()).route(record, error)
    _, topic, key, payload, headers = publish.call_args.args
    assert (topic, key, payload) == (destination, b"1001", record.value())
    assert header_value(headers, "original-topic") == b"orders"
    assert header_value(headers, "original-partition") == b"2"
    assert header_value(headers, "original-offset") == b"42"
    assert retry_count(headers) == (attempt + 1 if destination == "orders.retry" else attempt)
    if destination == "orders.dlq":
        assert header_value(headers, "timestamp")
        assert header_value(headers, "exception-type") == type(error).__name__.encode()
        assert header_value(headers, "failure-reason") == str(error).encode()


def test_origin_survives_multiple_hops(monkeypatch):
    publish = Mock()
    monkeypatch.setattr("consumer.retry.publish_confirmed", publish)
    original = [("original-topic", b"orders"), ("original-offset", b"5"),
                ("original-partition", b"1"), ("retry-count", b"3")]
    FailureRouter(Settings(), Mock()).route(message(original), TransientFailure("timeout"))
    headers = publish.call_args.args[-1]
    assert header_value(headers, "original-offset") == b"5"
    assert header_value(headers, "original-partition") == b"1"


@pytest.mark.parametrize("processing_error", [None, InvalidOrder("bad")])
def test_commit_after_success_or_confirmed_handoff(monkeypatch, processing_error):
    stopped = Event()
    record = message()
    consumer, producer, processor = Mock(), Mock(), Mock()
    consumer.poll.return_value = record
    processor.process.side_effect = processing_error
    events = []

    def commit(**kwargs):
        events.append("commit")
        stopped.set()

    consumer.commit.side_effect = commit
    monkeypatch.setattr("consumer.main.make_consumer", lambda *_: consumer)
    monkeypatch.setattr("consumer.main.make_producer", lambda *_: producer)
    monkeypatch.setattr("consumer.retry.publish_confirmed", lambda *_: events.append("delivered"))
    run_consumer(Settings(), stopped, processor)
    assert events == (["delivered", "commit"] if processing_error else ["commit"])
    consumer.commit.assert_called_once_with(message=record, asynchronous=False)
    consumer.close.assert_called_once()
    producer.flush.assert_called_once()


def test_failed_handoff_never_commits(monkeypatch):
    consumer, producer, processor = Mock(), Mock(), Mock()
    consumer.poll.return_value = message()
    processor.process.side_effect = InvalidOrder("bad")
    monkeypatch.setattr("consumer.main.make_consumer", lambda *_: consumer)
    monkeypatch.setattr("consumer.main.make_producer", lambda *_: producer)
    monkeypatch.setattr(
        "consumer.retry.publish_confirmed", Mock(side_effect=TimeoutError("offline"))
    )
    with pytest.raises(TimeoutError):
        run_consumer(Settings(), Event(), processor)
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()