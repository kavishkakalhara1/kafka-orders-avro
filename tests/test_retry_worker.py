import time
from threading import Event
from unittest.mock import Mock

import pytest
from confluent_kafka import TopicPartition

from common.config import Settings
from consumer.retry_worker import run_retry_worker


@pytest.mark.parametrize("revoked", [False, True])
def test_pending_retry_is_not_committed_on_shutdown_or_revocation(monkeypatch, revoked):
    stopped = Event()
    consumer, producer, publish = Mock(), Mock(), Mock()
    record = Mock()
    record.topic.return_value = "orders.retry"
    record.partition.return_value = 0
    record.error.return_value = None
    record.headers.return_value = [
        ("retry-count", b"1"), ("retry-at", str(time.time() + 60).encode())
    ]
    calls = 0

    def poll(timeout):
        nonlocal calls
        calls += 1
        if calls == 1:
            return record
        if revoked:
            callback = consumer.subscribe.call_args.kwargs["on_revoke"]
            callback(consumer, [TopicPartition("orders.retry", 0)])
        stopped.set()
        return None

    consumer.poll.side_effect = poll
    monkeypatch.setattr("consumer.retry_worker.make_consumer", lambda *_: consumer)
    monkeypatch.setattr("consumer.retry_worker.make_producer", lambda *_: producer)
    monkeypatch.setattr("consumer.retry_worker.publish_confirmed", publish)
    run_retry_worker(Settings(), stopped)
    consumer.pause.assert_called_once()
    consumer.commit.assert_not_called()
    publish.assert_not_called()
    consumer.close.assert_called_once()
    producer.flush.assert_called_once()


def test_due_retry_is_forwarded_before_commit(monkeypatch):
    stopped = Event()
    consumer, producer = Mock(), Mock()
    record = Mock()
    record.topic.return_value = "orders.retry"
    record.partition.return_value = 0
    record.error.return_value = None
    record.headers.return_value = [("retry-count", b"2"), ("retry-at", b"0")]
    consumer.poll.side_effect = [record, None]
    actions = []

    def commit(**kwargs):
        actions.append("commit")
        stopped.set()

    consumer.commit.side_effect = commit
    monkeypatch.setattr("consumer.retry_worker.make_consumer", lambda *_: consumer)
    monkeypatch.setattr("consumer.retry_worker.make_producer", lambda *_: producer)
    monkeypatch.setattr(
        "consumer.retry_worker.publish_confirmed", lambda *_: actions.append("forward")
    )
    run_retry_worker(Settings(), stopped)
    assert actions == ["forward", "commit"]
    consumer.commit.assert_called_once_with(message=record, asynchronous=False)
    consumer.resume.assert_called_once()