from dataclasses import replace

import pytest
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import SerializationError

from common.config import Settings
from consumer.averages import RunningAverages
from consumer.errors import (
    FailureKind,
    InvalidHeader,
    InvalidOrder,
    TransientFailure,
    classify_error,
)
from consumer.headers import header_value, replace_headers, retry_count
from consumer.retry import backoff_seconds
from consumer.retry_worker import retry_deadline


def test_running_averages():
    averages = RunningAverages()
    assert averages.snapshot()["global"] == {"count": 0, "sum": 0.0, "average": 0.0}
    for order_id, product, price in [("1", "Item1", 10), ("2", "Item2", 20), ("3", "Item1", 30)]:
        result = averages.add({"orderId": order_id, "product": product, "price": price})
    assert result["global"] == {"count": 3, "sum": 60, "average": 20}
    assert result["products"]["Item1"] == {"count": 2, "sum": 40, "average": 20}
    assert result["products"]["Item2"]["count"] == 1
    assert len(averages.products) == 2


@pytest.mark.parametrize("bad", [
    None, {}, {"orderId": "1", "price": 5},
    {"orderId": "1", "product": "", "price": 5},
    *[{"orderId": "1", "product": "Item1", "price": price}
      for price in (-1, float("nan"), float("inf"), "10", True)],
])
def test_invalid_orders_do_not_change_averages(bad):
    averages = RunningAverages()
    with pytest.raises(InvalidOrder):
        averages.add(bad)
    assert averages.global_stats.count == 0
    assert not averages.products


@pytest.mark.parametrize(("error", "expected"), [
    (TimeoutError(), FailureKind.TRANSIENT),
    (ConnectionError(), FailureKind.TRANSIENT),
    (TransientFailure("503"), FailureKind.TRANSIENT),
    (SchemaRegistryError(503, 50001, "unavailable"), FailureKind.TRANSIENT),
    (SchemaRegistryError(429, 42901, "rate limited"), FailureKind.TRANSIENT),
    (SchemaRegistryError(404, 40403, "unknown schema"), FailureKind.PERMANENT),
    (SchemaRegistryError(401, 40101, "unauthorized"), FailureKind.FATAL),
    (InvalidOrder("negative price"), FailureKind.PERMANENT),
    (InvalidHeader("bad count"), FailureKind.PERMANENT),
    (SerializationError("bad bytes"), FailureKind.PERMANENT),
    (RuntimeError("programming bug"), FailureKind.FATAL),
])
def test_error_classifier(error, expected):
    assert classify_error(error) == expected


def test_retry_header_replaces_duplicates_preserves_other_headers():
    assert retry_count(None) == 0
    headers = [("original-offset", b"12"), ("retry-count", b"1"), ("retry-count", b"2")]
    assert retry_count(headers) == 2
    updated = replace_headers(headers, **{"retry-count": retry_count(headers) + 1})
    assert retry_count(updated) == 3
    assert sum(name == "retry-count" for name, value in updated) == 1
    assert header_value(updated, "original-offset") == b"12"
    assert headers[-1][1] == b"2"


@pytest.mark.parametrize("value", [None, b"", b"-1", b"1.5", b"oops", b"\xff", b"1000001"])
def test_invalid_retry_headers(value):
    with pytest.raises(InvalidHeader):
        retry_count([("retry-count", value)])


def test_exponential_backoff_and_jitter():
    settings = Settings()
    assert [backoff_seconds(settings, attempt, 0.5) for attempt in (1, 2, 3)] == [1, 2, 4]
    assert backoff_seconds(settings, 2, 0) == 1.5
    assert backoff_seconds(settings, 2, 1) == 2.5
    assert backoff_seconds(settings, 1000000, 1) == settings.backoff_max
    with pytest.raises(ValueError):
        backoff_seconds(settings, 0)


@pytest.mark.parametrize("value", [None, b"nan", b"inf", b"-1", b"broken"])
def test_bad_deadline(value):
    with pytest.raises(InvalidHeader):
        retry_deadline([("retry-at", value)])


@pytest.mark.parametrize("updates", [
    {"produce_rate": 0}, {"backoff_base": float("nan")}, {"max_retries": -1},
    {"backoff_jitter": 2}, {"delivery_timeout": 120}, {"retry_topic": "orders"},
])
def test_invalid_settings(updates):
    with pytest.raises(ValueError):
        replace(Settings(), **updates).validate()