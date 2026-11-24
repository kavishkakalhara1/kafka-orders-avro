import logging
import random
import time
from datetime import UTC, datetime

from common.kafka import publish_confirmed
from consumer.errors import FailureKind, InvalidHeader, classify_error
from consumer.headers import header_value, replace_headers, retry_count

LOGGER = logging.getLogger(__name__)


def backoff_seconds(settings, attempt, random_value=None):
    if attempt < 1:
        raise ValueError("Retry attempts start at 1")
    exponent = min(attempt - 1, 60)
    base = min(settings.backoff_max, settings.backoff_base * 2 ** exponent)
    sample = random.random() if random_value is None else random_value
    return min(settings.backoff_max, base * (1 + settings.backoff_jitter * (2 * sample - 1)))


def origin_headers(message):
    headers = message.headers() or []
    origin = {
        "original-topic": message.topic(),
        "original-partition": message.partition(),
        "original-offset": message.offset(),
    }
    for name, value in origin.items():
        if header_value(headers, name) is None:
            headers = replace_headers(headers, **{name: value})
    return headers


class FailureRouter:
    def __init__(self, settings, producer):
        self.settings = settings
        self.producer = producer

    def route(self, message, error):
        kind = classify_error(error)
        if kind == FailureKind.FATAL:
            raise error
        try:
            attempt = retry_count(message.headers())
        except InvalidHeader as header_error:
            attempt, error, kind = 0, header_error, FailureKind.PERMANENT
        if kind == FailureKind.TRANSIENT and attempt < self.settings.max_retries:
            attempt += 1
            delay = backoff_seconds(self.settings, attempt)
            headers = replace_headers(origin_headers(message), **{
                "retry-count": attempt,
                "retry-at": time.time() + delay,
                "failure-reason": str(error),
                "exception-type": type(error).__name__,
            })
            publish_confirmed(
                self.producer, self.settings.retry_topic, message.key(), message.value(), headers
            )
            LOGGER.info("retry_scheduled", extra={"fields": {
                "retry_count": attempt, "delay_seconds": delay, "reason": str(error),
            }})
            return "retry"
        self.dead_letter(message, error, attempt)
        return "dlq"

    def dead_letter(self, message, error, attempt):
        headers = replace_headers(origin_headers(message), **{
            "failure-reason": str(error), "exception-type": type(error).__name__,
            "retry-count": attempt, "timestamp": datetime.now(UTC).isoformat(),
        })
        publish_confirmed(
            self.producer, self.settings.dlq_topic, message.key(), message.value(), headers
        )
        LOGGER.warning("dead_lettered", extra={"fields": {
            "retry_count": attempt, "reason": str(error), "exception_type": type(error).__name__,
        }})