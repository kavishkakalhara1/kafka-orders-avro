import logging
import math
import time

from confluent_kafka import KafkaException, TopicPartition

from common.config import Settings
from common.kafka import make_consumer, make_producer, publish_confirmed
from common.runtime import configure_logging, shutdown_event
from consumer.errors import InvalidHeader
from consumer.headers import header_value, retry_count
from consumer.retry import FailureRouter

LOGGER = logging.getLogger(__name__)


def retry_deadline(headers):
    try:
        value = header_value(headers, "retry-at")
        if value is None:
            raise ValueError("missing retry-at")
        deadline = float(value)
        if not math.isfinite(deadline) or deadline < 0:
            raise ValueError("retry-at must be a finite nonnegative timestamp")
        return deadline
    except (ValueError, TypeError) as error:
        raise InvalidHeader("Invalid retry-at header") from error


def run_retry_worker(settings, stopped):
    producer = make_producer(settings)
    consumer = make_consumer(settings, settings.retry_group)
    router = FailureRouter(settings, producer)
    pending = {}

    def revoked(client, partitions):
        for partition in partitions:
            pending.pop((partition.topic, partition.partition), None)

    consumer.subscribe([settings.retry_topic], on_revoke=revoked, on_lost=revoked)
    try:
        while not stopped.is_set():
            for key, (deadline, message) in list(pending.items()):
                if stopped.is_set():
                    break
                if deadline <= time.time():
                    publish_confirmed(
                        producer, settings.orders_topic, message.key(), message.value(),
                        message.headers() or [],
                    )
                    # Forward acknowledgement precedes commit, so a crash can duplicate, not lose.
                    consumer.commit(message=message, asynchronous=False)
                    del pending[key]
                    consumer.resume([TopicPartition(*key)])
                    LOGGER.info("retry_released", extra={"fields": {
                        "retry_count": retry_count(message.headers()),
                        "partition": message.partition(), "offset": message.offset(),
                    }})
            message = consumer.poll(settings.poll_seconds)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            try:
                deadline = retry_deadline(message.headers())
                retry_count(message.headers())
            except InvalidHeader as error:
                router.route(message, error)
                consumer.commit(message=message, asynchronous=False)
                continue
            key = (message.topic(), message.partition())
            consumer.pause([TopicPartition(*key)])
            pending[key] = (deadline, message)
    finally:
        try:
            producer.flush(settings.delivery_timeout)
        finally:
            # Paused records stay uncommitted and are fetched again after restart/rebalance.
            consumer.close()
        LOGGER.info("retry_worker_stopped", extra={"fields": {"pending": len(pending)}})


def main():
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    try:
        run_retry_worker(settings, shutdown_event())
    except Exception:
        LOGGER.exception("retry_worker_failed_uncommitted")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()