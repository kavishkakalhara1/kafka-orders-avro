import logging

from confluent_kafka import KafkaException

from common.config import Settings
from common.kafka import OrderCodec, make_consumer, make_producer
from common.runtime import configure_logging, shutdown_event
from consumer.processing import OrderProcessor
from consumer.retry import FailureRouter

LOGGER = logging.getLogger(__name__)


def run_consumer(settings, stopped, processor=None):
    processor = processor or OrderProcessor(OrderCodec(settings))
    producer = make_producer(settings)
    consumer = make_consumer(settings, settings.consumer_group)
    router = FailureRouter(settings, producer)
    consumer.subscribe([settings.orders_topic])
    try:
        while not stopped.is_set():
            message = consumer.poll(settings.poll_seconds)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            try:
                processor.process(message)
            except Exception as error:
                router.route(message, error)
            # Commit only after success or an acknowledged handoff. A crash between
            # those operations replays the record: at-least-once, never silent loss.
            consumer.commit(message=message, asynchronous=False)
    finally:
        try:
            producer.flush(settings.delivery_timeout)
        finally:
            # Completed records are already committed; never commit a failed/prefetched record.
            consumer.close()
        LOGGER.info("consumer_stopped")
    return processor.averages.snapshot()


def main():
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    try:
        run_consumer(settings, shutdown_event())
    except Exception:
        LOGGER.exception("consumer_failed_uncommitted")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()