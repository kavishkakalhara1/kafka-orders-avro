import logging
import struct

from confluent_kafka.serialization import SerializationError

from consumer.averages import RunningAverages, validate_order
from consumer.errors import InvalidOrder, TransientFailure
from consumer.headers import integer_header, retry_count

LOGGER = logging.getLogger(__name__)


class OrderProcessor:
    def __init__(self, codec):
        self.codec = codec
        self.averages = RunningAverages()

    def process(self, message):
        try:
            order = self.codec.decode(message.topic(), message.value())
        except (SerializationError, ValueError, EOFError, IndexError, struct.error) as error:
            raise InvalidOrder(f"Avro deserialization failed: {error}") from error
        validate_order(order)
        attempt = retry_count(message.headers())
        failures = integer_header(message.headers(), "simulate-transient-until")
        if attempt < failures:
            raise TransientFailure("Simulated DB timeout / HTTP 503")
        snapshot = self.averages.add(order)
        product_summary = ", ".join(
            f"{product}: {stats['average']:.2f} (n={stats['count']})"
            for product, stats in snapshot["products"].items()
        )
        summary = (
            f"Global: {snapshot['global']['average']:.2f} "
            f"(n={snapshot['global']['count']}) | {product_summary}"
        )
        LOGGER.info("averages_updated", extra={"fields": {
            "order_id": order["orderId"], "retry_count": attempt,
            "summary": summary, "averages": snapshot,
        }})
        return snapshot