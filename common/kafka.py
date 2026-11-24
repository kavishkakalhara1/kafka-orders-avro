import json
import logging
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

LOGGER = logging.getLogger(__name__)
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schemas" / "order.avsc"


class OrderCodec:
    def __init__(self, settings):
        self.registry = SchemaRegistryClient({"url": settings.registry_url, "timeout": 10})
        schema = SCHEMA_PATH.read_text()
        schema_id = self.registry.register_schema(
            f"{settings.orders_topic}-value", Schema(schema, "AVRO")
        )
        self.serializer = AvroSerializer(
            self.registry, schema, conf={"auto.register.schemas": False}
        )
        self.deserializer = AvroDeserializer(self.registry)
        LOGGER.info("schema_registered", extra={"fields": {"schema_id": schema_id}})

    def encode(self, topic, order):
        return self.serializer(order, SerializationContext(topic, MessageField.VALUE))

    def decode(self, topic, value):
        return self.deserializer(value, SerializationContext(topic, MessageField.VALUE))


def make_producer(settings):
    return Producer({
        "bootstrap.servers": settings.brokers,
        "enable.idempotence": True,
        "acks": "all",
        "delivery.timeout.ms": int(settings.delivery_timeout * 1000),
        "request.timeout.ms": int(settings.delivery_timeout * 1000),
    })


def make_consumer(settings, group):
    return Consumer({
        "bootstrap.servers": settings.brokers,
        "group.id": group,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
        "max.poll.interval.ms": 300000,
    })


def publish_confirmed(producer, topic, key, value, headers=()):
    result = []

    def delivered(error, message):
        result.append((error, message))

    producer.produce(topic, key=key, value=value, headers=list(headers), on_delivery=delivered)
    while not result:
        producer.poll(0.1)
    error, message = result[0]
    if error:
        raise KafkaException(error)
    LOGGER.info("message_delivered", extra={"fields": {
        "topic": message.topic(), "partition": message.partition(), "offset": message.offset(),
        "key": key.decode("utf-8", errors="replace") if isinstance(key, bytes) else key,
        "payload_bytes": len(value) if value is not None else 0,
    }})
    return message


def malformed_order(order):
    incomplete = {name: value for name, value in order.items() if name != "product"}
    return json.dumps(incomplete).encode("utf-8")