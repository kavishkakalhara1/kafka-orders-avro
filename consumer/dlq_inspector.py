import argparse
import base64
import json
import logging

from confluent_kafka import OFFSET_BEGINNING, KafkaException
from rich.console import Console
from rich.table import Table

from common.config import Settings
from common.kafka import OrderCodec, make_consumer
from common.runtime import configure_logging, shutdown_event

LOGGER = logging.getLogger(__name__)


def payload_preview(codec, message):
    try:
        return json.dumps(codec.decode(message.topic(), message.value()), ensure_ascii=True)
    except Exception:
        value = message.value()
        if value is None:
            return "<tombstone>"
        return (
            f"Raw UTF-8: {value.decode('utf-8', errors='replace')}\n"
            f"Base64: {base64.b64encode(value).decode('ascii')}"
        )


def inspect_dlq(settings, stopped, count=0, from_beginning=False):
    codec = OrderCodec(settings)
    consumer = make_consumer(settings, settings.dlq_group)
    console = Console()

    def assigned(client, partitions):
        if from_beginning:
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING
        client.assign(partitions)

    consumer.subscribe([settings.dlq_topic], on_assign=assigned)
    inspected = 0
    try:
        while not stopped.is_set() and (count == 0 or inspected < count):
            message = consumer.poll(settings.poll_seconds)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            table = Table(title=f"DLQ {message.partition()}:{message.offset()}")
            table.add_column("Field", style="cyan", no_wrap=True)
            table.add_column("Value", overflow="fold")
            table.add_row("key", (message.key() or b"").decode("utf-8", errors="replace"))
            for name, value in message.headers() or []:
                table.add_row(name, (value or b"").decode("utf-8", errors="replace"))
            table.add_row("payload", payload_preview(codec, message))
            console.print(table, markup=False)
            consumer.commit(message=message, asynchronous=False)
            inspected += 1
            LOGGER.info("dlq_inspected", extra={"fields": {
                "partition": message.partition(), "offset": message.offset(),
            }})
    finally:
        consumer.close()


def main():
    parser = argparse.ArgumentParser(description="Inspect DLQ payloads and provenance")
    parser.add_argument("--count", type=int, default=0, help="0 means follow continuously")
    parser.add_argument("--from-beginning", action="store_true")
    args = parser.parse_args()
    if args.count < 0:
        parser.error("Count must be nonnegative")
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    try:
        inspect_dlq(settings, shutdown_event(), args.count, args.from_beginning)
    except Exception:
        LOGGER.exception("dlq_inspector_failed")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()