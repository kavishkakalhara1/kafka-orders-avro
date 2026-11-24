import argparse
import logging
import random
import time

from common.config import Settings
from common.kafka import OrderCodec, make_producer, malformed_order, publish_confirmed
from common.runtime import configure_logging, shutdown_event

LOGGER = logging.getLogger(__name__)
PRODUCTS = ("Item1", "Item2", "Item3", "Item4")
MODES = ("normal", "transient", "exhausted", "negative", "missing-product", "malformed")


def produce_orders(settings, stopped, mode="normal", count=0, transient_failures=1):
    codec = OrderCodec(settings)
    producer = make_producer(settings)
    sequence = 0
    deadline = time.monotonic()
    try:
        while not stopped.is_set() and (count == 0 or sequence < count):
            selected = MODES[sequence % len(MODES)] if mode == "mixed" else mode
            order = {
                "orderId": str(settings.start_order_id + sequence),
                "product": random.choice(PRODUCTS),
                "price": round(random.uniform(10, 500), 2),
            }
            headers = [("demo-mode", selected.encode())]
            if selected == "negative":
                order["price"] = -order["price"]
            if selected in ("transient", "exhausted"):
                failures = (
                    transient_failures if selected == "transient" else settings.max_retries + 1
                )
                headers.append(("simulate-transient-until", str(failures).encode()))
            if selected == "missing-product":
                payload = malformed_order(order)
            elif selected == "malformed":
                payload = b"deliberately-not-an-avro-message"
            else:
                payload = codec.encode(settings.orders_topic, order)
            message = publish_confirmed(
                producer, settings.orders_topic, order["orderId"].encode(), payload, headers
            )
            LOGGER.info("order_produced", extra={"fields": {
                "order": order if selected != "missing-product" else {
                    name: value for name, value in order.items() if name != "product"
                },
                "mode": selected, "partition": message.partition(), "offset": message.offset(),
            }})
            sequence += 1
            deadline += 1 / settings.produce_rate
            stopped.wait(max(0, deadline - time.monotonic()))
    finally:
        remaining = producer.flush(settings.delivery_timeout)
        if remaining:
            LOGGER.error("producer_flush_incomplete", extra={"fields": {"remaining": remaining}})


def main():
    parser = argparse.ArgumentParser(description="Produce Schema Registry-backed Avro orders")
    parser.add_argument("--mode", choices=(*MODES, "mixed"), default="normal")
    parser.add_argument("--count", type=int, default=0, help="0 means continuous")
    parser.add_argument("--transient-failures", type=int, default=1)
    args = parser.parse_args()
    if args.count < 0 or args.transient_failures < 0:
        parser.error("Counts must be nonnegative")
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    try:
        produce_orders(settings, shutdown_event(), args.mode, args.count, args.transient_failures)
    except Exception:
        LOGGER.exception("producer_failed")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()