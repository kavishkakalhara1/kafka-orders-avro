import json
import time
from queue import Queue
from threading import Event, Thread

import pytest
from confluent_kafka import TopicPartition

from common.kafka import OrderCodec, make_consumer, make_producer, publish_confirmed
from consumer.headers import header_value, retry_count
from consumer.main import run_consumer
from consumer.processing import OrderProcessor
from consumer.retry_worker import run_retry_worker


@pytest.mark.integration
def test_avro_orders_retry_dlq_and_commits(compose_stack):
    settings = compose_stack
    codec = OrderCodec(settings)
    schema = codec.registry.get_latest_version(f"{settings.orders_topic}-value")
    assert json.loads(schema.schema.schema_str)["name"] == "Order"
    with pytest.raises(ValueError):
        codec.encode(settings.orders_topic, {"orderId": "bad", "price": 10.0})
    completed, failures = Queue(), Queue()
    stopped = Event()

    class RecordingProcessor(OrderProcessor):
        def process(self, message):
            snapshot = super().process(message)
            completed.put((message.key(), snapshot))
            return snapshot

    processor = RecordingProcessor(codec)

    def guarded(target, *args):
        try:
            target(*args)
        except Exception as error:
            failures.put(error)
            stopped.set()

    threads = [
        Thread(target=guarded, args=(run_consumer, settings, stopped, processor), daemon=True),
        Thread(target=guarded, args=(run_retry_worker, settings, stopped), daemon=True),
    ]
    observer = make_consumer(settings, "integration-audit")
    observer.subscribe([settings.retry_topic, settings.dlq_topic])
    producer = make_producer(settings)
    originals = {}
    dlq_records, retries = {}, []
    successes = []
    for thread in threads:
        thread.start()
    try:
        cases = [
            ("transient", "Item1", 20, 2), ("normal1", "Item1", 10, 0),
            ("normal2", "Item2", 30, 0), ("exhausted", "Item1", 100, 4),
            ("negative", "Item1", -10, 0), ("missing", None, 10, 0),
            ("malformed", "Item1", 10, 0),
        ]
        for order_id, product, price, transient_failures in cases:
            order = {"orderId": order_id, "product": product, "price": float(price)}
            if order_id == "missing":
                payload = b'{"orderId":"missing","price":10}'
            elif order_id == "malformed":
                payload = b"not-avro"
            else:
                payload = codec.encode(settings.orders_topic, order)
            headers = [("simulate-transient-until", str(transient_failures).encode())]
            message = publish_confirmed(
                producer, settings.orders_topic, order_id.encode(), payload, headers
            )
            originals[order_id.encode()] = (payload, message.partition(), message.offset())

        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if not failures.empty():
                raise failures.get()
            while not completed.empty():
                successes.append(completed.get())
            if len(successes) == 3 and len(dlq_records) == 4 and len(retries) == 5:
                break
            record = observer.poll(0.1)
            if record is None:
                continue
            assert not record.error(), record.error()
            if record.topic() == settings.dlq_topic:
                dlq_records[record.key()] = record
            else:
                retries.append((record.key(), retry_count(record.headers())))

        assert not failures.qsize()
        assert len(successes) == 3
        assert successes[0][0] != b"transient", "A delayed retry must not block normal orders"
        assert successes[-1][1]["global"] == {"count": 3, "sum": 60.0, "average": 20.0}
        assert successes[-1][1]["products"]["Item1"]["average"] == 15.0
        assert sorted(attempt for key, attempt in retries if key == b"transient") == [1, 2]
        assert sorted(attempt for key, attempt in retries if key == b"exhausted") == [1, 2, 3]
        assert set(dlq_records) == {b"negative", b"missing", b"malformed", b"exhausted"}
        for key, record in dlq_records.items():
            payload, partition, offset = originals[key]
            assert record.value() == payload
            headers = record.headers()
            assert header_value(headers, "original-topic") == settings.orders_topic.encode()
            assert header_value(headers, "original-partition") == str(partition).encode()
            assert header_value(headers, "original-offset") == str(offset).encode()
            assert header_value(headers, "failure-reason")
            assert header_value(headers, "exception-type")
            assert header_value(headers, "timestamp")
            assert retry_count(headers) == (3 if key == b"exhausted" else 0)
    finally:
        stopped.set()
        for thread in threads:
            thread.join(timeout=20)
        observer.close()
        producer.flush(15)
    assert not any(thread.is_alive() for thread in threads)
    assert failures.empty()
    for topic, group in [
        (settings.orders_topic, settings.consumer_group),
        (settings.retry_topic, settings.retry_group),
    ]:
        checker = make_consumer(settings, group)
        try:
            partitions = [TopicPartition(topic, partition) for partition in range(3)]
            for committed in checker.committed(partitions, timeout=10):
                _, high = checker.get_watermark_offsets(committed, timeout=10)
                if high:
                    assert committed.offset == high
        finally:
            checker.close()