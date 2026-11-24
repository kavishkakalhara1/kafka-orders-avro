import base64
import json
import logging
import math
import mimetypes
import os
import signal
import subprocess
import sys
import time
import uuid
from collections import Counter, deque
from dataclasses import replace
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Event, RLock, Thread
from urllib.parse import parse_qs, urlparse

from confluent_kafka import TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from common.config import Settings
from common.kafka import SCHEMA_PATH, OrderCodec, make_consumer
from common.runtime import configure_logging
from producer.main import MODES

ROOT = Path(__file__).resolve().parents[1]
STATIC = Path(__file__).resolve().parent / "static"
LOGGER = logging.getLogger(__name__)
WORKERS = {"consumer": "consumer.main", "retry": "consumer.retry_worker"}


def producer_options(data):
    mode = data.get("mode", "normal")
    if mode not in (*MODES, "mixed"):
        raise ValueError("Unknown order mode")
    count = data.get("count", 1)
    rate = data.get("rate", 1)
    failures = data.get("failures", 2)
    if type(count) is not int or not 0 <= count <= 1000:
        raise ValueError("Count must be an integer from 0 (continuous) to 1000")
    if type(rate) not in (int, float) or not math.isfinite(rate) or not 0.1 <= rate <= 20:
        raise ValueError("Rate must be between 0.1 and 20 orders/sec")
    if type(failures) is not int or not 1 <= failures <= 10:
        raise ValueError("Transient failures must be an integer from 1 to 10")
    return mode, count, rate, failures


class Dashboard:
    def __init__(self, settings):
        prefix = os.getenv("DASHBOARD_TOPIC_PREFIX", "demo.orders")
        self.settings = replace(
            settings, orders_topic=prefix, retry_topic=f"{prefix}.retry",
            dlq_topic=f"{prefix}.dlq", consumer_group=f"{prefix}.processors",
            retry_group=f"{prefix}.retry-workers",
        )
        self.settings.validate()
        self.lock = RLock()
        self.stopped = Event()
        self.processes = {}
        self.readers = []
        self.events = deque(maxlen=200)
        self.records = deque(maxlen=300)
        self.trend = deque(maxlen=60)
        self.counts = Counter()
        self.averages = {"global": {"count": 0, "sum": 0, "average": 0}, "products": {}}
        self.health = {"kafka": False, "registry": False, "error": "Connecting to services"}
        self.ready = False
        self.schema_id = None
        self.offsets = []
        self.offsets_updated_at = None
        self.started_at = datetime.now(UTC).isoformat()
        self.sequence = time.time_ns() // 1000
        self.observer = None

    def start(self):
        self.observer = Thread(target=self.observe, daemon=True, name="dashboard-observer")
        self.observer.start()

    def ingest(self, source, entry):
        event = entry.get("event", "log")
        with self.lock:
            self.events.append({**entry, "source": source, "id": uuid.uuid4().hex})
            self.counts[event] += 1
            if event == "averages_updated":
                self.averages = entry["averages"]
                self.trend.append(self.averages["global"]["average"])

    def environment(self):
        settings = self.settings
        return {
            **os.environ, "PYTHONUNBUFFERED": "1",
            "KAFKA_BOOTSTRAP_SERVERS": settings.brokers,
            "SCHEMA_REGISTRY_URL": settings.registry_url,
            "ORDERS_TOPIC": settings.orders_topic, "RETRY_TOPIC": settings.retry_topic,
            "DLQ_TOPIC": settings.dlq_topic, "CONSUMER_GROUP": settings.consumer_group,
            "RETRY_GROUP": settings.retry_group, "LOG_LEVEL": "INFO",
            "NO_PROXY": "localhost,127.0.0.1," + os.getenv("NO_PROXY", ""),
        }

    def launch(self, name, data=None):
        with self.lock:
            if not self.ready or self.stopped.is_set():
                raise ValueError("Kafka and Schema Registry must be ready first")
            current = self.processes.get(name)
            if current and current.poll() is None:
                raise ValueError(f"{name.title()} is already running")
            environment = self.environment()
            if name == "producer":
                mode, count, rate, failures = producer_options(data or {})
                self.sequence = max(self.sequence + 1_000_000, time.time_ns() // 1000)
                environment.update(START_ORDER_ID=str(self.sequence), PRODUCE_RATE=str(rate))
                args = ["producer.main", "--mode", mode, "--count", str(count),
                        "--transient-failures", str(failures)]
            elif name in WORKERS:
                args = [WORKERS[name]]
            else:
                raise ValueError("Unknown worker")
            if name == "consumer":
                self.averages = {"global": {"count": 0, "sum": 0, "average": 0}, "products": {}}
                self.trend.clear()
            process = subprocess.Popen(
                [sys.executable, "-m", *args], cwd=ROOT, env=environment,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            )
            self.processes[name] = process
            reader = Thread(target=self.read_process, args=(name, process), daemon=True)
            self.readers = [thread for thread in self.readers if thread.is_alive()]
            self.readers.append(reader)
            reader.start()

    def read_process(self, name, process):
        with process.stdout:
            for line in process.stdout:
                try:
                    entry = json.loads(line)
                    if not isinstance(entry, dict):
                        raise ValueError("Not a log object")
                except (ValueError, TypeError):
                    entry = {"event": "runtime_log", "message": line.strip(), "level": "INFO"}
                self.ingest(name, entry)
        code = process.wait()
        self.ingest(name, {"event": "worker_stopped", "exit_code": code,
                           "timestamp": datetime.now(UTC).isoformat(),
                           "level": "INFO" if code == 0 else "ERROR"})

    def stop_worker(self, name):
        if name not in (*WORKERS, "producer"):
            raise ValueError("Unknown worker")
        with self.lock:
            process = self.processes.get(name)
            if process and process.poll() is None:
                process.terminate()
        if process:
            try:
                process.wait(timeout=40)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)

    def observe(self):
        settings = self.settings
        consumer = None
        offset_reader = None
        try:
            while not self.stopped.is_set():
                try:
                    admin = AdminClient({"bootstrap.servers": settings.brokers})
                    metadata = admin.list_topics(timeout=3)
                    with self.lock:
                        self.health["kafka"] = True
                    topics = [settings.orders_topic, settings.retry_topic, settings.dlq_topic]
                    for topic in topics:
                        if topic not in metadata.topics:
                            future = admin.create_topics([
                                NewTopic(topic, settings.topic_partitions, 1)
                            ])[topic]
                            future.result(timeout=10)
                    codec = OrderCodec(settings)
                    registered = codec.registry.get_latest_version(f"{settings.orders_topic}-value")
                    consumer = make_consumer(settings, f"dashboard-observer-{uuid.uuid4().hex}")
                    consumer.subscribe(topics)
                    offset_reader = make_consumer(settings, settings.consumer_group)
                    with self.lock:
                        self.schema_id = registered.schema_id
                        self.health = {"kafka": True, "registry": True, "error": None}
                        self.ready = True
                    break
                except Exception as error:
                    with self.lock:
                        self.health["error"] = str(error)
                    if self.stopped.wait(3):
                        return
            if consumer is None:
                return
            next_health_check = time.monotonic() + 5
            while not self.stopped.is_set():
                message = consumer.poll(0.2)
                if message and not message.error():
                    self.record_message(codec, message)
                if time.monotonic() >= next_health_check:
                    for name, check in (
                        ("kafka", lambda: admin.list_topics(timeout=2)),
                        ("registry", lambda: codec.registry.get_subjects()),
                    ):
                        try:
                            check()
                            with self.lock:
                                self.health[name] = True
                        except Exception as error:
                            with self.lock:
                                self.health[name] = False
                                self.health["error"] = str(error)
                    with self.lock:
                        self.ready = self.health["kafka"] and self.health["registry"]
                        if self.ready:
                            self.health["error"] = None
                    if self.health["kafka"]:
                        try:
                            self.read_offsets(offset_reader)
                        except Exception as error:
                            LOGGER.warning("offset_read_failed", extra={"fields": {
                                "reason": str(error),
                            }})
                    next_health_check = time.monotonic() + 5
        except Exception as error:
            LOGGER.exception("dashboard_observer_failed")
            with self.lock:
                self.ready = False
                self.health["error"] = str(error)
        finally:
            if consumer:
                consumer.close()
            if offset_reader:
                offset_reader.close()

    def read_offsets(self, consumer):
        metadata = consumer.list_topics(self.settings.orders_topic, timeout=2)
        partitions = [TopicPartition(self.settings.orders_topic, partition)
                      for partition in metadata.topics[self.settings.orders_topic].partitions]
        offsets = []
        for committed in consumer.committed(partitions, timeout=2):
            low, high = consumer.get_watermark_offsets(committed, timeout=2)
            position = committed.offset if committed.offset >= 0 else low
            offsets.append({"partition": committed.partition,
                            "committed": committed.offset if committed.offset >= 0 else None,
                            "end": high, "lag": max(0, high - position)})
        with self.lock:
            self.offsets = offsets
            self.offsets_updated_at = datetime.now(UTC).isoformat()

    def record_message(self, codec, message):
        raw = message.value()
        try:
            payload = codec.decode(message.topic(), raw)
        except Exception:
            payload = raw.decode("utf-8", errors="replace") if raw is not None else None
        record = {
            "id": f"{message.topic()}:{message.partition()}:{message.offset()}",
            "topic": message.topic(), "partition": message.partition(), "offset": message.offset(),
            "key": (message.key() or b"").decode("utf-8", errors="replace"),
            "payload": payload, "base64": base64.b64encode(raw or b"").decode(),
            "headers": {key: (value or b"").decode("utf-8", errors="replace")
                        for key, value in (message.headers() or [])},
            "timestamp": message.timestamp()[1],
        }
        with self.lock:
            self.records.append(record)

    def snapshot(self):
        with self.lock:
            return json.loads(json.dumps({
                "ready": self.ready, "health": self.health, "started_at": self.started_at,
                "workers": {name: {
                    "running": name in self.processes and self.processes[name].poll() is None,
                    "exit_code": self.processes[name].poll() if name in self.processes else None,
                } for name in (*WORKERS, "producer")},
                "counts": self.counts, "averages": self.averages, "trend": list(self.trend),
                "events": list(reversed(self.events)), "records": list(reversed(self.records)),
                "schema": json.loads(SCHEMA_PATH.read_text()), "schema_id": self.schema_id,
                "offsets": self.offsets, "offsets_updated_at": self.offsets_updated_at,
                "consumer_group": self.settings.consumer_group,
                "topics": {"orders": self.settings.orders_topic, "retry": self.settings.retry_topic,
                           "dlq": self.settings.dlq_topic},
                "config": {"max_retries": self.settings.max_retries,
                           "backoff_base": self.settings.backoff_base,
                           "backoff_max": self.settings.backoff_max,
                           "jitter": self.settings.backoff_jitter,
                           "brokers": self.settings.brokers,
                           "registry": self.settings.registry_url},
            }))

    def close(self):
        self.stopped.set()
        for name in ("producer", "consumer", "retry"):
            self.stop_worker(name)
        if self.observer:
            self.observer.join(timeout=15)


def handler_for(dashboard):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format_string, *args):
            return

        def send_content(self, code, content, content_type="application/json", attachment=None):
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(content)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            if attachment:
                self.send_header("Content-Disposition", f'attachment; filename="{attachment}"')
            self.end_headers()
            self.wfile.write(content)

        def do_GET(self):
            path = urlparse(self.path).path
            if path == "/api/state":
                self.send_content(200, json.dumps(dashboard.snapshot()).encode())
                return
            if path == "/api/export":
                query = parse_qs(urlparse(self.path).query)
                topic = query.get("topic", ["all"])[0]
                search = query.get("search", [""])[0].lower()
                snapshot = dashboard.snapshot()
                if topic not in ("all", *snapshot["topics"]):
                    self.send_content(400, b'{"error":"Unknown topic filter"}')
                    return
                records = [record for record in snapshot["records"]
                           if (topic == "all" or record["topic"] == snapshot["topics"][topic])
                           and search in json.dumps(record).lower()]
                self.send_content(200, json.dumps(records, indent=2).encode(),
                                  attachment=f"orderflow-{topic}.json")
                return
            files = {"/": "index.html", "/app.js": "app.js", "/style.css": "style.css",
                     "/lucide.js": "lucide.js"}
            if path not in files or not (STATIC / files[path]).exists():
                self.send_content(404, b'{"error":"Not found"}')
                return
            file = STATIC / files[path]
            self.send_content(200, file.read_bytes(), mimetypes.guess_type(file)[0] or "text/plain")

        def do_POST(self):
            origin = self.headers.get("Origin")
            if origin and origin != f"http://{self.headers.get('Host')}":
                self.send_content(403, b'{"error":"Cross-origin commands are not allowed"}')
                return
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size <= 4096 or self.headers.get("Content-Type") != "application/json":
                    raise ValueError("Expected a small JSON request")
                data = json.loads(self.rfile.read(size))
                if not isinstance(data, dict):
                    raise ValueError("Expected a JSON object")
                if self.path == "/api/produce":
                    dashboard.launch("producer", data)
                elif self.path == "/api/control":
                    if data.get("action") == "start":
                        dashboard.launch(data.get("worker"))
                    elif data.get("action") == "stop":
                        dashboard.stop_worker(data.get("worker"))
                    else:
                        raise ValueError("Unknown action")
                else:
                    self.send_content(404, b'{"error":"Not found"}')
                    return
                self.send_content(200, b'{"ok":true}')
            except (ValueError, TypeError) as error:
                self.send_content(400, json.dumps({"error": str(error)}).encode())
            except Exception:
                LOGGER.exception("dashboard_command_failed")
                self.send_content(500, b'{"error":"Command failed. Check server logs."}')

    return Handler


def main():
    settings = Settings.from_env()
    os.environ["NO_PROXY"] = "localhost,127.0.0.1," + os.getenv("NO_PROXY", "")
    configure_logging(settings.log_level)
    dashboard = Dashboard(settings)
    port = int(os.getenv("DASHBOARD_PORT", "8090"))
    server = ThreadingHTTPServer(("127.0.0.1", port), handler_for(dashboard))
    server.daemon_threads = True

    def shutdown(signum, frame):
        Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    dashboard.start()
    LOGGER.info("dashboard_started", extra={"fields": {"url": f"http://127.0.0.1:{port}"}})
    try:
        server.serve_forever()
    finally:
        dashboard.close()
        server.server_close()


if __name__ == "__main__":
    main()