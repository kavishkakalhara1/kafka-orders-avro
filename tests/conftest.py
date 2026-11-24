import os
import socket
import subprocess
import uuid
from pathlib import Path

import pytest
from confluent_kafka.admin import AdminClient, NewTopic

from common.config import Settings

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def compose_stack():
    if os.getenv("RUN_INTEGRATION") != "1":
        pytest.skip("Run make integration to start an isolated Docker Compose stack")
    sockets = [socket.socket(), socket.socket()]
    try:
        for listener in sockets:
            listener.bind(("127.0.0.1", 0))
        kafka_port, registry_port = [listener.getsockname()[1] for listener in sockets]
    finally:
        for listener in sockets:
            listener.close()
    environment = {
        **os.environ, "KAFKA_PORT": str(kafka_port), "SCHEMA_REGISTRY_PORT": str(registry_port),
    }
    project = f"orders-test-{uuid.uuid4().hex[:10]}"
    command = ["docker", "compose", "-p", project, "-f", str(ROOT / "docker-compose.yml")]
    try:
        subprocess.run(
            [*command, "up", "-d", "--wait", "--wait-timeout", "180", "kafka", "schema-registry"],
            env=environment, cwd=ROOT, check=True, timeout=600,
        )
        settings = Settings(
            brokers=f"localhost:{kafka_port}", registry_url=f"http://localhost:{registry_port}",
            backoff_base=0.3, backoff_max=0.6, backoff_jitter=0, poll_seconds=0.05,
        )
        admin = AdminClient({"bootstrap.servers": settings.brokers})
        topics = [settings.orders_topic, settings.retry_topic, settings.dlq_topic]
        for future in admin.create_topics([
            NewTopic(topic, num_partitions=3, replication_factor=1) for topic in topics
        ]).values():
            future.result(timeout=30)
        yield settings
    finally:
        subprocess.run(
            [*command, "logs", "--tail", "40"], env=environment, cwd=ROOT, timeout=30, check=False
        )
        subprocess.run(
            [*command, "down", "-v", "--remove-orphans"],
            env=environment, cwd=ROOT, timeout=90, check=False,
        )