import math
import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    brokers: str = "localhost:9092"
    registry_url: str = "http://localhost:8081"
    orders_topic: str = "orders"
    retry_topic: str = "orders.retry"
    dlq_topic: str = "orders.dlq"
    consumer_group: str = "order-processors"
    retry_group: str = "order-retry-workers"
    dlq_group: str = "order-dlq-inspector"
    topic_partitions: int = 3
    produce_rate: float = 1.0
    start_order_id: int = 1001
    max_retries: int = 3
    backoff_base: float = 1.0
    backoff_max: float = 30.0
    backoff_jitter: float = 0.25
    delivery_timeout: float = 15.0
    poll_seconds: float = 0.2
    log_level: str = "INFO"

    @classmethod
    def from_env(cls):
        load_dotenv()
        names = {
            "brokers": "KAFKA_BOOTSTRAP_SERVERS",
            "registry_url": "SCHEMA_REGISTRY_URL",
            "backoff_base": "BACKOFF_BASE_SECONDS",
            "backoff_max": "BACKOFF_MAX_SECONDS",
            "delivery_timeout": "DELIVERY_TIMEOUT_SECONDS",
        }
        defaults = cls()
        values = {}
        for name in cls.__dataclass_fields__:
            default = getattr(defaults, name)
            values[name] = type(default)(os.getenv(names.get(name, name.upper()), str(default)))
        settings = cls(**values)
        settings.validate()
        return settings

    def validate(self):
        positive_fields = (
            "produce_rate", "delivery_timeout", "poll_seconds", "backoff_base", "backoff_max"
        )
        for name in positive_fields:
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if self.max_retries < 0 or self.topic_partitions < 1 or self.start_order_id < 0:
            raise ValueError("Invalid retry count, partition count, or starting order ID")
        if not 0 <= self.backoff_jitter <= 1 or self.backoff_max < self.backoff_base:
            raise ValueError("Jitter must be in [0, 1] and maximum backoff >= base backoff")
        topics = (self.orders_topic, self.retry_topic, self.dlq_topic)
        if not all(topics) or len(set(topics)) != 3:
            raise ValueError("Orders, retry, and DLQ topics must be nonempty and distinct")
        if self.delivery_timeout >= 120:
            raise ValueError("Delivery timeout must be below 120s (consumer poll budget)")