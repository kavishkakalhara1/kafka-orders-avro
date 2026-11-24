from enum import StrEnum

from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import SerializationError
from httpx import TransportError


class InvalidOrder(ValueError):
    pass


class InvalidHeader(ValueError):
    pass


class TransientFailure(RuntimeError):
    def __init__(self, message, status_code=503):
        super().__init__(message)
        self.status_code = status_code


class FailureKind(StrEnum):
    TRANSIENT = "transient"
    PERMANENT = "permanent"
    FATAL = "fatal"


def classify_error(error):
    if isinstance(error, (InvalidOrder, InvalidHeader)):
        return FailureKind.PERMANENT
    if isinstance(error, (TransientFailure, TimeoutError, ConnectionError, TransportError)):
        return FailureKind.TRANSIENT
    status_code = getattr(error, "http_status_code", getattr(error, "status_code", None))
    if status_code in (408, 429) or (isinstance(status_code, int) and 500 <= status_code < 600):
        return FailureKind.TRANSIENT
    if isinstance(error, SchemaRegistryError):
        return FailureKind.PERMANENT if status_code in (404, 422) else FailureKind.FATAL
    if isinstance(error, SerializationError):
        return FailureKind.PERMANENT
    return FailureKind.FATAL