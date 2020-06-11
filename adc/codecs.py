from typing import Any, Dict, Generic, TypeVar

import json

from .message import KafkaMessage

T = TypeVar("T")


class KafkaCodec(Generic[T]):
    """A KafkaCodec provides methods to encode a single message from a Python
    object into a KafkaMessage, and to decode back out.

    This is a generic and abstract type, intended to be subclassed for
    particular type mappings.

    """
    def serialize(self, msg: T) -> KafkaMessage:
        raise NotImplementedError()

    def deserialize(self, msg: KafkaMessage) -> T:
        raise NotImplementedError()


class RawCodec(KafkaCodec[KafkaMessage]):
    """Write and read explicit KafkaMessages."""
    def serialize(self, msg: KafkaMessage) -> KafkaMessage:
        return msg

    def deserialize(self, msg: KafkaMessage) -> KafkaMessage:
        return msg


class SimpleBytesCodec(KafkaCodec[bytes]):
    """Encode bytes as the value on a Kafka message. Hide all other implementation
    details.

    """
    def serialize(self, msg: bytes) -> KafkaMessage:
        return KafkaMessage(value=msg)

    def deserialize(self, msg: KafkaMessage) -> bytes:
        return msg.value


class JSONKafkaCodec(KafkaCodec[Dict]):
    """A KafkaCodec which encodes dictionaries to UTF-8 byte values for the
    KafkaMessages.

    """
    def serialize(self, msg: Dict) -> KafkaMessage:
        return KafkaMessage(
            value=json.dumps(msg).encode("utf-8")
        )

    def deserialize(self, msg: KafkaMessage) -> Dict:
        return json.loads(msg.value.decode("utf-8"))
