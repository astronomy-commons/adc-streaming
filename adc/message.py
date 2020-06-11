from typing import Any, Dict, Optional
import abc
import confluent_kafka  # type: ignore
import datetime
import dataclasses
import json


@dataclasses.dataclass
class KafkaMessage:
    """ KafkaMessage represents the data held by a message in Kafka. """
    value: bytes

    key: Optional[bytes] = None
    headers: Optional[Dict[str, bytes]] = None
    timestamp: Optional[datetime.datetime] = None

    topic: Optional[str] = None
    partition: Optional[int] = None
    offset: Optional[int] = None

    def _unix_timestamp(self) -> Optional[float]:
        if self.timestamp is None:
            return None
        return self.timestamp.timestamp() * 1000.0

    @classmethod
    def _from_confluent_kafka(cls, msg: confluent_kafka.Message) -> 'KafkaMessage':
        km = KafkaMessage(
            key=msg.key(),
            value=msg.value(),
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )
        timestamp_type, timestamp_epoch_ms = msg.timestamp()
        if timestamp_type != confluent_kafka.TIMESTAMP_NOT_AVAILABLE:
            km.timestamp = datetime.datetime.fromtimestamp(timestamp_epoch_ms / 1000.0)

        if msg.headers() is not None:
            km.headers = dict(msg.headers())

        return km
