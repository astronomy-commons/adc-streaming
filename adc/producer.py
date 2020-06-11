from typing import Any, Dict, List, Optional, Union
import abc
import dataclasses
import logging

import confluent_kafka  # type: ignore

from .auth import SASLAuth
from .errors import ErrorCallback, DeliveryCallback, log_client_errors, log_delivery_errors
from .codecs import KafkaCodec, RawCodec
from .message import KafkaMessage


class Producer:
    conf: 'ProducerConfig'
    codec: KafkaCodec
    _producer: confluent_kafka.Producer
    logger: logging.Logger

    def __init__(self, conf: 'ProducerConfig', codec: KafkaCodec = RawCodec()) -> None:
        self.logger = logging.getLogger("adc-streaming.producer")
        self.codec = codec
        self.conf = conf
        self._producer = confluent_kafka.Producer(conf._to_confluent_kafka())

    def write(self,
              msg: Any,
              cb: Optional[DeliveryCallback] = None) -> None:
        """ Write a message to the Kafka broker.

        msg must be of a type which is encodable using the Producer's codec.
        """
        kafka_msg = self.codec.serialize(msg)
        if kafka_msg.topic is None:
            kafka_msg.topic = self.conf.topic
        self._write_kafka_msg(kafka_msg, on_delivery=cb)

    def _write_kafka_msg(self,
                         msg: KafkaMessage,
                         on_delivery: Optional[DeliveryCallback] = None) -> None:
        kwargs: Dict[str, Any] = {
            "value": msg.value,
            "topic": msg.topic,
        }
        if msg.key is not None:
            kwargs["key"] = msg.key
        if msg.partition is not None:
            kwargs["partition"] = msg.partition
        if msg.timestamp is not None:
            kwargs["timestamp"] = msg._unix_timestamp()
        if on_delivery is not None:
            kwargs["on_delivery"] = on_delivery
        self._producer.produce(**kwargs)

    def flush(self) -> int:
        """Attempt to flush enqueued messages. Return the number of messages still
        enqueued after the attempt.

        """
        return self._producer.flush()

    def close(self) -> None:
        self.flush()

    def __enter__(self) -> 'Producer':
        return self

    def __exit__(self, type, value, traceback) -> bool:
        self.close()
        if type == KeyboardInterrupt:
            print("Aborted (CTRL-C).")
            return True
        return False


@dataclasses.dataclass
class ProducerConfig:
    broker_urls: List[str]
    topic: str
    auth: Optional[SASLAuth] = None
    delivery_callback: Optional[DeliveryCallback] = log_delivery_errors
    error_callback: Optional[ErrorCallback] = log_client_errors

    def _to_confluent_kafka(self) -> Dict:
        config = {
            "bootstrap.servers": ",".join(self.broker_urls),
        }
        if self.auth is not None:
            config.update(self.auth())
        return config
