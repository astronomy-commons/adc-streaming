import abc
from ast import comprehension
import dataclasses
import logging
from datetime import timedelta
from typing import Dict, List, Optional, Union
try:  # this will work only in python >= 3.8
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

import confluent_kafka  # type: ignore

from .auth import SASLAuth
from .errors import (DeliveryCallback, ErrorCallback, log_client_errors,
                     log_delivery_errors)
from .oidc import set_oauth_cb


class Producer:
    conf: 'ProducerConfig'
    _producer: confluent_kafka.Producer
    logger: logging.Logger

    def __init__(self, conf: 'ProducerConfig') -> None:
        self.logger = logging.getLogger("adc-streaming.producer")
        self.conf = conf
        self.logger.debug(f"connecting to producer with config {conf._to_confluent_kafka()}")
        self._producer = confluent_kafka.Producer(conf._to_confluent_kafka())
        # Workaround for https://github.com/confluentinc/librdkafka/issues/3753#issuecomment-1058272987.
        # FIXME: Remove once fixed upstream, or on removal of oauth_cb.
        self._producer.poll(0)

    def write(self,
              msg: Union[bytes, 'Serializable'],
              headers: Optional[Union[dict, list]] = None,
              delivery_callback: Optional[DeliveryCallback] = log_delivery_errors) -> None:
        if isinstance(msg, Serializable):
            msg = msg.serialize()
        self.logger.debug("writing message to %s", self.conf.topic)
        if delivery_callback is not None:
            self._producer.produce(self.conf.topic, msg, headers=headers,
                                   on_delivery=delivery_callback)
        else:
            self._producer.produce(self.conf.topic, msg, headers=headers)

    def flush(self, timeout: timedelta = timedelta(seconds=10)) -> int:
        """Attempt to flush enqueued messages. Return the number of messages still
        enqueued after the attempt.

        """
        n = self._producer.flush(timeout.total_seconds())
        if n > 0:
            self.logger.debug("flushed messages, %d still enqueued", n)
        else:
            self.logger.debug("flushed all messages")
        return n

    def close(self) -> int:
        self.logger.debug("shutting down producer")
        return self.flush()

    def __enter__(self) -> 'Producer':
        return self

    def __exit__(self, type, value, traceback) -> bool:
        if type == KeyboardInterrupt:
            print("Aborted (CTRL-C).")
            return True
        if type is None and value is None and traceback is None:
            n_unsent = self.close()
            if n_unsent > 0:
                raise Exception(f"{n_unsent} messages remain unsent, some data may have been lost!")
            return False
        return False


@dataclasses.dataclass
class ProducerConfig:
    broker_urls: List[str]
    topic: str
    auth: Optional[SASLAuth] = None
    error_callback: Optional[ErrorCallback] = log_client_errors

    # produce_timeout sets the maximum amount of time that the backend can take
    # to send a message to Kafka. Use a value of 0 to never timeout.
    produce_timeout: timedelta = timedelta(seconds=10)

    # produce_backoff_time sets the time the backend will wait before retrying
    # to send a message to Kafka. May not be less than one millisecond.
    produce_backoff_time: timedelta = timedelta(milliseconds=100)

    # use_idempotence instructs the backend whether to ensure that messages are
    # recorded by the broker exactly once and in the order of production.
    use_idempotence: bool = False

    # reconnect_backoff_time is the time that the backend should initially wait
    # before attempting to reconnect to Kafka if its connection fails.
    # Repeated failures will cause the wait time to be increased exponentially,
    # with a random variation, until reconnect_max_time is reached.
    reconnect_backoff_time: timedelta = timedelta(milliseconds=100)

    # reconnect_max_time is the longest time that the backend should wait
    # between attempts to reconnect to Kafka.
    reconnect_max_time: timedelta = timedelta(seconds=10)

    compression_type: Optional[Union[Literal['gzip'], Literal['snappy'], Literal['lz4'], Literal['zstd']]] = None

    # maximum message size, before compression
    message_max_bytes: Optional[int] = None

    def _to_confluent_kafka(self) -> Dict:
        def as_ms(td: timedelta):
            """Convert a timedelta object to a duration in milliseconds"""
            return int(td.total_seconds() * 1000.0)

        if self.produce_backoff_time < timedelta(milliseconds=1):
            raise ValueError("produce_backoff_time may not be less than one millisecond")
        config = {
            "bootstrap.servers": ",".join(self.broker_urls),
            "enable.idempotence": self.use_idempotence,
            "message.timeout.ms": as_ms(self.produce_timeout),
            "reconnect.backoff.max.ms": as_ms(self.reconnect_max_time),
            "reconnect.backoff.ms": as_ms(self.reconnect_backoff_time),
            "retry.backoff.ms": as_ms(self.produce_backoff_time),
            "compression.type": self.compression_type or 'none',
        }
        if self.message_max_bytes is not None:
            config['message.max.bytes'] = self.message_max_bytes
        if self.error_callback is not None:
            config["error_cb"] = self.error_callback
        if self.auth is not None:
            config.update(self.auth())
        set_oauth_cb(config)
        return config


class Serializable(abc.ABC):
    def serialize(self) -> bytes:
        raise NotImplementedError()
