import abc
import dataclasses
import logging
from datetime import timedelta
from typing import Dict, List, Optional, Union

import confluent_kafka  # type: ignore

from .auth import SASLAuth
from .errors import (DeliveryCallback, ErrorCallback, log_client_errors,
                     log_delivery_errors)


class Producer:
    conf: 'ProducerConfig'
    _producer: confluent_kafka.Producer
    logger: logging.Logger

    def __init__(self, conf: 'ProducerConfig') -> None:
        self.logger = logging.getLogger("adc-streaming.producer")
        self.conf = conf
        self.logger.debug(f"connecting to producer with config {conf._to_confluent_kafka()}")
        self._producer = confluent_kafka.Producer(conf._to_confluent_kafka())

    def write(self,
              msg: Union[bytes, 'Serializable']) -> None:
        if isinstance(msg, Serializable):
            msg = msg.serialize()
        self.logger.debug("writing message to %s", self.conf.topic)
        if self.conf.delivery_callback is not None:
            self._producer.produce(self.conf.topic, msg, on_delivery=self.conf.delivery_callback)
        else:
            self._producer.produce(self.conf.topic, msg)

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
    delivery_callback: Optional[DeliveryCallback] = log_delivery_errors
    error_callback: Optional[ErrorCallback] = log_client_errors

    # produce_timeout sets the maximum amount of time that the backend can take
    # to send a message to Kafka. Use a value of 0 to never timeout.
    produce_timeout = timedelta(seconds=10)

    def _to_confluent_kafka(self) -> Dict:
        config = {
            "bootstrap.servers": ",".join(self.broker_urls),
            "message.timeout.ms": self.produce_timeout.total_seconds() * 1000.0,
        }
        if self.error_callback is not None:
            config["error_cb"] = self.error_callback
        if self.auth is not None:
            config.update(self.auth())
        return config


class Serializable(abc.ABC):
    def serialize(self) -> bytes:
        raise NotImplementedError()
