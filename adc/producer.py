from typing import Dict, List, Optional, Union
import abc
import dataclasses
import logging

import confluent_kafka  # type: ignore

from .auth import SASLAuth
from .errors import ErrorCallback, DeliveryCallback, log_client_errors, log_delivery_errors


class Producer:
    conf: 'ProducerConfig'
    _producer: confluent_kafka.Producer
    logger: logging.Logger

    def __init__(self, conf: 'ProducerConfig') -> None:
        self.logger = logging.getLogger("adc-streaming.producer")
        self.conf = conf
        self._producer = confluent_kafka.Producer(conf._to_confluent_kafka())

    def write(self,
              msg: Union[bytes, 'Serializable'],
              cb: Optional[DeliveryCallback] = None) -> None:
        if isinstance(msg, Serializable):
            msg = msg.serialize()
        self._producer.produce(self.conf.topic, msg, on_delivery=cb)

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


class Serializable(abc.ABC):
    def serialize(self) -> bytes:
        raise NotImplementedError()
