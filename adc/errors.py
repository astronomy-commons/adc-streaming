import logging
from typing import Callable

import confluent_kafka  # type: ignore

logger = logging.getLogger("adc-streaming")


ErrorCallback = Callable[[confluent_kafka.KafkaError], None]


def log_client_errors(kafka_error: confluent_kafka.KafkaError):
    if kafka_error.code() == confluent_kafka.KafkaError._ALL_BROKERS_DOWN:
        # This error occurs very frequently. It's not nearly as fatal as it
        # sounds: it really indicates that the client's broker metadata has
        # timed out. It appears to get triggered in races during client
        # shutdown, too. See https://github.com/edenhill/librdkafka/issues/2543
        # for more background.
        logger.warn("client is currently disconnected from all brokers")
    else:
        logger.error(f"internal kafka error: {kafka_error}")
        raise(KafkaException.from_kafka_error(kafka_error))


DeliveryCallback = Callable[[confluent_kafka.KafkaError, confluent_kafka.Message], None]


def log_delivery_errors(
        kafka_error: confluent_kafka.KafkaError,
        msg: confluent_kafka.Message) -> None:
    if kafka_error is not None:
        logger.error(f"delivery error: {kafka_error}")


def raise_delivery_errors(kafka_error: confluent_kafka.KafkaError,
                          msg: confluent_kafka.Message) -> None:
    if kafka_error is not None:
        raise KafkaException.from_kafka_error(kafka_error)
    elif msg.error() is not None:
        raise KafkaException.from_kafka_error(msg.error())


class KafkaException(Exception):
    @classmethod
    def from_kafka_error(cls, error):
        return cls(error.name(), error.str())

    def __init__(self, name, message):
        self.name = name
        self.message = message
        msg = f"Error communicating with Kafka: code={name} {message}"
        super(KafkaException, self).__init__(msg)
