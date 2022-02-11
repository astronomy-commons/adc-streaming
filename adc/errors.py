from contextlib import contextmanager
import logging
from typing import Callable

import confluent_kafka  # type: ignore
from packaging.version import Version

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
        return cls(error)

    def __init__(self, error):
        self.error = error
        self.name = error.name()
        self.reason = error.str()
        self.retriable = error.retriable()
        self.fatal = error.fatal()
        msg = f"Error communicating with Kafka: code={self.name} {self.reason}"
        super(KafkaException, self).__init__(msg)


@contextmanager
def catch_kafka_version_support_errors():
    try:
        yield
    except confluent_kafka.KafkaException as e:
        err, *_ = e.args
        librdkafka_version, *_ = confluent_kafka.libversion()
        if (
            err.code() == confluent_kafka.KafkaError._INVALID_ARG
            and any(err.str() == f'No such configuration property: "{key}"'
                    for key in ['sasl.oauthbearer.client.id',
                                'sasl.oauthbearer.client.secret',
                                'sasl.oauthbearer.method',
                                'sasl.oauthbearer.token.endpoint.url'])
            and Version(librdkafka_version) < Version('1.9.0')
        ):
            raise RuntimeError(f'OpenID Connect support requires librdkafka >= 1.9.0, but you have {librdkafka_version}. Please install a newer version of confluent-kafka.') from e
        raise
