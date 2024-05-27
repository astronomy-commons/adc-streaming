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


DeliveryCallback = Callable[[confluent_kafka.KafkaError, confluent_kafka.Message], None]


def log_delivery_errors(
        kafka_error: confluent_kafka.KafkaError,
        msg: confluent_kafka.Message) -> None:
    if kafka_error is not None:
        logger.error(f"delivery error: {kafka_error}")


def raise_delivery_errors(kafka_error: confluent_kafka.KafkaError,
                          msg: confluent_kafka.Message) -> None:
    if kafka_error is not None:
        raise KafkaException.from_kafka_error(kafka_error, msg)
    elif msg.error() is not None:
        raise KafkaException.from_kafka_error(msg.error(), msg)


def _get_topic_related_errors():
    """Build a set of all Kafka error codes which seem to relate to a specific topic.

    This uses a list extracted from all documented error codes up to confluent_kafka v2.4,
    but some of these errors did not exist or were not exposed in earlier versions.
    To maintain backward compatibility, this function checks whether each error exists before
    attempting to otherwise refer to it.
    """
    err_names = [
        "_UNKNOWN_TOPIC",
        "_NO_OFFSET",
        "_LOG_TRUNCATION",
        "OFFSET_OUT_OF_RANGE",
        "UNKNOWN_TOPIC_OR_PART",
        "NOT_LEADER_FOR_PARTITION",
        "TOPIC_EXCEPTION",
        "NOT_ENOUGH_REPLICAS",
        "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
        "INVALID_COMMIT_OFFSET_SIZE",
        "TOPIC_AUTHORIZATION_FAILED",
        "TOPIC_ALREADY_EXISTS",
        "INVALID_PARTITIONS",
        "INVALID_REPLICATION_FACTOR",
        "INVALID_REPLICA_ASSIGNMENT",
        "REASSIGNMENT_IN_PROGRESS",
        "TOPIC_DELETION_DISABLED",
        "OFFSET_NOT_AVAILABLE",
        "PREFERRED_LEADER_NOT_AVAILABLE",
        "NO_REASSIGNMENT_IN_PROGRESS",
        "GROUP_SUBSCRIBED_TO_TOPIC",
        "UNSTABLE_OFFSET_COMMIT",
        "UNKNOWN_TOPIC_ID",
    ]
    errors = set()
    for name in err_names:
        if hasattr(confluent_kafka.KafkaError, name):
            errors.add(getattr(confluent_kafka.KafkaError, name))
        else:
            logger.debug(f"{name} does not exist in confluent_kafka version "
                         f"{confluent_kafka.__version__} ({confluent_kafka.libversion()})")
    return errors


class KafkaException(Exception):
    @classmethod
    def from_kafka_error(cls, error, msg=None):
        return cls(error, msg)

    topic_related_errors = _get_topic_related_errors()

    def __init__(self, error, msg=None):
        self.error = error
        self.name = error.name()
        self.reason = error.str()
        self.retriable = error.retriable()
        self.fatal = error.fatal()
        self.message = msg
        ex_msg = f"Error communicating with Kafka: code={self.name} {self.reason}"
        if msg and error.code() in KafkaException.topic_related_errors:
            ex_msg += f" on topic {msg.topic()}"
        super(KafkaException, self).__init__(ex_msg)
