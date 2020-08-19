import dataclasses
import enum
import logging
from datetime import timedelta
from typing import Dict, Iterator, List, Optional, Set

import confluent_kafka  # type: ignore
import confluent_kafka.admin  # type: ignore

from .auth import SASLAuth
from .errors import ErrorCallback, log_client_errors


class Consumer:
    conf: 'ConsumerConfig'
    _consumer: confluent_kafka.Consumer
    logger: logging.Logger

    def __init__(self, conf: 'ConsumerConfig') -> None:
        self.logger = logging.getLogger("adc-streaming.consumer")
        self.conf = conf
        self._consumer = confluent_kafka.Consumer(conf._to_confluent_kafka())

    def subscribe(self,
                  topic: str,
                  timeout: timedelta = timedelta(seconds=10)):
        """Subscribes to a topic for consuming. This method doesn't use Kafka's
        Consumer Groups; it assigns all partitions manually to this
        process.

        The topic must already exist for the subscription to succeed.
        """
        self.logger.debug(f"subscribing to topic {topic}")

        try:
            topic_meta = self.describe_topic(topic, timeout)
        except KeyError:
            raise ValueError(f"topic {topic} does not exist on the broker, so can't subscribe")

        assignment = []
        for partition_id in topic_meta.partitions.keys():
            self.logger.debug(f"adding subscription to topic partition={partition_id}")
            tp = confluent_kafka.TopicPartition(
                topic=topic,
                partition=partition_id,
            )
            assignment.append(tp)

        self.logger.debug("registering topic assignment")
        self._consumer.assign(assignment)

    def describe_topic(
            self,
            topic: str,
            timeout: timedelta = timedelta(seconds=5.0)) -> confluent_kafka.admin.TopicMetadata:
        """Fetch confluent_kafka.admin.TopicMetadata describing a topic.
        """
        self.logger.debug(f"fetching cluster metadata to describe topic name={topic}")
        cluster_meta = self._consumer.list_topics(timeout=timeout.total_seconds())
        self.logger.debug(f"cluster metadata: {cluster_meta.topics}")
        return cluster_meta.topics[topic]

    def mark_done(self, msg: confluent_kafka.Message):
        """
        Mark a message as fully-processed. In the background, the client will
        continuously synchronize this information with Kafka so that the stream can be
        resumed from this point in the future.
        """
        self._consumer.store_offsets(msg)

    def stream(self,
               autocommit: bool = True,
               batch_size: int = 100,
               batch_timeout: timedelta = timedelta(seconds=1.0)
               ) -> Iterator[confluent_kafka.Message]:
        """Returns a stream which iterates over the messages in the topics
        to which the client is subscribed.

        If autocommit is true, then messages are automatically marked as handled
        when the next message is yielded. This removes the need to call
        'mark_done' on each message. Callers using asynchronous message
        processing or with complex processing needs should disable this.

        batch_size controls the number of messages to request from Kafka per
        batch. Higher values may be more efficient, but may add latency.

        batch_timeout controls how long the client should wait for Kafka to
        provide a full batch of batch_size messages. Higher values may be more
        efficient, but may add latency.

        If the consumer's configuration has read_forever set to False, then the
        stream stops when the client has hit the last message in all partitions.
        This set of partitions is calculated just once when iterate() is first
        called; calling subscribe() after iterate() may cause inconsistent
        behavior in this case.

        """
        if self.conf.read_forever:
            return self._stream_forever(autocommit, batch_size, batch_timeout)
        else:
            return self._stream_until_eof(autocommit, batch_size, batch_timeout)

    def _stream_forever(self,
                        autocommit: bool = True,
                        batch_size: int = 100,
                        batch_timeout: timedelta = timedelta(seconds=1.0),
                        ) -> Iterator[confluent_kafka.Message]:
        last_message: confluent_kafka.Message = None
        while True:
            messages = self._consumer.consume(batch_size, batch_timeout.total_seconds())
            for m in messages:
                err = m.error()
                if err is None:
                    self.logger.debug(f"read message from partition {m.partition()}")
                    yield m
                    # Automatically mark message as processed, if desired
                    if autocommit and last_message is not None:
                        self.mark_done(last_message)
                        last_message = m
                else:
                    raise(confluent_kafka.KafkaException(err))

    def _stream_until_eof(self,
                          autocommit: bool = True,
                          batch_size: int = 100,
                          batch_timeout: timedelta = timedelta(seconds=1.0),
                          ) -> Iterator[confluent_kafka.Message]:
        assignment = self._consumer.assignment()

        # Make a map of topic-name -> set of partition IDs we're assigned to.
        # When we hit a partition EOF, remove that partition from the map.
        active_partitions: Dict[str, Set[int]] = {}
        for tp in assignment:
            if tp.topic not in active_partitions:
                active_partitions[tp.topic] = set()
            self.logger.debug(f"tracking until eof for topic={tp.topic} partition={tp.partition}")
            active_partitions[tp.topic].add(tp.partition)

        last_message: confluent_kafka.Message = None
        while len(active_partitions) > 0:
            messages = self._consumer.consume(batch_size, batch_timeout.total_seconds())
            for m in messages:
                err = m.error()
                if err is None:
                    self.logger.debug(f"read message from partition {m.partition()}")
                    yield m
                    # Automatically mark message as processed, if desired
                    if autocommit and last_message is not None:
                        self.mark_done(last_message)
                        last_message = m
                elif err.code() == confluent_kafka.KafkaError._PARTITION_EOF:
                    self.logger.debug(f"eof for topic={m.topic()} partition={m.partition()}")
                    # Done with this partition, remove it
                    partition_set = active_partitions[m.topic()]
                    partition_set.remove(m.partition())
                    if len(partition_set) == 0:
                        # Done with all partitions for the topic, remove it
                        del active_partitions[m.topic()]
                else:
                    raise(confluent_kafka.KafkaException(err))

    def close(self):
        """ Close the consumer, ending its subscriptions. """
        self._consumer.close()


class ConsumerStartPosition(enum.Enum):
    EARLIEST = 1
    LATEST = 2

    def __str__(self):
        return self.name.lower()


@dataclasses.dataclass
class ConsumerConfig:
    broker_urls: List[str]
    group_id: str

    # When we have reached the last message on a topic, should we hold the
    # stream open to wait for more messages?
    read_forever: bool = True

    # When reading a topic for the first time, where should we start in the
    # stream? Note that, if the topic has already been consumed under the
    # provided group_id, then consumption will start after the last message that
    # was marked done with consumer.mark_done, regardless of this setting. This
    # is only used when the position in the stream is unknown.
    #
    # This is specified as a logical offset via a ConsumerStartPosition value.
    start_at: ConsumerStartPosition = ConsumerStartPosition.EARLIEST

    # Authentication package to pass in to read from Kafka.
    auth: Optional[SASLAuth] = None

    # Callback to execute whenever an internal Kafka error occurs.
    error_callback: Optional[ErrorCallback] = log_client_errors

    # How often should we save our progress to Kafka?
    offset_commit_interval: timedelta = timedelta(seconds=5)

    def _to_confluent_kafka(self) -> Dict:
        config = {
            "bootstrap.servers": ",".join(self.broker_urls),
            "error_cb": self.error_callback,
            "group.id": self.group_id,
            "enable.auto.commit": True,
            "auto.commit.interval.ms": int(self.offset_commit_interval.total_seconds() * 1000),
            "enable.auto.offset.store": False,
            "queued.min.messages": 1000,
            "enable.partition.eof": not self.read_forever,
        }
        if self.start_at is ConsumerStartPosition.EARLIEST:
            default_topic_config = config.get("default.topic.config", {})
            default_topic_config = {
                "auto.offset.reset": "EARLIEST",
            }
            config["default.topic.config"] = default_topic_config
        elif self.start_at is ConsumerStartPosition.LATEST:
            # FIXME: librdkafka has a bug in offset handling - it caches
            # "OFFSET_END", and will repeatedly move to the end of the
            # topic. See https://github.com/edenhill/librdkafka/pull/2876 -
            # it should get fixed in v1.5 of librdkafka.

            librdkafka_version = confluent_kafka.libversion()[0]
            if librdkafka_version < "1.5.0":
                self.logger.warn(
                    "In librdkafka before v1.5, LATEST offsets have buggy behavior; you may "
                    f"not receive data (your librdkafka version is {librdkafka_version}). See "
                    "https://github.com/confluentinc/confluent-kafka-dotnet/issues/1254.")
            default_topic_config = config.get("default.topic.config", {})
            default_topic_config = {
                "auto.offset.reset": "LATEST",
            }
            config["default.topic.config"] = default_topic_config

        if self.auth is not None:
            config.update(self.auth())
        return config
