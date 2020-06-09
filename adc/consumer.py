from typing import Dict, List, Optional, Iterator, Set
import dataclasses
from datetime import timedelta
import enum
import logging

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
                offset=confluent_kafka.OFFSET_BEGINNING,
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

    def message_stream(self,
                       autocommit: bool = True,
                       batch_size: int = 100,
                       batch_timeout: timedelta = timedelta(seconds=1.0),
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

        The stream stops when the client has hit the last message in all
        partitions. This set of partitions is calculated just once when
        iterate() is first called; calling subscribe() after iterate() may
        cause inconsistent behavior.

        """
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
    PRODUCER = 3

    def __str__(self):
        return self.name.lower()


@dataclasses.dataclass
class ConsumerConfig:
    broker_urls: List[str]
    group_id: str

    # When reading a topic for the first time, where should we start in the
    # stream?
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
            "default.topic.config": {
                "auto.offset.reset": str(self.start_at),
            },
            "enable.auto.commit": True,
            "auto.commit.interval.ms": int(self.offset_commit_interval.total_seconds() * 1000),
            "enable.auto.offset.store": False,
            "queued.min.messages": 1000,
        }
        if self.auth is not None:
            config.update(self.auth())
        return config
