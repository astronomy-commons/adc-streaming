from __future__ import annotations

from typing import Dict, List, Optional, Iterator, Set
import dataclasses
from datetime import timedelta
import enum
import logging

import confluent_kafka  # type: ignore

from .auth import SASLAuth
from .errors import ErrorCallback, log_client_errors


class Consumer:
    conf: ConsumerConfig
    _consumer: confluent_kafka.Consumer
    logger: logging.Logger

    def __init__(self, conf: ConsumerConfig) -> None:
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
            timeout: timedelta = timedelta(seconds=5.0)) -> confluent_kafka.TopicMetadata:
        """Fetch confluent_kafka.TopicMetadata describing a topic.
        """
        self.logger.debug(f"fetching cluster metadata to describe topic name={topic}")
        cluster_meta = self._consumer.list_topics(timeout=timeout.total_seconds())
        self.logger.debug(f"cluster metadata: {cluster_meta.topics}")
        return cluster_meta.topics[topic]

    def message_stream(self,
                       batch_size: int = 100,
                       batch_timeout: timedelta = timedelta(seconds=1.0),
                       ) -> Iterator[confluent_kafka.Message]:
        """Returns a stream which iterates over the messages in the topics
        to which the client is subscribed.

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

        while len(active_partitions) > 0:
            messages = self._consumer.consume(batch_size, batch_timeout.total_seconds())
            for m in messages:
                err = m.error()
                if err is None:
                    self.logger.debug(f"read message from partition {m.partition()}")
                    yield m
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

    start_at: ConsumerStartPosition = ConsumerStartPosition.EARLIEST
    auth: Optional[SASLAuth] = None
    error_callback: Optional[ErrorCallback] = log_client_errors

    def _to_confluent_kafka(self) -> Dict:
        config = {
            "bootstrap.servers": ",".join(self.broker_urls),
            "error_cb": self.error_callback,
            "group.id": self.group_id,
            "default.topic.config": {
                "auto.offset.reset": str(self.start_at),
            },
            "enable.auto.commit": True,
            "queued.min.messages": 1000,
        }
        if self.auth is not None:
            config.update(self.auth())
        return config
