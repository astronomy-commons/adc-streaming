import logging
import warnings
from contextlib import contextmanager
from typing import Iterable, List, Optional, Union

import confluent_kafka  # type: ignore

from adc import auth, consumer, errors, kafka, producer

logger = logging.getLogger("adc-streaming")


def open(url: str,
         mode: str = 'r',
         auth: Optional[auth.SASLAuth] = None,
         start_at: consumer.ConsumerStartPosition = consumer.ConsumerStartPosition.EARLIEST,  # noqa: E501
         read_forever: bool = True,
         ) -> Union[producer.Producer, Iterable[confluent_kafka.Message]]:
    group_id, broker_addresses, topics = kafka.parse_kafka_url(url)
    logger.debug("connecting to addresses=%s  group_id=%s  topics=%s",
                 broker_addresses, group_id, topics)
    if mode == "r":
        if group_id is None:
            raise ValueError("group ID must be set when in reader mode")
        return _open_consumer(group_id, broker_addresses, topics, auth, start_at, read_forever)
    elif mode == "w":
        if len(topics) != 1:
            raise ValueError("must specify exactly one topic in write mode")
        if group_id is not None:
            warnings.warn("group ID has no effect when opening a stream in write mode")
        if start_at is not consumer.ConsumerStartPosition.EARLIEST:
            warnings.warn("start_at has no effect when opening a stream in write mode")
        if read_forever is not True:
            warnings.warn("read_forever has no effect when opening a stream in write mode")
        return _open_producer(broker_addresses, topics[0], auth)
    else:
        raise ValueError("mode must be either 'w' or 'r'")


@contextmanager
def _open_consumer(
        group_id: str,
        broker_addresses: List[str],
        topics: List[str],
        auth: Optional[auth.SASLAuth],
        start_at: consumer.ConsumerStartPosition,
        read_forever: bool,
) -> Iterable[confluent_kafka.Message]:
    client = consumer.Consumer(consumer.ConsumerConfig(
        broker_urls=broker_addresses,
        group_id=group_id,
        auth=auth,
        start_at=start_at,
        read_forever=read_forever,
    ))
    for t in topics:
        client.subscribe(t)

    try:
        yield client.stream()
    finally:
        client.close()


def _open_producer(
        broker_addresses: List[str],
        topic: str,
        auth: Optional[auth.SASLAuth],
) -> producer.Producer:
    return producer.Producer(producer.ProducerConfig(
        broker_urls=broker_addresses,
        topic=topic,
        auth=auth,
        delivery_callback=errors.raise_delivery_errors,
    ))
