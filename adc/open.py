from __future__ import annotations

from typing import Union, Iterable, List
import confluent_kafka

from adc import producer, consumer, kafka


def open(url: str, mode: str = 'r') -> Union[producer.Producer, Iterable[confluent_kafka.Message]]:
    group_id, broker_addresses, topics = kafka.parse_kafka_url(url)
    if mode == "r":
        return _open_consumer(group_id, broker_addresses, topics)
    elif mode == "w":
        if len(topics) != 1:
            raise ValueError("must specify exactly one topic in write mode")
        return _open_producer(broker_addresses, topics[0])
    else:
        raise ValueError("mode must be either 'w' or 'r'")


def _open_consumer(
        group_id: str,
        broker_addresses: List[str],
        topics: List[str]) -> Iterable[confluent_kafka.Message]:
    client = consumer.Consumer(consumer.ConsumerConfig(
        broker_urls=broker_addresses,
        group_id=group_id,
    ))
    for t in topics:
        client.subscribe(t)

    for msg in client.message_stream():
        yield msg

    client.close()


def _open_producer(broker_addresses: List[str], topic: str) -> producer.Producer:
    return producer.Producer(producer.ProducerConfig(
        broker_urls=broker_addresses,
        topic=topic,
    ))
