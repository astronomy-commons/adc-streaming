import logging
import tempfile
import time
import unittest
from datetime import timedelta
from typing import List

import docker
import pytest

import adc.consumer
import adc.io
import adc.producer

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("adc-streaming").setLevel(logging.DEBUG)
logger = logging.getLogger("adc-streaming.tests")


@pytest.mark.integration_test
class KafkaIntegrationTestCase(unittest.TestCase):
    """This test runs a Kafka broker in a Docker container, and makes sure that
    messages can round-trip through that broker, testing both the Producer and
    Consumer sides of the library.

    It can be pretty slow, since it has to set up a container and wait for Kafka
    to come online.

    """
    @classmethod
    def setUpClass(cls):
        cls.kafka = KafkaDockerConnection()

    @classmethod
    def tearDownClass(cls):
        cls.kafka.close()

    def test_round_trip(self):
        """Try writing a message into the Kafka broker, and try pulling the same
        message back out.

        """
        topic = "test_round_trip"
        # Push one message in...
        simple_write_msg(self.kafka, topic, "can you hear me?")
        # ... and pull it back out.
        consumer = adc.consumer.Consumer(adc.consumer.ConsumerConfig(
            broker_urls=[self.kafka.address],
            group_id="test_consumer",
            auth=self.kafka.auth,
        ))
        consumer.subscribe(topic)
        stream = consumer.stream()

        msg = next(stream)
        if msg.error() is not None:
            raise Exception(msg.error())

        self.assertEqual(msg.topic(), topic)
        self.assertEqual(msg.value(), b"can you hear me?")

    @unittest.skip("skipping due to bug in librdkafka")
    def test_consume_from_end(self):
        # Write a few messages.
        topic = "test_consume_from_end"
        simple_write_msgs(self.kafka, topic, [
            "message 1",
            "message 2",
            "message 3",
        ])
        # Start a consumer from the end position
        consumer = adc.consumer.Consumer(adc.consumer.ConsumerConfig(
            broker_urls=[self.kafka.address],
            group_id="test_consumer",
            auth=self.kafka.auth,
            start_at=adc.consumer.ConsumerStartPosition.LATEST,
        ))
        consumer.subscribe(topic)
        stream = consumer.stream()

        # Now add messages after the "end"
        simple_write_msg(self.kafka, topic, "message 4")

        msg = next(stream)
        self.assertEqual(msg.topic(), topic)
        self.assertEqual(msg.value(), b"message 4")

    def test_consume_from_beginning(self):
        # Write a few messages.
        topic = "test_consume_from_beginning"
        batch = [
            "message 1",
            "message 2",
            "message 3",
            "message 4",
        ]
        simple_write_msgs(self.kafka, topic, batch)

        # Start a consumer from the beginning.
        consumer = adc.consumer.Consumer(adc.consumer.ConsumerConfig(
            broker_urls=[self.kafka.address],
            group_id="test_consumer",
            auth=self.kafka.auth,
            read_forever=False,
            start_at=adc.consumer.ConsumerStartPosition.EARLIEST,
        ))
        consumer.subscribe(topic)
        stream = consumer.stream()
        msgs = [msg for msg in stream]

        assert consumer._stop_event.is_set()
        self.assertEqual(len(batch), len(msgs))
        for expected, actual in zip(batch, msgs):
            self.assertEqual(actual.topic(), topic)
            self.assertEqual(actual.value().decode(), expected)

    def test_consume_stored_offsets(self):
        # Write first batch of messages.
        topic = "test_stored_offsets"
        batch_1 = [
            "message 1",
            "message 2",
            "message 3",
            "message 4",
        ]
        simple_write_msgs(self.kafka, topic, batch_1)

        # Start first consumer, reading from earliest offset.
        consumer_1 = adc.consumer.Consumer(adc.consumer.ConsumerConfig(
            broker_urls=[self.kafka.address],
            group_id="test_consumer_1",
            auth=self.kafka.auth,
            read_forever=False,
            start_at=adc.consumer.ConsumerStartPosition.EARLIEST,
        ))
        consumer_1.subscribe(topic)
        stream_1 = consumer_1.stream()
        msgs_1 = [msg for msg in stream_1]

        # Check that all messages from first batch are processed.
        assert consumer_1._stop_event.is_set()
        self.assertEqual(len(batch_1), len(msgs_1))
        for expected, actual in zip(batch_1, msgs_1):
            self.assertEqual(actual.topic(), topic)
            self.assertEqual(actual.value().decode(), expected)

        # Write second batch of messages.
        batch_2 = [
            "message 5",
            "message 6",
            "message 7",
        ]
        simple_write_msgs(self.kafka, topic, batch_2)

        # Read more messages from first consumer. This should now
        # only read from the stored offset, so that only the second
        # batch is processed.
        stream_1 = consumer_1.stream()
        msgs_1 = [msg for msg in stream_1]
        assert consumer_1._stop_event.is_set()
        self.assertEqual(len(batch_2), len(msgs_1))
        for expected, actual in zip(batch_2, msgs_1):
            self.assertEqual(actual.topic(), topic)
            self.assertEqual(actual.value().decode(), expected)

        # Start second consumer, also reading from earliest offset.
        consumer_2 = adc.consumer.Consumer(adc.consumer.ConsumerConfig(
            broker_urls=[self.kafka.address],
            group_id="test_consumer_2",
            auth=self.kafka.auth,
            read_forever=False,
            start_at=adc.consumer.ConsumerStartPosition.EARLIEST,
        ))
        consumer_2.subscribe(topic)
        stream_2 = consumer_2.stream()
        msgs_2 = [msg for msg in stream_2]

        # Now check that messages from both batches are processed.
        assert consumer_2._stop_event.is_set()
        self.assertEqual(len(batch_1 + batch_2), len(msgs_2))
        for expected, actual in zip(batch_1 + batch_2, msgs_2):
            self.assertEqual(actual.topic(), topic)
            self.assertEqual(actual.value().decode(), expected)

    def test_consume_not_forever(self):
        topic = "test_consume_not_forever"
        simple_write_msg(self.kafka, topic, "message 1")

        consumer = adc.consumer.Consumer(adc.consumer.ConsumerConfig(
            broker_urls=[self.kafka.address],
            group_id="test_consumer",
            auth=self.kafka.auth,
            read_forever=False
        ))
        consumer.subscribe(topic)
        stream = consumer.stream()

        msg = next(stream)
        if msg.error() is not None:
            raise Exception(msg.error())
        self.assertEqual(msg.topic(), topic)
        self.assertEqual(msg.value(), b"message 1")
        assert not consumer._stop_event.is_set()
        with self.assertRaises(StopIteration):
            next(stream)
        assert consumer._stop_event.is_set()

    def test_consumer_terminating_in_thread(self):
        topic = "test_consume_forever_in_thread"
        simple_write_msgs(
            self.kafka, topic, ["message 1", "message 2", "message 3"])

        consumer = adc.consumer.Consumer(adc.consumer.ConsumerConfig(
            broker_urls=[self.kafka.address],
            group_id="test_consumer",
            auth=self.kafka.auth,
            read_forever=True
        ))
        consumer.subscribe(topic)

        import threading
        t = threading.Thread(
            target=lambda c: {_ for _ in c.stream()}, args=(consumer,),
            name="ListenerThread")
        t.start()
        # stop listener
        consumer.stop()
        t.join()
        assert t.is_alive() is False

    def test_contextmanager_support(self):
        topic = "test_contextmanager_support"
        url = f"kafka://{self.kafka.address}/{topic}"
        with adc.io.open(url, mode="w", auth=self.kafka.auth) as p:
            p.write("message 1")
            p.write("message 2")
            p.write("message 3")

        logger.info("done with writes")

        group = "test_contextmanager_group"
        url = f"kafka://{group}@{self.kafka.address}/{topic}"
        with adc.io.open(url, mode="r", auth=self.kafka.auth, read_forever=False) as stream:
            messages = [m for m in stream]
        logger.info("done with reads")
        self.assertEqual(len(messages), 3)
        self.assertEqual(messages[0].value(), b"message 1")
        self.assertEqual(messages[1].value(), b"message 2")
        self.assertEqual(messages[2].value(), b"message 3")


class KafkaDockerConnection:
    """Holds connection information for communicating with a Kafka broker running
    inside a docker container.

    """

    def __init__(self):
        """Starts a Docker container running a Kafka broker, waits for it to come
        online, and prepares authentication credentials for connecting to the
        broker.

        """
        self.docker_client = docker.from_env()

        logger.info("setting up network")
        self.net = self.get_or_create_docker_network()
        logger.info("setting up container")
        self.container = self.get_or_create_container()
        logger.info("getting kafka address")
        self.address = self.poll_for_kafka_broker_address()

        logger.info("waiting for kafka to come online")
        self.poll_for_kafka_active()

        logger.info("setting up auth")
        self.certfile = tempfile.NamedTemporaryFile(
            prefix="adc-integration-test-",
            suffix=".pem",
            mode="w+b",
        )
        self.certfile.write(self.get_broker_cert(self.container))
        self.certfile.flush()
        logger.info(f"certfile written to {self.certfile.name}")
        self.auth = adc.auth.SASLAuth(
            user="test", password="test-pass",
            ssl_ca_location=self.certfile.name,
        )

    def poll_for_kafka_broker_address(self, maxiter=20, sleep=timedelta(milliseconds=500)):
        """Block until the Docker daemon tells us the IP and Port of the Kafa broker.

        Returns the ip and port as a string in the form "ip:port."
"""
        i = 0
        while (not self.query_kafka_broker_address()) and i < maxiter:
            logger.info("polling to wait for container to acquire port...")
            time.sleep(sleep.total_seconds())
            i = i + 1
            self.container.reload()
        assert i < maxiter
        return self.query_kafka_broker_address()

    def query_kafka_broker_address(self):
        """Ask the Docker API for the exposed port of the Kafka broker."""
        addrs = self.container.attrs.get("NetworkSettings", {}).get(
            "Ports", {}).get("9092/tcp", [])
        if not addrs:
            return None
        ip = addrs[0]['HostIp']
        port = addrs[0]['HostPort']
        return f"{ip}:{port}"

    def poll_for_kafka_active(self, maxiter=20, sleep=timedelta(milliseconds=500)):
        """Block until Kafka's network listener is accepting connections."""
        i = 0
        while (not self.query_kafka_active()) and i < maxiter:
            logger.info("polling to wait for kafka to come online...")
            time.sleep(sleep.total_seconds())
            i = i + 1
        assert i < maxiter

    def query_kafka_active(self):
        """Returns True if the Kafka broker's listener is accepting connections.

        This works by running netcat within the container and checking its exit code.
        """
        exit_code, _ = self.container.exec_run(
            "/bin/nc localhost 9092",
        )
        return exit_code == 0

    def get_broker_cert(self, container):
        """Returns the byte string contents of the generated TLS certificate
        used by the broker.
        """
        code, output = container.exec_run(
            "/bin/cat /root/shared/tls/cacert.pem")
        if code != 0:
            raise AssertionError(b"failed to get broker cert:" + output)
        return output

    def close(self):
        """Closes open files, shuts down containers, and tears down the docker network.

        """
        self.certfile.close()
        logger.info("tearing down container")
        self.container.stop()
        logger.info("tearing down network")
        self.net.remove()

    def exec_in_container(self, cmd):
        exit_code, output = self.container.exec_run(cmd)
        msg = f"Exit error running {cmd}: {output}"
        self.assertEqual(exit_code, 0, msg)
        return output

    def get_or_create_container(self):
        """Starts a scimma/server container named 'adc-integration-test-server' and
        returns a handle referencing the container. If a container with that
        name is already running, it's returned instead.
        """
        containers = self.docker_client.containers.list(
            filters={"name": "adc-integration-test-server"},
        )
        if containers:
            return containers[0]

        return self.docker_client.containers.run(
            image="scimma/server:latest",
            name="adc-integration-test-server",
            detach=True,
            auto_remove=True,
            network=self.net.name,
            # Setting None below the OS pick an ephemeral port.
            ports={"9092/tcp": None},
        )

    def get_or_create_docker_network(self):
        """Returns a docker network named adc-integration-test, creating it if it
        doesn't exist already.

        """
        nets = self.docker_client.networks.list(
            names="adc-integration-test")
        if nets:
            return nets[0]
        return self.docker_client.networks.create(name="adc-integration-test")


def simple_write_msg(conn: KafkaDockerConnection, topic: str, msg: str):
    producer = adc.producer.Producer(adc.producer.ProducerConfig(
        broker_urls=[conn.address],
        topic=topic,
        auth=conn.auth,
    ))
    producer.write(msg)
    producer.flush()


def simple_write_msgs(conn: KafkaDockerConnection, topic: str, msgs: List[str]):
    producer = adc.producer.Producer(adc.producer.ProducerConfig(
        broker_urls=[conn.address],
        topic=topic,
        auth=conn.auth,
    ))
    for m in msgs:
        producer.write(m)
    producer.flush()
