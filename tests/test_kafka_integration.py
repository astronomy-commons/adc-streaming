import unittest
import logging
import tempfile
from datetime import timedelta
import time
import pytest

import docker

import adc.consumer
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
    docker_client = docker.from_env()

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

    def setUp(self):
        """Runs all pre-test setup: starts a Docker container running a Kafka broker,
        waits for it to come online, and prepares authentication credentials for
        connecting to the broker.

        """
        logger.info("setting up network")
        self.net = self.get_or_create_docker_network()
        logger.info("setting up container")
        self.container = self.get_or_create_container()
        logger.info("getting kafka address")
        self.kafka_address = self.poll_for_kafka_broker_address()

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

    def tearDown(self):
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

    def test_round_trip(self):
        """Try writing a message into the Kafka broker, and try pulling the same
        message back out.

        """
        topic = "test_round_trip"

        producer = adc.producer.Producer(adc.producer.ProducerConfig(
            broker_urls=[self.kafka_address],
            topic=topic,
            auth=self.auth,
        ))

        # Push one message in...
        producer.write("can you hear me?")
        enqueued = producer.flush()
        # All messages should have been sent...
        self.assertEqual(enqueued, 0)

        # ... and pull it back out.
        consumer = adc.consumer.Consumer(adc.consumer.ConsumerConfig(
            broker_urls=[self.kafka_address],
            group_id="test_consumer",
            auth=self.auth,
        ))
        consumer.subscribe(topic)
        stream = consumer.message_stream()

        msg = next(stream)
        if msg.error() is not None:
            raise Exception(msg.error())

        self.assertEqual(msg.topic(), topic)
        self.assertEqual(msg.value(), b"can you hear me?")

    @unittest.skip("skipping due to bug in librdkafka")
    def test_consume_from_end(self):
        # Write a few messages.
        topic = "test_consume_from_end"
        producer = adc.producer.Producer(adc.producer.ProducerConfig(
            broker_urls=[self.kafka_address],
            topic=topic,
            auth=self.auth,
        ))
        producer.write("message 1")
        producer.write("message 2")
        producer.write("message 3")
        enqueued = producer.flush()
        self.assertEqual(enqueued, 0)

        # Start a consumer from the end position
        consumer = adc.consumer.Consumer(adc.consumer.ConsumerConfig(
            broker_urls=[self.kafka_address],
            group_id="test_consumer",
            auth=self.auth,
            start_at=adc.consumer.ConsumerStartPosition.LATEST,
            read_forever=False,
        ))
        consumer.subscribe(topic)
        stream = consumer.message_stream()

        # Now add messages after the "end"
        producer.write("message 4")
        enqueued = producer.flush()
        self.assertEqual(enqueued, 0)

        msg = next(stream)
        self.assertEqual(msg.topic(), topic)
        self.assertEqual(msg.value(), b"message 4")

    def test_consume_from_specified_offset(self):
        self.skipTest("skipping due to bug in librdkafka")
        # Write a few messages.
        topic = "test_consume_from_end"
        producer = adc.producer.Producer(adc.producer.ProducerConfig(
            broker_urls=[self.kafka_address],
            topic=topic,
            auth=self.auth,
        ))
        producer.write("message 1")
        producer.write("message 2")
        producer.write("message 3")
        producer.write("message 4")
        enqueued = producer.flush()
        self.assertEqual(enqueued, 0)

        # Start a consumer from the third message (offset '2')
        consumer = adc.consumer.Consumer(adc.consumer.ConsumerConfig(
            broker_urls=[self.kafka_address],
            group_id="test_consumer",
            auth=self.auth,
            start_at=2,
        ))
        consumer.subscribe(topic)
        stream = consumer.message_stream()

        msg = next(stream)
        self.assertEqual(msg.topic(), topic)
        self.assertEqual(msg.value(), b"message 3")
        msg = next(stream)
        self.assertEqual(msg.topic(), topic)
        self.assertEqual(msg.value(), b"message 4")
