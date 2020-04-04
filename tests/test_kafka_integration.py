import unittest
import logging
import tempfile
from datetime import timedelta
import time
import pytest

import docker

import adc.streaming
import adc.auth

logging.basicConfig(level=logging.INFO)


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
            logging.info("polling to wait for container to acquire port...")
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
            logging.info("polling to wait for kafka to come online...")
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
        logging.info("setting up network")
        self.net = self.get_or_create_docker_network()
        logging.info("setting up container")
        self.container = self.get_or_create_container()
        logging.info("getting kafka address")
        self.kafka_address = self.poll_for_kafka_broker_address()

        logging.info("waiting for kafka to come online")
        self.poll_for_kafka_active()

        logging.info("setting up auth")
        self.certfile = tempfile.NamedTemporaryFile(
            prefix="adc-integration-test-",
            suffix=".pem",
            mode="w+b",
        )
        self.certfile.write(self.get_broker_cert(self.container))
        self.certfile.flush()
        logging.info(f"certfile written to {self.certfile.name}")
        self.auth = adc.auth.SASLAuth(
            user="test", password="test-pass",
            ssl_ca_location=self.certfile.name,
        )

    def tearDown(self):
        self.certfile.close()
        logging.info("tearing down container")
        self.container.stop()
        logging.info("tearing down network")
        self.net.remove()

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
        broker = adc.streaming.AlertBroker(
            broker_url="kafka://group@" + self.kafka_address + "/topic",
            mode="rw",
            auth=self.auth,
            start_at="earliest",
            format="blob",
        )
        # Push one message in...
        broker.write("can you hear me?")
        broker.flush()

        # ... and pull it back out.
        msgs = broker.c.consume(1, 5.0)
        self.assertTrue(len(msgs) != 0)
        msg = msgs[0]
        if msg.error() is not None:
            raise Exception(msg.error())

        self.assertEqual(msg.topic(), "topic")
        self.assertEqual(msg.value(), b"can you hear me?")

        # FIXME: The following is how the API *should* work, but fails consistently.
        # have = next(broker(timeout=1))
        # self.assertEqual(have, "can you hear me?")
