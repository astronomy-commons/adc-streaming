import unittest
import logging
import tempfile
from datetime import timedelta
import time

import docker

import genesis.streaming
import genesis.auth

logging.basicConfig(level=logging.INFO)


class KafkaIntegrationTestCase(unittest.TestCase):
    docker_client = docker.from_env()

    def poll_for_kafka_broker_address(self, maxiter=20, sleep=timedelta(milliseconds=500)):
        i = 0
        while (not self.query_kafka_broker_address()) and i < maxiter:
            logging.info("polling to wait for container to acquire port...")
            time.sleep(sleep.total_seconds())
            i = i + 1
            self.container.reload()
        assert i < maxiter
        raw_addr = self.query_kafka_broker_address()
        ip = raw_addr[0]['HostIp']
        port = raw_addr[0]['HostPort']
        return f"{ip}:{port}"

    def query_kafka_broker_address(self):
        return self.container.attrs.get("NetworkSettings", {}).get("Ports", {}).get("9092/tcp", [])

    def poll_for_kafka_active(self, maxiter=20, sleep=timedelta(milliseconds=500)):
        i = 0
        while (not self.query_kafka_active()) and i < maxiter:
            logging.info("polling to wait for kafka to come online...")
            time.sleep(sleep.total_seconds())
            i = i + 1
        assert i < maxiter

    def query_kafka_active(self):
        exit_code, _ = self.container.exec_run(
            "/bin/bash -c 'nc localhost 9092 -w 5'",
        )
        return exit_code == 0

    def get_broker_cert(self, container):
        code, output = container.exec_run(
            "/bin/cat /root/shared/tls/cacert.pem")
        if code != 0:
            raise AssertionError(b"failed to get broker cert:" + output)
        return output

    def setUp(self):
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
            prefix="genesis-integration-test-",
            suffix=".pem",
            mode="w+b",
        )
        self.certfile.write(self.get_broker_cert(self.container))
        self.certfile.flush()
        logging.info(f"certfile written to {self.certfile.name}")
        self.auth = genesis.auth.SASLAuth(
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
        containers = self.docker_client.containers.list(
            filters={"name": "genesis-integration-test-server"},
        )
        if containers:
            return containers[0]

        return self.docker_client.containers.run(
            image="scimma/server:latest",
            name="genesis-integration-test-server",
            detach=True,
            auto_remove=True,
            network=self.net.name,
            ports={"9092/tcp": None},
        )

    def get_or_create_docker_network(self):
        nets = self.docker_client.networks.list(
            names="genesis-integration-test")
        if nets:
            return nets[0]
        return self.docker_client.networks.create(name="genesis-integration-test")

    def test_round_trip(self):
        broker = genesis.streaming.AlertBroker(
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
