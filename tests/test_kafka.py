import unittest

from adc import kafka


class TestKafkaURLParsing(unittest.TestCase):
    def test_fully_populated(self):
        group, brokers, topic = kafka.parse_kafka_url(
            "kafka://group@broker/topic")
        self.assertEqual(group, "group")
        self.assertListEqual(brokers, ["broker"])
        self.assertEqual(topic, ["topic"])

    def test_multiple_broker(self):
        group, brokers, topic = kafka.parse_kafka_url(
            "kafka://broker1,broker2/topic")
        self.assertIs(group, None)
        self.assertListEqual(brokers, ["broker1", "broker2"])
        self.assertEqual(topic, ["topic"])

    def test_multiple_broker_with_group(self):
        group, brokers, topic = kafka.parse_kafka_url(
            "kafka://group@broker1,broker2/topic")
        self.assertEqual(group, "group")
        self.assertListEqual(brokers, ["broker1", "broker2"])
        self.assertEqual(topic, ["topic"])

    def test_multiple_topics(self):
        group, brokers, topic = kafka.parse_kafka_url(
            "kafka://group@broker1,broker2/topic1,topic2")
        self.assertEqual(group, "group")
        self.assertListEqual(brokers, ["broker1", "broker2"])
        self.assertEqual(topic, ["topic1", "topic2"])

    def test_hostport_address(self):
        group, brokers, topic = kafka.parse_kafka_url(
            "kafka://group@127.0.0.1:9092/topic")
        self.assertEqual(group, "group")
        self.assertListEqual(brokers, ["127.0.0.1:9092"])
        self.assertEqual(topic, ["topic"])

    def test_no_group(self):
        group, brokers, topic = kafka.parse_kafka_url("kafka://broker/topic")
        self.assertIs(group, None)
        self.assertListEqual(brokers, ["broker"])
        self.assertEqual(topic, ["topic"])

    def test_no_topic(self):
        group, brokers, topic = kafka.parse_kafka_url("kafka://group@broker")
        self.assertEqual(group, "group")
        self.assertListEqual(brokers, ["broker"])
        self.assertIs(topic, None)

    def test_no_topic_trailing_slash(self):
        group, brokers, topic = kafka.parse_kafka_url("kafka://group@broker/")
        self.assertEqual(group, "group")
        self.assertListEqual(brokers, ["broker"])
        self.assertIs(topic, None)

    def test_bad_scheme(self):
        with self.assertRaises(ValueError):
            kafka.parse_kafka_url("http://group@broker")

    def test_no_scheme(self):
        with self.assertRaises(ValueError):
            kafka.parse_kafka_url("group@broker")

    def test_empty_scheme(self):
        with self.assertRaises(ValueError):
            kafka.parse_kafka_url("://group@broker")
