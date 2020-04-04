import unittest

from adc.streaming import AlertBroker

class TestAlertBrokerInitialization(unittest.TestCase):
    def test_producer_rejects_multitopic_urls(self):
        with self.assertRaises(ValueError):
            AlertBroker("kafka://group@broker/topic1,topic2", mode="w")

    def test_consumer_accepts_multitopic_urls(self):
        # Test passes if this raises no error
        AlertBroker("kafka://group@broker/topic1,topic2", mode="r")

    def test_readwrite_rejects_multitopic_urls(self):
        with self.assertRaises(ValueError):
            AlertBroker("kafka://group@broker/topic1,topic2", mode="rw")
