import unittest
from fastavro import writer as fastavro_writer
from fastavro import parse_schema as parse_avro_schema
from adc.streaming import AlertBroker, parse_avro
from io import BytesIO


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


class TestAvro(unittest.TestCase):
    def test_parse_avro(self):
        """ Encode some data, and check that adc.streaming.parse_avro can read it."""
        schema = {
            "doc": "A weather reading.",
            "name": "Weather",
            "namespace": "test",
            "type": "record",
            "fields": [
                {"name": "station", "type": "string"},
                {"name": "time", "type": "long"},
                {"name": "temp", "type": "int"},
            ],
        }
        records_in = [
            {u"station": u"011990-99999", u"temp": 0, u"time": 1433269388},
            {u"station": u"011990-99999", u"temp": 22, u"time": 1433270389},
            {u"station": u"011990-99999", u"temp": -11, u"time": 1433273379},
            {u"station": u"012650-99999", u"temp": 111, u"time": 1433275478},
        ]

        parsed_schema = parse_avro_schema(schema)
        with BytesIO() as buf:
            fastavro_writer(buf, parsed_schema, records_in)
            buf.seek(0)
            encoded_bytes = buf.read()

        # Okay, actual test really starts about here.
        records_out = [r for r in parse_avro(encoded_bytes)]

        self.assertEqual(records_in, records_out)
