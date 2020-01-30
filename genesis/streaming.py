#!/usr/bin/env python

import sys, io, time, argparse, string, signal, itertools, os.path
import json, base64
import fastavro, fastavro.write

from confluent_kafka import Consumer, KafkaError, TopicPartition, Producer
from contextlib import contextmanager
from collections import namedtuple

from multiprocessing import Pool as MPPool

# FIXME: Make this into a proper class (safety in the unlikely case the user returns HEARTBEAT_SENTINEL)
HEARTBEAT_SENTINEL = "__heartbeat__"
def is_heartbeat(msg):
	return isinstance(msg, str) and msg == HEARTBEAT_SENTINEL

assert is_heartbeat(HEARTBEAT_SENTINEL)

def _noop(msg):
	return msg

## Message parsing

def parse_avro(val):
	with io.BytesIO(val) as fp:
		rr = fastavro.reader(fp)
		for record in rr:
			yield rr

def parse_json(val):
	yield json.loads(val)

def parse_blob(val):
	yield val

_MESSAGE_PARSERS = {
	'avro': parse_avro,
	'json': parse_json,
	'blob': parse_blob
}

## Message serialization

def serialize_json(val):
	return json.dumps(val)

def serialize_blob(val):
	return val

_MESSAGE_SERIALIZERS = {
	'json': serialize_json,
	'blob': serialize_blob
}


##

class ParseAndFilter:
	def __init__(self, parser, filter):
		self.parser = parser if parser is not None else parse_blob
		self.filter = filter if filter is not None else _noop

	def __call__(self, msg):
		topic, part, offs, val = msg
		for record in self.parser(val):
			return topic, part, offs, self.filter(record)

## URL parsing

def parse_kafka_url(val, allow_no_topic=False):
	assert val.startswith("kafka://")

	val = val[len("kafka://"):]

	try:
		(groupid_brokers, topics) = val.split('/')
	except ValueError:
		if not allow_no_topic:
			raise argparse.ArgumentError(self, f'A kafka:// url must be of the form kafka://[groupid@]broker[,broker2[,...]]/topicspec[,topicspec[,...]].')
		else:
			groupid_brokers, topics = val, None

	try:
		(groupid, brokers) = groupid_brokers.split('@')
	except ValueError:
		(groupid, brokers) = (None, groupid_brokers)

	topics = topics.split(',') if topics is not None else []
	
	return (groupid, brokers, topics)


def open(url, mode='r', **kwargs):
	return AlertBroker(url, mode, **kwargs)

class AlertBroker:
	c = None
	p = None

	def __init__(self, broker_url, mode='r', start_at='latest', format='avro'):
		self.groupid, self.brokers, self.topics = parse_kafka_url(broker_url)

		# mode can be 'r', 'w', or 'rw'; other characters are ignored
		assert 'r' in mode or 'w' in mode

		if self.groupid is None:
			# if groupid hasn't been given, emulate a low-level consumer:
			#   - generate a random groupid
			#   - disable committing (so we never commit the random groupid)
			#   - start reading from the earliest (smallest) offset
			import getpass, random
			self.groupid = getpass.getuser() + '-' + ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(20))
#			print(f"Generated fake groupid {self.groupid}")

		if 'r' in mode:
			self.c = Consumer({
				'bootstrap.servers': self.brokers,
				'group.id': self.groupid,
				'default.topic.config': {
					'auto.offset.reset': start_at
				},
				'enable.auto.commit': False,
				'queued.min.messages': 1000,
			})

			self.c.subscribe(self.topics)
			self._parser = _MESSAGE_PARSERS[format]		# message deserializer

		if 'w' in mode:
			self.p = Producer({
				'bootstrap.servers': self.brokers,
			})
			self._serialize = _MESSAGE_SERIALIZERS[format]	# message serializer

		self._buffer = {}		# local raw message buffer

		self._idx = 0

		self._consumed = {}		# locally consumed (not necessarily committe)
		self._committed = {}	# locally committed, but not yet committed on kafka
		self.last_commit_time = None

	# context manager protocol
	def __enter__(self):
		return self

	def __exit__(self, type, value, traceback):
		self.close()
		if type == KeyboardInterrupt:
			print("Aborted (CTRL-C).")
			return True

	def close(self):
		if self.c:
			self._commit_to_kafka(defer=False)
			self.c.unsubscribe()
			self.c.close()
			self.c = None
		if self.p:
			self.p.flush()

	def _raw_stream(self, timeout):
		last_msg_time = time.time()
		while True:
			msgs = self.c.consume(1000, 1.0)
			if not msgs:
				if timeout and (time.time() - last_msg_time >= timeout):
					return
				yield HEARTBEAT_SENTINEL
			else:
				# check for errors
				for msg in msgs:
					if msg.error() is None:
						continue

					if msg.error().code() == KafkaError._PARTITION_EOF:
						continue

					raise Exception(msg.error())
				yield msgs

				# reset the timeout counter
				last_msg_time = time.time()

			# actualy commit any offsets the user committed
			self._commit_to_kafka(defer=True)

	# returns a generator returning deserialized, user-processed, messages + heartbeats
	def _filtered_stream(self, mapper, filter, timeout):
		t_last = time.time()

		for msgs in itertools.chain([self._buffer.values()], self._raw_stream(timeout=timeout)):
			if is_heartbeat(msgs):
				yield None, msgs
			else:
				self._buffer = dict( enumerate(msgs) )

				# unpack and copy so we don't pickle the world (if multiprocessing)
				msgs = [ (msg.topic(), msg.partition(), msg.offset(), msg.value()) for msg in msgs ]

				# process the messages on the workers
				for i, (topic, part, offs, rec) in enumerate(mapper(ParseAndFilter(parser=self._parser, filter=filter), msgs)):
					# pop the message from the buffer, indicating we've processed it
					del self._buffer[i]

					# mark as consumed and increment the index _before_ we yield,
					# as we may never come back from yielding (if the user decides
					# to break from the loop)
					self._consumed[(topic, part)] = offs
					self._idx += 1

					# yield if the filter didn't return None
					if rec is not None:
						yield self._idx-1, rec

	def commit(self, defer=True):
		self._committed = self._consumed.copy()
		self._commit_to_kafka(defer=defer)

	def _commit_to_kafka(self, defer=True):
		# Occasionally commit read offsets (note: while this ensures we won't lose any
		# messages, some may be duplicated if the program is killed before offsets are committed).
		now = time.time()
		last = self.last_commit_time if self.last_commit_time is not None else 0
		if self._committed and (not defer or now - last > 5.):
			print("COMMITTING", file=sys.stderr)
			tp = [ TopicPartition(_topic, _part, _offs + 1) for (_topic, _part), _offs in self._committed.items() ]
			#print("   TP=", tp)
			self.c.commit(offsets=tp)

			# drop all _committd offsets from _consumed; no need to commit them again if the user
			# calls commit()
			self._consumed = {k:v for k,v in self._consumed.items() if k not in self._committed}
			self._committed = {}

			self.last_commit_time = now

	def _stream(self, filter, mapper, progress, timeout, limit):
		import warnings
		with warnings.catch_warnings():
			# hide the annoying 'TqdmExperimentalWarning' warning
			warnings.simplefilter("ignore")
			from tqdm.autonotebook import tqdm

		t = tqdm(disable=not progress, total=limit, desc='Alerts processed', unit=' alerts', mininterval=0.5, smoothing=0.5, miniters=0)

		nread = 0
		for idx, rec in self._filtered_stream(mapper=mapper, filter=filter, timeout=timeout):
			if is_heartbeat(rec):
				t.update(0)
				continue

			yield idx, rec

			t.update()
			nread += 1

			if nread == limit:
				break

		t.close()

	# returns a generator for the user-mapped messages using the filter function,
	# possibly executed on ncores and up to maxread values, + heartbeats
	def __call__(self, filter=None, pool=None, progress=False, timeout=None, limit=None):
		if pool:
			mapper = lambda fun, vec: pool.imap(fun, vec, chunksize=100)
		else:
			mapper = map

		yield from self._stream(filter, mapper=mapper, progress=progress, timeout=timeout, limit=limit)

	def __iter__(self):
		return self.__call__()

	# producer functionality
	def write(self, msg):
		packet = self._serialize(msg)
		self.p.produce(self.topics[0], packet)

	def flush(self):
		return self.p.flush()

@contextmanager
def Pool(*args, **kwarg):
	original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
	p = MPPool(*args, **kwarg)
	signal.signal(signal.SIGINT, original_sigint_handler)
	try:
		yield p
	finally:
		p.close()

if __name__ == "__main__":
	def my_filter(msg):
		#return msg
		return None if msg.candidate.ssnamenr == 'null' else msg

	try:
		from datetime import datetime
		with Pool(5) as workers:
			with AlertBroker("kafka://broker0.do.alerts.wtf/test6", start_at="earliest") as stream:
				for nread, (idx, rec) in enumerate(stream(filter=my_filter, pool=workers, progress=True, timeout=10), start=1):

					## do stuff
					cd = rec.candidate
					print(f"[{datetime.now()}] {nread}/{idx}:", cd.jd, cd.ssdistnr, cd.ssnamenr)

					stream.commit()
				stream.commit(defer=False)

		# with AlertBroker("kafka://broker0.do.alerts.wtf/test8", start_at="earliest") as stream:
		# 	for nread, (idx, rec) in enumerate(stream(progress=True, timeout=2), start=1):

		# 		## do stuff
		# 		cd = rec.candidate
		# 		print(f"[{datetime.now()}] {nread}/{idx}:", cd.jd, cd.ssdistnr, cd.ssnamenr)

		# 		stream.commit()

		# 		if nread == 10:
		# 			break

		# 	print("OUTSEDE")
		# 	print("GOING IN")

		# 	for nread, (idx, rec) in enumerate(stream(progress=True, timeout=10), start=1):
		# 		cd = rec.candidate
		# 		print(f"[{datetime.now()}] {nread}/{idx}:", cd.jd, cd.ssdistnr, cd.ssnamenr)
		# 		stream.commit()

			stream.commit(defer=False)
	except KeyboardInterrupt:
		pass
