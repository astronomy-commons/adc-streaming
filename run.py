import genesis.streaming as gs

message = { 'hello': 'world' }

with gs.open("kafka://localhost/test1", "rw", start_at="earliest", format="json", config="kafkacat.conf") as stream:
	stream.write(message)

#with gs.open("kafka://localhost/test1", "r", start_at="earliest", format="json") as stream:
	for _, rec in stream(timeout=10):
		print(rec)
