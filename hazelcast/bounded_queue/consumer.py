import hazelcast
from hazelcast.config import Config

config = Config()
config.cluster_name = "lab2"
client = hazelcast.HazelcastClient(config)

queue = client.get_queue("bounded-queue").blocking()

while True:
    item = queue.take()

    if item == -1:
        print("Got poison pill")
        break

    print(f"Consuming {item}")

client.shutdown()
