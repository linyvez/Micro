import hazelcast
from hazelcast.config import Config
import time

config = Config()
config.cluster_name = "lab2"
client = hazelcast.HazelcastClient(config)

queue = client.get_queue("bounded-queue").blocking()

for i in range(1, 101):
    queue.put(i)
    print(f"Added {i}")
    time.sleep(0.1)

queue.put(-1)
queue.put(-1)

client.shutdown()
