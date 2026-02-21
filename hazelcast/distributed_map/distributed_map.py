import hazelcast
from hazelcast.config import Config

config = Config()
config.cluster_name = "lab2"
client = hazelcast.HazelcastClient(config)

distributed_map = client.get_map("distributed-map").blocking()

for i in range(1000):
    distributed_map.set(i, i)

client.shutdown()
