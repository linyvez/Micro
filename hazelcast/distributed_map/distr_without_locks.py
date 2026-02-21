import hazelcast
from hazelcast.config import Config

def main():
    config = Config()
    config.cluster_name = "lab2"
    client = hazelcast.HazelcastClient(config)

    distributed_map = client.get_map("distributed-map").blocking()

    for _ in range(10000):
        value = distributed_map.get("key")
        value += 1
        distributed_map.put("key", value)

    client.shutdown()

if __name__ == "__main__":
    main()
