import hazelcast
from hazelcast.config import Config

def main():
    config = Config()
    config.cluster_name = "lab2"
    client = hazelcast.HazelcastClient(config)

    distributed_map = client.get_map("distributed-map").blocking()

    for _ in range(10000):
        flag = False
        while not flag:
            old_value = distributed_map.get("key")
            new_value = old_value + 1
            if distributed_map.replace_if_same("key", old_value, new_value):
                flag = True

    client.shutdown()

if __name__ == "__main__":
    main()
