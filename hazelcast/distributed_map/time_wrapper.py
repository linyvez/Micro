import hazelcast
from hazelcast.config import Config
import time
import subprocess

config = Config()
config.cluster_name = "lab2"
client = hazelcast.HazelcastClient(config)

distributed_map = client.get_map("distributed-map").blocking()
distributed_map.put("key", 0)
client.shutdown()

start = time.perf_counter()

processes = []
for _ in range(3):
    process = subprocess.Popen(["python", "distr_opt_locks.py"]) # <- should change file name for different tests
    processes.append(process)

for process in processes:
    process.wait()

end = time.perf_counter()
total_time = round(end - start, 2)

client = hazelcast.HazelcastClient(config)
value = client.get_map("distributed-map").blocking().get("key")

print(f"Time taken: {total_time}s\nFinal value = {value}")

client.shutdown()
