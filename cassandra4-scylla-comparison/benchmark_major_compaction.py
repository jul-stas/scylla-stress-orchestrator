#!/bin/python3

import sys
import os

sys.path.insert(1, f"{os.environ['SSO']}/src/")

from sso import common
from sso.cs import CassandraStress
from sso.common import Iteration
from sso.scylla import Scylla
from sso.hdr import parse_profile_summary_file
from sso.cassandra import Cassandra
from datetime import datetime

print("Test started at:", datetime.now().strftime("%H:%M:%S"))

if len(sys.argv) < 2:
    raise Exception("Usage: ./benchmark_latency_throughput.py [PROFILE_NAME]")

# Load properties
profile_name = sys.argv[1]
props = common.load_yaml(f'{profile_name}.yml')
env = common.load_yaml(f'environment_{profile_name}.yml')
cluster_private_ips = env['cluster_private_ips']
cluster_string = ",".join(cluster_private_ips)
cluster_public_ips = env['cluster_public_ips']
loadgenerator_public_ips = env['loadgenerator_public_ips']
loadgenerator_count = len(loadgenerator_public_ips)

# Run parameters

# Row size of default cassandra-stress workload.
# Measured experimentally.
ROW_SIZE_BYTES = 210 * 1024 * 1024 * 1024 / 720_000_000

# 200GB per node
TARGET_DATASET_SIZE = len(cluster_private_ips) * 200 * 1024 * 1024 * 1024

REPLICATION_FACTOR = 3
ROW_COUNT = int(TARGET_DATASET_SIZE / ROW_SIZE_BYTES / REPLICATION_FACTOR)

# FIXME - background load changed to very low
BACKGROUND_LOAD_OPS = 1000

# Start Scylla/Cassandra nodes
if props['cluster_type'] == 'scylla':
    cluster = Scylla(env['cluster_public_ips'], env['cluster_private_ips'], env['cluster_private_ips'][0], props)
    cluster.install()
    cluster.start()
else:
    cluster = Cassandra(env['cluster_public_ips'], env['cluster_private_ips'], env['cluster_private_ips'][0], props)
    cluster.install()
    cluster.start()

print("Nodes started at:", datetime.now().strftime("%H:%M:%S"))

cs = CassandraStress(env['loadgenerator_public_ips'], props)
cs.install()
cs.prepare()

print("Loading started at:", datetime.now().strftime("%H:%M:%S"))

cs.stress_seq_range(ROW_COUNT, 'write cl=QUORUM', f'-schema "replication(strategy=SimpleStrategy,replication_factor={REPLICATION_FACTOR})" -log hdrfile=profile.hdr -graph file=report.html title=benchmark revision=benchmark-0 -mode native cql3 -rate "threads=30" -node {cluster_string}')

print("Run started at:", datetime.now().strftime("%H:%M:%S"))

# Background load
background_load = cs.loop_stress(f'mixed ratio\\(write=1,read=1\\) duration=5m cl=QUORUM -pop dist=UNIFORM\\(1..{ROW_COUNT}\\) -log hdrfile=profile.hdr -graph file=report.html title=benchmark revision=benchmark-0 -mode native cql3 -rate "threads=100 fixed={BACKGROUND_LOAD_OPS // loadgenerator_count}/s" -node {cluster_string}')

iteration = Iteration(f'{profile_name}/compact', ignore_git=True)

compact_start = datetime.now()

for i in range(len(cluster_public_ips)):
    print("Compacting node", i, "started at:", datetime.now().strftime("%H:%M:%S"))
    cluster.nodetool("compact", i)
    print("Compacting node", i, "ended at:", datetime.now().strftime("%H:%M:%S"))

compact_end = datetime.now()

with open(f'{iteration.dir}/result.txt', 'a') as writer:
    writer.write(f'Major compaction on all nodes took (s): {(compact_end - compact_start).total_seconds()}\n')

print("Run ended at:", datetime.now().strftime("%H:%M:%S"))

background_load.request_stop()
background_load.join()
print("Background load ended:", datetime.now().strftime("%H:%M:%S"))
