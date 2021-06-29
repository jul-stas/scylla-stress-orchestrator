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

# 100GB per node
TARGET_DATASET_SIZE = len(cluster_private_ips) * 100 * 1024 * 1024 * 1024

REPLICATION_FACTOR = 3
ROW_COUNT = int(TARGET_DATASET_SIZE / ROW_SIZE_BYTES / REPLICATION_FACTOR)

START_RATE     = 10000
RATE_INCREMENT = 10000

MAX_99_PERCENTILE_LATENCY = 10.0

# Start Scylla/Cassandra nodes
if props['cluster_type'] == 'scylla':
    s = Scylla(env['cluster_public_ips'], env['cluster_private_ips'], env['cluster_private_ips'][0], props)
    s.install()
    s.start()
else:
    cassandra = Cassandra(env['cluster_public_ips'], env['cluster_private_ips'], env['cluster_private_ips'][0], props)
    cassandra.install()
    cassandra.start()

print("Nodes started at:", datetime.now().strftime("%H:%M:%S"))

cs = CassandraStress(env['loadgenerator_public_ips'], props)
cs.install()
cs.prepare()

print("Loading started at:", datetime.now().strftime("%H:%M:%S"))

cs.stress_seq_range(ROW_COUNT, 'write cl=QUORUM', f'-schema "replication(strategy=SimpleStrategy,replication_factor={REPLICATION_FACTOR})" -log hdrfile=profile.hdr -graph file=report.html title=benchmark revision=benchmark-0 -mode native cql3 -rate "threads=30" -node {cluster_string}')

print("Run started at:", datetime.now().strftime("%H:%M:%S"))

rate = START_RATE

while True:
    print("Run iteration started at:", datetime.now().strftime("%H:%M:%S"))

    iteration = Iteration(f'{env["deployment_id"]}/cassandra-stress-{rate}', ignore_git=True)

    cs.stress(f'mixed ratio\\(write=1,read=1\\) duration=15m cl=QUORUM -pop dist=UNIFORM\\(1..{ROW_COUNT}\\) -log hdrfile=profile.hdr -graph file=report.html title=benchmark revision=benchmark-0 -mode native cql3 -rate "threads=100 fixed={rate // loadgenerator_count}/s" -node {cluster_string}')

    cs.collect_results(iteration.dir)

    write_profile_summary = parse_profile_summary_file(f'{iteration.dir}/profile-summary.txt', 'WRITE')
    print('WRITE_PROFILE', write_profile_summary)

    read_profile_summary = parse_profile_summary_file(f'{iteration.dir}/profile-summary.txt', 'READ')
    print('READ_PROFILE', read_profile_summary)

    if write_profile_summary.p99_latency_ms > MAX_99_PERCENTILE_LATENCY:
        break
    if read_profile_summary.p99_latency_ms > MAX_99_PERCENTILE_LATENCY:
        break

    rate += RATE_INCREMENT

print("Run ended at:", datetime.now().strftime("%H:%M:%S"))