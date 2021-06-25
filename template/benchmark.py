#!/bin/python3

import sys
import os

sys.path.insert(1, f"{os.environ['SSO']}/src/")

from time import sleep
from sso import common
from sso import terraform
from sso.cs import CassandraStress
from sso.common import Iteration
from sso import prometheus
from sso.scylla import Scylla
from sso.cassandra import Cassandra

if len(sys.argv) < 2:
	raise Exception("Usage: ./benchmark.py [PROFILE_NAME]")

profile_name = sys.argv[1]

# Load the properties
props = common.load_yaml(f'{profile_name}.yml')

# Load information about the created machines.
env = common.load_yaml(f'environment_{profile_name}.yml')
cluster_private_ips = env['cluster_private_ips']
cluster_string = ",".join(cluster_private_ips)

# Start Scylla/Cassandra nodes
if props['cluster_type'] == 'scylla':
	scylla = Scylla(env['cluster_public_ips'], env['cluster_private_ips'], env['cluster_private_ips'][0], props)
	scylla.install()
	scylla.start()
else:
	cassandra = Cassandra(env['cluster_public_ips'], env['cluster_private_ips'], env['cluster_private_ips'][0], props)
	cassandra.install()
	cassandra.start()

iteration = Iteration("dummy-benchmark", ignore_git=True)

# Setup cassandra stress
cs = CassandraStress(env['loadgenerator_public_ips'], props)
cs.install()
cs.prepare()

# Total number of items.
items = 10_000_000

cs.upload("stress_example.yaml")

# Insert the test data.
cs.insert("stress_example.yaml", items, cluster_string)
 
# Actual work
cs.stress(f'user profile=./stress_example.yaml "ops(insert=1)" duration=2m -pop seq=1..{items} -log hdrfile=profile.hdr -graph file=report.html title=benchmark revision=benchmark-0 -mode native cql3 -rate threads=200 -node {cluster_string}')  


# collect the results.
cs.collect_results(iteration.dir)

prometheus.download_and_clear(env, props, iteration)

# Automatically terminates the cluster.
#terraform.destroy(props['terraform_plan'])
print("Call 'unprovision-terraform' to destroy the created infrastructure!")
