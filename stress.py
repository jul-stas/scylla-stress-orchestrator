#!/bin/python3

import common
from common import CassandraStress
from common import Iteration
#from common import HdrLogMerging


properties = common.load_yaml('properties.yml')
environment = common.load_yaml('environment.yml')
 
cluster_private_ips = environment['cluster_private_ips']
cluster_string = ",".join(cluster_private_ips)

iteration = Iteration("foobar")
    
cs = CassandraStress(environment['loadgenerator_public_ips'], properties)
cs.install()
cs.prepare();

#cs.upload("stress_example.yaml")
#cs.stress(f'user profile=./stress_example.yaml "ops(insert=1)" n=1m -mode native cql3 -rate threads=50 -node {cluster_string}')  
#cs.stress(f'user profile=./stress_example.yaml "ops(singleclothes=1, insert=1)" n=1m -log hdrfile=profile.hdr -graph file=profile.html title=store revision=benchmark-0 -mode native cql3 -rate threads=50 -node {cluster_string}')  


cs.stress(f'write n=2m -log hdrfile=store.hdr -graph file=store.html title=store revision=benchmark-0 -mode native cql3 -rate threads=50 -node {cluster_string}')   
#cs.stress(f'read  n=2m -log hdrfile=load.hdr -graph file=load.html   title=load revision=benchmark-0 -mode native cql3 -rate threads=50 -node {cluster_string}')
#cs.stress(f'mixed ratio\(write=1,read=3\) n=2m -log hdrfile=mixed.hdr -graph file=mixed.html title=mixed revision=benchmark-0 -mode native cql3 -rate threads=50 -node {cluster_string}')   

cs.get_results(iteration.dir)

#merge = HdrLogMerging(properties)
#merge.merge_logs("/eng/scylla/scylla-stress/trials/foobar/10-02-2021_11-35-34")

