import os
from time import sleep
from sso.ssh import PSSH, SSH
from sso.cql import wait_for_cql_start

import os
import time

from datetime import datetime
from sso.hdr import HdrLogProcessor
from sso.ssh import SSH
from sso.util import run_parallel, find_java, WorkerThread, log_important

# Assumes Scylla was started from official Scylla AMI.
class Scylla:

    def __init__(self, cluster_public_ips, cluster_private_ips, seed_private_ip, properties):
        self.properties = properties
        self.cluster_public_ips = cluster_public_ips
        self.cluster_private_ips = cluster_private_ips
        self.seed_private_ip = seed_private_ip
        self.ssh_user = properties['cluster_user']
  
    def __new_ssh(self, ip):
        return SSH(ip, self.ssh_user, self.properties['ssh_options'])

    def __install(self, ip):
        ssh = self.__new_ssh(ip)

        # Scylla AMI automatically performs setup
        # and then starts up. Each node is a separate 1-node cluster.
        # Here, we wait for this startup.
        wait_for_cql_start(ip)

        # Scylla started. Now we stop it and wipe
        # the data it generated.

        # FIXME - stop scylla-server more forcefully?
        ssh.exec("sudo systemctl stop scylla-server")
        ssh.exec("sudo rm -rf /var/lib/scylla/data/*")
        ssh.exec("sudo rm -rf /var/lib/scylla/commitlog/*")

        # Patch configuration files
        ssh.exec("sudo sed -i \"s/cluster_name:.*/cluster_name: cluster1/g\" /etc/scylla/scylla.yaml")
        ssh.exec(f'sudo sed -i \"s/seeds:.*/seeds: {self.seed_private_ip} /g\" /etc/scylla/scylla.yaml')

    def install(self):
        log_important("Installing Scylla: started")
        run_parallel(self.__install, [(ip,) for ip in self.cluster_public_ips])
        log_important("Installing Scylla: done")
        
    def start(self):
        print(f"Starting Scylla nodes {self.cluster_public_ips}")
        for public_ip in self.cluster_public_ips:
            ssh = self.__new_ssh(public_ip)
            ssh.exec("sudo systemctl start scylla-server")
            wait_for_cql_start(public_ip)
        print(f"Starting Scylla nodes {self.cluster_public_ips}: done")

    def stop(self):
        print("Not implemented!")
