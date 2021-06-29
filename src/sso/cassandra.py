 
import os
import time

from datetime import datetime
from sso.hdr import HdrLogProcessor
from sso.ssh import SSH
from sso.cql import wait_for_cql_start
from sso.util import run_parallel, find_java, WorkerThread, log_important
from sso.raid import RAID

class Cassandra:

    def __init__(self, cluster_public_ips, cluster_private_ips, seed_private_ip, properties, setup_raid=True, cassandra_version=None):
        self.properties = properties
        self.cluster_public_ips = cluster_public_ips
        self.cluster_private_ips = cluster_private_ips
        self.seed_private_ip = seed_private_ip
        if cassandra_version is not None:
            self.cassandra_version = cassandra_version
        else:
            self.cassandra_version = properties['cassandra_version']
            
        self.ssh_user = properties['cluster_user']
        self.setup_raid = setup_raid
        # trigger early detection of missing java.
        find_java(properties)
  
    def __new_ssh(self, ip):
        return SSH(ip, self.ssh_user, self.properties['ssh_options'])

    def __install(self, ip):
        ssh = self.__new_ssh(ip)
        ssh.update()
        print(f'    [{ip}] Installing Cassandra: started')
        ssh.install_one('openjdk-11-jdk', 'java-11-openjdk')
        ssh.install('wget')
        private_ip = self.__find_private_ip(ip)
        path_prefix = 'cassandra-raid/' if self.setup_raid else './'
        ssh.exec(f"""
            set -e
            
            if [ -d '{path_prefix}apache-cassandra-{self.cassandra_version}' ]; then
                echo Cassandra {self.cassandra_version} already installed.
                exit 0
            fi
            
            wget -q -N https://mirrors.netix.net/apache/cassandra/{self.cassandra_version}/apache-cassandra-{self.cassandra_version}-bin.tar.gz
            tar -xzf apache-cassandra-{self.cassandra_version}-bin.tar.gz -C {path_prefix}
            cd {path_prefix}apache-cassandra-{self.cassandra_version}
            sudo sed -i \"s/seeds:.*/seeds: {self.seed_private_ip} /g\" conf/cassandra.yaml
            sudo sed -i \"s/listen_address:.*/listen_address: {private_ip} /g\" conf/cassandra.yaml
            sudo sed -i \"s/rpc_address:.*/rpc_address: {private_ip} /g\" conf/cassandra.yaml
        """)
        print(f'    [{ip}] Installing Cassandra: done')

    def __find_private_ip(self, public_ip):
        index = self.cluster_public_ips.index(public_ip)
        return self.cluster_private_ips[index]

    def install(self):
        log_important("Installing Cassandra: started")
        if self.setup_raid:
            log_important("Installing Cassandra: setting up RAID")
            raid = RAID(self.cluster_public_ips, self.ssh_user, '/dev/nvme*n1', 'cassandra-raid', 0, self.properties)
            raid.install()
            log_important("Installing Cassandra: finished setting up RAID")
        run_parallel(self.__install, [(ip,) for ip in self.cluster_public_ips])
        log_important("Installing Cassandra: done")
        
    def __start(self, ip):
        print(f'    [{ip}] Starting Cassandra: started')
        ssh = self.__new_ssh(ip)
        path_prefix = 'cassandra-raid/' if self.setup_raid else './'
        ssh.exec(f"""
            set -e
            cd {path_prefix}apache-cassandra-{self.cassandra_version}
            if [ -f 'cassandra.pid' ]; then
                pid=$(cat cassandra.pid)
                kill $pid
                while kill -0 $pid; do 
                    sleep 1
                done
                rm -f 'cassandra.pid'
            fi
            bin/cassandra -p cassandra.pid 2>&1 >> cassandra.out & 
            """)
        print(f'    [{ip}] Starting Cassandra: done')

    def start(self):
        print(f"Starting Cassandra nodes {self.cluster_public_ips}")
        for public_ip in self.cluster_public_ips:
            self.__start(public_ip)
            wait_for_cql_start(public_ip)
        print(f"Starting Cassandra nodes {self.cluster_public_ips}: done")
        
    def __stop(self, ip):
        print(f'    [{ip}] Stopping Cassandra: started')
        ssh = self.__new_ssh(ip)
        path_prefix = 'cassandra-raid/' if self.setup_raid else './'
        ssh.exec(f"""
            set -e
            cd {path_prefix}apache-cassandra-{self.cassandra_version}
            if [ -f 'cassandra.pid' ]; then
                pid=$(cat cassandra.pid)
                kill $pid
                while kill -0 $pid; do 
                    sleep 1
                done
                rm -f 'cassandra.pid'
            fi
            """)
        print(f'    [{ip}] Stopping Cassandra: done')

    def stop(self):
        log_important("Stop Cassandra: started")
        run_parallel(self.__stop, [(ip,) for ip in self.cluster_public_ips])
        log_important("Stop Cassandra: done")    
