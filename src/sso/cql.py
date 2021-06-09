from cassandra.cluster import Cluster

def wait_for_cql_start(node_ip):
	# TODO - timeout
	print(f'    [{node_ip}] Waiting for CQL to start (node bootstrap finished). This could take a while.')
	while True:
		try:
			cluster = Cluster([node_ip], connect_timeout=30)
			session = cluster.connect()
		except:
			# print(f'    [{node_ip}] Could not connect to CQL, retrying.')
			pass
		else:
			print(f'    [{node_ip}] Successfully connected to CQL.')
			break