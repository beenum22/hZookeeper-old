#!/usr/bin/env python
import json
import zmq
import os
import logging
import sys
import time
from kazoo.client import KazooClient
from sys import path
from threading import Thread

path.append("hydra/src/main/python")

from hydra.lib import util
from hydra.lib.hdaemon import HDaemonRepSrv

l = util.createlogger('HWPub', logging.INFO)


class ZKPub(HDaemonRepSrv):
	def __init__(self,port,run_data,zk_server_ip):
		self.run_data = run_data
		self.zk_server_ip = zk_server_ip
		HDaemonRepSrv.__init__(self,port)
		self.register_fn('startstress', self.stress_start)
		self.register_fn('stopstress', self.stress_stop)
		self.register_fn('increasestress', self.stress_increase)

	def stress_start(self):
		self.run_data['test_action'] = 'startstress'
		return 'ok', self.run_data['test_action']

	def stress_stop(self):
		self.run_data['test_action'] = 'stopstress'
		return ('ok', 'Muneeb')

	def stress_increase(self):
		self.run_data['test_action'] = 'increasestress'
		return ('ok', None)        	

	def write(self,j):

		l.info("Thread-%s"%j)
		zk = KazooClient(hosts=self.zk_server_ip)	# Connection to the zookeeper server
		zk.start()
		znodes = []
		zk.ensure_path("/Hydra")	# Make sure the /Hydra path exists in zookeeper hierarchy 

		while True:
			if self.run_data['test_action'] == 'stopstress':
				l.info("Stopping thread-%s"%j)
				break
			else:	
				for i in range(1000):
					t = zk.create("/Hydra/h-", b'Muneeb', ephemeral=True, sequence=True)
					znodes.append(t)
			time.sleep(1)		

#		l.info(znodes)

		time.sleep(2)
		zk.stop()

		return 'ok', None
	


def run(argv):
	"""
	This function would be called when hw_test launches hw_pub app.
	:param argv: Function will take publisher_port as argument. A ZMQ publisher socket will be opened with this port.
	:return:
	"""
    # Use PORT0 (this is the port which Mesos assigns to the applicaiton), as control port. HAnalyzer will send all
    # signals to this port.   

#    l.info("KAZOOOOOOO")
	zk_server_ip=argv[1]

#    l.info(threads)
	list_threads=[]
 	pub_rep_port = os.environ.get('PORT0')
	run_data = {'test_action': 'waiting',
		    'test_status': 'stopped'}

	print ("Starting ZKstress  at port [%s]", pub_rep_port)
	hd = ZKPub(pub_rep_port, run_data, zk_server_ip)
	hd.run()
	j=1 #initialize thread count
	while True:
#		l.info("  running")
		if run_data['test_action'] == 'startstress':
			l.info("Starting stress")
			run_data['test_status'] = 'start'
			r = Thread(target=hd.write , args=(j,))
			r.start()
			run_data['test_action'] = 'waiting'
			j += 1
		elif run_data['test_action'] == 'increasestress':
			l.info("Increasing stress threads")
			ir = Thread(target=hd.write , args=(j,))
			ir.start()
			run_data['test_action'] = 'waiting'
			j += 1
				
		elif run_data['test_action'] == 'stopstress':
			l.info("Got stopstress signal")
			run_data['test_status'] = 'stop'
			break
		else:
			l.info("waiting for a signal to start stressing")
			time.sleep(2)	

#		print "Threads number is %s"% len(run_data['stats'].keys())
			
#			run_data['stats']['successfull_threads'] = str(len(run_data['stats'].keys()))
			
			
if __name__ == "__main__":
	run(sys.argv)
