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
		self.register_fn('startreader', self.start_reader)
		self.register_fn('stopreader', self.stop_reader)
		self.register_fn('getstats', self.get_stats)

	def start_reader(self):
		self.run_data['test_action']='startreader'
		return 'ok', None
	
	def stop_reader(self):
		self.run_data['test_action']='stopreader'
		return ('ok', None)

	def get_stats(self):
		self.run_data['test_action']='getstats'
#		stats = json.dumps(self.run_data['stats'])
		return ('ok', self.run_data['stats'])

	def reader(self, j):
		l.info("started thread-%s"%j)
		zkr = KazooClient(hosts=self.zk_server_ip)
		zkr.start()
		l.info("About to start reading")
		totalread_end = 0
		read_time = []
		while True:
			time.sleep(1)
			print "test"
			time.sleep(1)
			child = zkr.get_children("/Hydra")
			child_len = len(child)
			if child_len != 0:
				for r in child:
#					time.sleep(0.5)
					if self.run_data['test_status'] == 'start':
						l.info(r)
						read_time_start=time.time()*1000
						data, stat = zkr.get("/Hydra/%s"%r)

						read_time_end=(time.time()*1000)
						read_time_diff = read_time_end - read_time_start
						l.info(read_time_diff)
						totalread_end = totalread_end + read_time_diff
						l.info(totalread_end)
						dict_read={'read':{}, 'total':{}}
						dict_read['read'][read_time_end] = read_time_diff
						l.info(dict_read)
						read_time.append(dict_read['read'])
					else:
						break
					dict_read['total'][time.time()*1000] = totalread_end
					self.run_data['stats']['read_times'] = read_time
					self.run_data['stats']['total_reads'] = dict_read['total']
					l.info(self.run_data['stats'])
										

			else:
				l.info("No znode created yet")
			if self.run_data['test_status'] == 'stop':
				zkr.stop()
				l.info("Reader test ended")					
				break
		return 'ok', None

def run(argv):
	zk_server_ip=argv[1]
	reader_rep_port = os.environ.get('PORT0')
	run_data = {'stats': {},
		    'test_action': 'waiting',
		    'test_status': 'stopped'}
	l.info ("Starting ZKreader  at port [%s]", reader_rep_port)		    	
	hd = ZKPub(reader_rep_port, run_data, zk_server_ip)
	hd.run()
	j = 1
	while True:
		l.info("Reader code running")
		if run_data['test_action'] == 'startreader':
			run_data['test_status'] = 'start'
			l.info("Starting reader thread-%s"%j)
			r = Thread(target=hd.reader , args=(j,))
			r.start()
			
			run_data['test_action'] = 'waiting'
			j += 1
			
		elif run_data['test_action'] == 'stopreader':
			run_data['test_status'] = 'stop'
			time.sleep(1)
#			break
		elif run_data['test_action'] == 'getstats':
			time.sleep(2)
			break
		else:
			time.sleep(1)	

	
	
if __name__ == "__main__":
	run(sys.argv)	
	
	
	
	
	
	
	
