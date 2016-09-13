#!/usr/bin/env python
import numpy
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
	def __init__(self,port,run_data, znodes_cr, znodes_data, znodes_mod, threads):
		self.run_data = run_data
		self.znodes_cr = znodes_cr
		self.znodes_data = znodes_data
		self.znodes_mod = znodes_mod
		self.threads = int(threads)
		HDaemonRepSrv.__init__(self,port)
		self.register_fn('sendmsg', self.test_start)
		self.register_fn('getstats', self.get_stats)
		self.register_fn('teststatus', self.test_status)
		self.run_data['stats']['watches'] = []
	def test_start(self):
		self.run_data['start']=True
		return 'ok', None

	def test_status(self):
		return ('ok', self.run_data['test_status'])

	def get_stats(self):
        	return ('ok', self.run_data['stats'])
        	
	def reader(self):
		zkr = KazooClient(hosts='10.10.0.73:2181')
		zkr.start()
		l.info("Side stress reader running")
		while True: 
			time.sleep(1)
			child = zkr.get_children("/Hydra")
			child_len = len(child)
			if child_len != 0:
				for r in child:
					data, stat = zk.get(r)
			else:
				l.info("No znode created yet")
		zkr.stop()					

		        	

	def trigger(self, event):
#	l.info(event[2], type(event[2])
		watch_rec = time.time()*1000
#		l.info(event)
		if event[0] == 'CHANGED':		
			l.info("Watched Triggered due to data change in %s"%event[2])
			zkt = KazooClient(hosts='10.10.0.73:2181')
			zkt.start()
			data, stat =  zkt.get(event[2])
			l.info(data)
			l.info(stat)
			l.info(stat[3])
			l.info(int(watch_rec))
			
			watch_time = watch_rec - float(stat[3])
			l.info(watch_time)
			zkt.stop()	
			l.info(self.run_data['watches'])
			self.run_data['watches']+=1
			l.info(self.run_data['watches'])
#			self.run_data['stats']['watch_latency-%s'%(self.run_data['watches'])] = watch_time
			self.run_data['stats']['watches'].append(watch_time)
		else:
			l.info("Watch triggered just beacause of node deletion")		

	def send_msg(self,j):
		"""
		Function to handle the 'sendmsg' signal by test.
		It will start sending 'arg1' number of messages to subscribers.
		:param arg1: Number of messages to send to the subscriber.
		:return:
		"""
	
		l.info("send_msg has been called with argument %s" % self.znodes_cr)
		self.run_data['test_status'] = 'running'
		data=dict()

		conn_time_start = time.time()*1000
		zk = KazooClient(hosts='10.10.0.73:2181')	# Connection to the zookeeper server
		zk.start()
		conn_time_end = (time.time()*1000)-conn_time_start

		req_time=[]		# initialize request times list
		read_time = []
		modify_time = []
		znodes = []
		mod_znodes = []
		totalread_end = 0
		totalwrite_end= 0
		totalmodify_end = 0
		zk.ensure_path("/Hydra")	# Make sure the /Hydra path exists in zookeeper hierarchy 

	# create znodes	and store data inside, calculate the times
#	totalwrite_start=time.time()*1000	
		for i in range(int(self.znodes_cr)):
			req_time_start=time.time()*1000
			t = zk.create("/Hydra/h-", self.znodes_data.encode('utf8'), ephemeral=True, sequence=True)
			req_time_end=(time.time()*1000) - req_time_start
			req_time.append(req_time_end)
			znodes.append(t)
	
			totalwrite_end = totalwrite_end + req_time_end
		l.info(znodes)

	#znodes reading
		for x in znodes:
			read_time_start=time.time()*1000
			data, stat = zk.get(x, watch=self.trigger)
#		l.info("data= %s : stat= %s"%(data,stat))
			read_time_end=(time.time()*1000) - read_time_start
			totalread_end = totalread_end + read_time_end
			read_time.append(read_time_end)
#		l.info(znodes[9])	
#		l.info(type(znodes[9]))
#		zk.set(znodes[9], b"I have changed!")
#		l.info("data changed")		

	#modify requested znodes
		for y in range(int(self.znodes_mod)):
			modify_time_start=time.time()*1000
			zk.set(znodes[y], b"I have changed!")
			modify_time_end=(time.time()*1000) - modify_time_start
			l.info("successfully modified %s" % znodes[y])
			modify_time.append(modify_time_end)
			totalmodify_end = totalmodify_end + modify_time_end
			mod_znodes.append(znodes[y])
		l.info("modified znodes : %s"%mod_znodes)
#	time.sleep(20)

		self.run_data['stats']['thread-%s'%(j+1)] = {}

		self.run_data['stats']['thread-%s'%(j+1)]['Connection_time'] = conn_time_end

		self.run_data['stats']['thread-%s'%(j+1)]['requested_znodes'] = int(self.znodes_cr)

		self.run_data['stats']['thread-%s'%(j+1)]['95_write_percentile'] = numpy.percentile(req_time, 95)
		self.run_data['stats']['thread-%s'%(j+1)]['90_write_percentile'] = numpy.percentile(req_time, 90)
		self.run_data['stats']['thread-%s'%(j+1)]['95_read_percentile'] = numpy.percentile(read_time, 95)
		self.run_data['stats']['thread-%s'%(j+1)]['90_read_percentile'] = numpy.percentile(read_time, 90)
		self.run_data['stats']['thread-%s'%(j+1)]['95_modify_percentile'] = numpy.percentile(modify_time, 95)
		self.run_data['stats']['thread-%s'%(j+1)]['90_modify_percentile'] = numpy.percentile(modify_time, 90)

		self.run_data['stats']['thread-%s'%(j+1)]['median_write'] = numpy.median(req_time)
		self.run_data['stats']['thread-%s'%(j+1)]['median_read'] = numpy.median(read_time)
		self.run_data['stats']['thread-%s'%(j+1)]['median_modify'] = numpy.median(modify_time)

		self.run_data['stats']['thread-%s'%(j+1)]['mean_write'] = numpy.mean(req_time)
		self.run_data['stats']['thread-%s'%(j+1)]['mean_read'] = numpy.mean(read_time)
		self.run_data['stats']['thread-%s'%(j+1)]['mean_modify'] = numpy.mean(modify_time)

#	l.info (self.run_data['stats']['thread-%s'%(j+1)]['95_write_percentile'])
#	l.info (req_time)
	
		self.run_data['stats']['thread-%s'%(j+1)]['total_read_latency/ms'] = totalread_end
		self.run_data['stats']['thread-%s'%(j+1)]['total_write_latency/ms'] = totalwrite_end
		self.run_data['stats']['thread-%s'%(j+1)]['total_modify_latency/ms'] = totalmodify_end
		self.run_data['stats']['thread-%s'%(j+1)]['min_write_latency/ms'] = min(req_time)
		self.run_data['stats']['thread-%s'%(j+1)]['max_write_latency/ms'] = max(req_time)
		self.run_data['stats']['thread-%s'%(j+1)]['min_read_latency/ms'] = min(read_time)
		self.run_data['stats']['thread-%s'%(j+1)]['max_read_latency/ms'] = max(read_time)
		self.run_data['stats']['thread-%s'%(j+1)]['min_modify_latency/ms'] = min(modify_time)
		self.run_data['stats']['thread-%s'%(j+1)]['max_modify_latency/ms'] = max(modify_time)

		self.run_data['stats']['thread-%s'%(j+1)]['write_rate/ms'] = int(self.znodes_cr)/totalwrite_end
		self.run_data['stats']['thread-%s'%(j+1)]['read_rate/ms'] = int(self.znodes_cr)/totalread_end
		time.sleep(5)
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
	znodes_cr=argv[1]
	znodes_data=argv[2]
	znodes_mod=argv[3]
	stress_reader=argv[4]
	threads=argv[5]
    
#    l.info(threads)
	list_threads=[]
 	pub_rep_port = os.environ.get('PORT0')

	run_data = {'start': False,
		    'stats': {},
		    'watches': 0,
		    'test_status': 'stopped'}
	print ("Starting ZKstress  at port [%s]", pub_rep_port)
	hd = ZKPub(pub_rep_port, run_data, znodes_cr, znodes_data, znodes_mod, threads)
	hd.run()

	while True:
		time.sleep(3)
		if  hd.run_data['start']==True:
			l.info ("Start signal received, Let's rock n roll")
			if stress_reader == 'yes':
				l.info("Starting stress reader thread")
				r=Thread(target=hd.reader)
				r.start()
			else:
				l.info("No stress reader thread activated")

			for j in range(int(threads)):
				l.info ("starting thread-%i"%(j+1))
				t=Thread( target=hd.send_msg, args=(j,))
				list_threads.append(t)
				t.start()
			for x in list_threads:
				x.join()
#		print "Threads number is %s"% len(run_data['stats'].keys())
			
			run_data['stats']['successfull_threads'] = str(len(run_data['stats'].keys()))
			l.info(type(znodes_mod))
			while run_data['watches']<int(znodes_mod):
				l.info(run_data['watches'])
				time.sleep(1)
				l.info("Not done with watches yet")
			l.info("done with watches check")				
			run_data['test_status']='stopping'
			run_data['start']=False
			l.info("Done with threads")
		elif run_data['test_status']=='stopped':
			l.info("Still start signal not received, wait more")
		else:
			break
			
			
if __name__ == "__main__":
	run(sys.argv)
