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
    def __init__(self,port,run_data, znodes_cr, znodes_data, znodes_del, threads):
	self.run_data = run_data
	self.znodes_cr = znodes_cr
	self.znodes_data = znodes_data
	self.znodes_del = znodes_del
	self.threads = int(threads)
        HDaemonRepSrv.__init__(self,port)
        self.register_fn('sendmsg', self.test_start)
	self.register_fn('getstats', self.get_stats)
	self.register_fn('teststatus', self.test_status)

    def test_start(self):
	self.run_data['start']=True
	return 'ok', None

    def test_status(self):
	return ('ok', self.run_data['test_status'])

    def get_stats(self):
        return ('ok', self.run_data['stats'])

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
	zk.ensure_path("/Hydra")	# Make sure the /Hydra path exists in zookeeper hierarchy 
	
	totalwrite_start=time.time()*1000	
        for i in range(int(self.znodes_cr)):
		req_time_start=time.time()*1000
		zk.create("/Hydra/h-", "", ephemeral=True, sequence=True)
		req_time_end=(time.time()*1000) - req_time_start
		req_time.append(req_time_end)
	totalwrite_end=(time.time()*1000)-totalwrite_start
	self.run_data['stats']['thread-%s'%(j+1)] = {}
	
	self.run_data['stats']['thread-%s'%(j+1)]['Connection_time'] = conn_time_end
	self.run_data['stats']['thread-%s'%(j+1)]['95_write_percentile'] = numpy.percentile(req_time, 95)
	self.run_data['stats']['thread-%s'%(j+1)]['90_write_percentile'] = numpy.percentile(req_time, 90)
	self.run_data['stats']['thread-%s'%(j+1)]['median_write'] = numpy.median(req_time)
	self.run_data['stats']['thread-%s'%(j+1)]['mean_write'] = numpy.mean(req_time)

#	l.info (self.run_data['stats']['thread-%s'%(j+1)]['95_write_percentile'])
#	l.info (req_time)
	self.run_data['stats']['thread-%s'%(j+1)]['requested_znodes'] = int(self.znodes_cr)
	self.run_data['stats']['thread-%s'%(j+1)]['total_write_latency/ms'] = totalwrite_end
	self.run_data['stats']['thread-%s'%(j+1)]['min_write_latency/ms'] = min(req_time)
	self.run_data['stats']['thread-%s'%(j+1)]['max_write_latency/ms'] = max(req_time)

	self.run_data['stats']['thread-%s'%(j+1)]['write_rate/ms'] = int(self.znodes_cr)/totalwrite_end
	totalread_start = time.time()*1000
	children=zk.get_children("/Hydra/")
	totalread_end = (time.time()*1000) - totalread_start
	self.run_data['stats']['thread-%s'%(j+1)]['total_read_latency/ms'] = totalread_end
#	self.run_data['stats']['znodes_cr'] = len(children)
	zk.stop()
	
#	self.run_data['threads_info']='done'
#	self.run_data['test_status']='stopping'
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
    znodes_del=argv[3]
    threads=argv[4]
    
    l.info(threads)
    list_threads=[]
    pub_rep_port = os.environ.get('PORT0')

    run_data = {'start': False,
		'stats': {},
#		'stats': {'thread_number':{},
#			  'ip-port':{},
#			  'total_write':{},
#			  'total_read':{},
#			  'requested_znodes':{},
#			  'max_write_latency':{},
#			  'min_write_latency':znodes_cr}},
                'test_status': 'stopped'}
    print ("Starting ZKstress  at port [%s]", pub_rep_port)
    hd = ZKPub(pub_rep_port, run_data, znodes_cr, znodes_data, znodes_del, threads)
    hd.run()

    while True:
        time.sleep(3)
	if  hd.run_data['start']==True:
		l.info ("Start signal received, Let's rock n roll")
		for j in range(int(threads)):
			l.info ("starting thread-%i"%(j+1))
        	        t=Thread( target=hd.send_msg, args=(j,))
			list_threads.append(t)
        	        t.start()
		for x in list_threads:
			x.join()
#		print "Threads number is %s"% len(run_data['stats'].keys())
		run_data['stats']['successfull_threads'] = str(len(run_data['stats'].keys()))
		run_data['test_status']='stopping'
		run_data['start']=False
		l.info("Done with threads")
	else:
		l.info("Still start signal not received, wait more")
		pass
			
if __name__ == "__main__":
    run(sys.argv)
