#!/usr/bin/env python
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
    def __init__(self,port,run_data,msg_cnt,threads,pub_rep_port):
	self.run_data = run_data
	self.msg_cnt = msg_cnt
	self.threads = int(threads)
	self.pub_rep_port = pub_rep_port
        HDaemonRepSrv.__init__(self,port)
        self.register_fn('sendmsg', self.test_start)
	self.register_fn('getstats', self.get_stats)
	self.register_fn('teststatus', self.test_status)

    def test_start(self):
	self.run_data['start']=True
	#add a loop here whose range is threads/client which comes from the arguments
	#find a way to populate the run.data{}
	
#	threads=[]
#	for j in range(self.threads):
#		l.info ("starting thread-%i"%(j+1))
#		t=Thread( target=self.send_msg, args=(j,))	
#		threads.append(t)
#		t.start()
#	for x in threads:
#		x.join()
#	self.run_data['stats']['threads']='Threads running'
#	l.info ("All threads Started")
	return 'ok', None

    def test_status(self):
	return ('ok', self.run_data['test_status'])

    def get_stats(self):
        return ('ok', self.run_data)

    def send_msg(self,j):
        """
        Function to handle the 'sendmsg' signal by test.
        It will start sending 'arg1' number of messages to subscribers.
        :param arg1: Number of messages to send to the subscriber.
        :return:
        """
        l.info("send_msg has been called with argument %s" % self.msg_cnt)
	self.run_data['test_status'] = 'running'
	data=dict()

	zk = KazooClient(hosts='10.10.0.73:2181')
	zk.start()

	t=[]
	zk.ensure_path("/Hydra")
	
	totalwrite1=time.clock()
        for i in range(int(self.msg_cnt)):
		
		t1=time.clock()
		zk.create("/Hydra/h-", "", ephemeral=True, sequence=True)
		t2=time.clock() - t1
		t.append(t2)
	totalwrite2=time.clock()-totalwrite1
	self.run_data['stats']['port-%s'%self.pub_rep_port]['write_time_%i'%(j+1)] = totalwrite2
#	self.run_data['stats']['port-%s'%self.pub_rep_port]['min_write_%i'%j] = min(t)
#	self.run_data['stats']['port-%s'%self.pub_rep_port]['max_write_%i'%j] = max(t)
#	children=zk.get_children("/Hydra/")
#	self.run_data['stats']['msg_cnt'] = len(children)
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
    msg_cnt=argv[1]
    threads=argv[2]
    l.info(threads)
    list_threads=[]
    pub_rep_port = os.environ.get('PORT0')

    run_data = {'start': False,
		'threads_info':{},
		'stats': {'port-%s'%pub_rep_port:{}},
                'test_status': 'stopped'}
    print ("Starting ZKstress  at port [%s]", pub_rep_port)
    hd = ZKPub(pub_rep_port, run_data, msg_cnt, threads, pub_rep_port)
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
		run_data['test_status']='stopping'
		run_data['start']=False
		l.info("Done with threads")
	else:
		l.info("Still start signal not received, wait more")
		pass
			

if __name__ == "__main__":
    run(sys.argv)
