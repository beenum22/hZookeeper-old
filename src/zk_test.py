#!/usr/bin/env python
import time
import sys
import math
from sys import path
path.append("hydra/src/main/python")
from hydra.lib.runtestbase import HydraBase
from ConfigParser import ConfigParser
from optparse import OptionParser
from hydra.lib.h_analyser import HAnalyser
tout_60s = 60000



class ZKPubAnalyser(HAnalyser):
	def __init__(self, server_ip, server_port, task_id):
		HAnalyser.__init__(self, server_ip, server_port, task_id)


class ZK(HydraBase):
	def __init__(self, options):
		self.config = ConfigParser()
		self.options = options
		HydraBase.__init__(self, test_name='ZKstress', options=self.options, app_dirs=['src', 'hydra'])

		self.zk_pub_app_id = self.format_appname("/zk-pub")

		self.zk_pub_task_ip = None

		self.zk_pub_cmd_port = None

		self.zkpa = None  # Pub Analyzer

		self.add_appid(self.zk_pub_app_id)

	def run_test(self):
		"""
		Function which actually runs
		"""
        
		self.start_init()

		self.launch_zk_pub()

		self.post_run(self.options)
		
		
	def post_run(self,options):
#		print self.zk_pub_app_id
#		print self.apps[self.zk_pub_app_id]
		self.options = options
		task_list = self.all_task_ids[self.zk_pub_app_id]
		print ("Communicating signals to zk_stress_client")

#		print task_list
		for task_id in task_list:
#			print task_list	
			info = self.apps[self.zk_pub_app_id]['ip_port_map'][task_id]
#			print self.apps
#			print info
			port = info[0]
			ip = info[1]
			ha_list = []
			self.zkpa = ZKPubAnalyser(ip, port, task_id)
			print "Sending sengmsg signal to %s : %s" %(ip,port)
			self.zkpa.do_req_resp('sendmsg', tout_60s)
#			print time.clock()," : ", task_id
			ha_list.append(self.zkpa)
#
#			print enumerate(ha_list)

		for task_id in task_list:
								
#	        for idx, self.zkpa in enumerate(ha_list):
#       	     		l.debug('Waiting for task [%s] in [%s:%s] test to END. Iteration: %s' % (self.zkpa.task_id, self.zkpa.server_ip, self.zkpa.port, idx))
#				print self.zkpa.task_id, self.zkpa.server_ip, self.zkpa.port, idx
#				self.zkpa.wait_for_testend()
			print "Waiting for tests to end"
			while True:
				(stat, r)=self.zkpa.do_req_resp('teststatus', tout_60s)
				if r=='stopping':
					break
				time.sleep(1)
			print "Getting stats" 
			(status, resp) = self.zkpa.do_req_resp('getstats', tout_60s)
#			print resp,"*****:",time.clock()," : ",task_id
#			"""
			print "*******************"
#			"""
			print resp['stats']
#			with open('test.txt', 'w') as f:
#				for key, value in resp.items():
#					f.write('%s:%s\n' % (key, value))					
#			"""
			print "*******************"
#			"""

	def launch_zk_pub(self):
		"""
		Function to launch zookeeper stress app.
		"""
		print ("Launching the Zookeeper stress app")
		max_threads_per_client = 9
		if self.options.client_count > max_threads_per_client:
#			threads_per_client = max_threads_per_client
			client_count = math.ceil(self.options.client_count / float(max_threads_per_client))
			print "Clients to launch : %s" %client_count
			threads_per_client = int(math.ceil(self.options.client_count / client_count))
			print "Threads per client : %s" %threads_per_client
			
			
		else:
			threads_per_client = self.options.client_count
		self.create_binary_app(name=self.zk_pub_app_id, app_script='./src/zk_stress.py %s %s'
									  % (self.options.msg_count , threads_per_client),
	                               cpus=0.01, mem=50, ports=[0])
		if self.options.client_count > max_threads_per_client:
#			tt= self.options.client_count /float( max_threads_per_client )
#			print tt
#			client_count = math.ceil(self.options.client_count / float(max_threads_per_client))
#			l.info("Number of Zookeeper-Stress Clients to launch = %s" % (client_count))
			self.scale_and_verify_app(self.zk_pub_app_id, client_count)
#			print client_count, self.options.client_count, max_threads_per_client
			print "Done scalling !"
#		time.sleep(20)
class RunTest(object):
	def __init__(self, argv):
        	usage = ('python %prog --msg_count=<Total Operations>'
                	 '--client_count=<Total clients to launch>')

        	parser = OptionParser(description='zookeeper scale test master',
        	                      version="0.1", usage=usage)
		parser.add_option("--msg_count", dest='msg_count', type='int')
		parser.add_option("--client_count", dest='client_count', type='int')
#		parser.add_option("--threads", dest='threads', type='int')
		(options, args) = parser.parse_args()
		if ((len(args) != 0)):
			parser.print_help()
			sys.exit(1)



		print options
#		num_msgs = int(argv[1])
#		client_count = int(argv[2])
#		zk_server_ip = argv[3] 

		r = ZK(options)

		r.start_appserver()

		r.run_test()


#	        print ("About to sleep for 15")
#       time.sleep(15)
#		r.delete_all_launched_apps()
		r.stop_appserver()

if __name__ == "__main__":
	RunTest(sys.argv)
