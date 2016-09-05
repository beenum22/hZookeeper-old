#!/usr/bin/env python
import json
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
		self.options = options
		self.results = {}
		task_list = self.all_task_ids[self.zk_pub_app_id]
		print ("Communicating signals to zk_stress_client")
<<<<<<< Updated upstream
#		print task_list
=======

>>>>>>> Stashed changes
		for task_id in task_list:
			info = self.apps[self.zk_pub_app_id]['ip_port_map'][task_id]
			port = info[0]
			ip = info[1]
			ha_list = []
			self.zkpa = ZKPubAnalyser(ip, port, task_id)
			print "Sending sengmsg signal to %s : %s" %(ip,port)
			self.zkpa.do_req_resp('sendmsg', tout_60s)
			ha_list.append(self.zkpa)
		
		self.results['total_write']= []
		self.results['95_write']= []
		self.results['min_write']= []
		self.results['max_write']= []
		self.results['total_read']= []
		self .results['90_write']= []
		self .results['median_write']= []
		self .results['mean_write']= []
		self.results['write_rate']= []
		for task_id in task_list:
#			print task_id
			info = self.apps[self.zk_pub_app_id]['ip_port_map'][task_id]
			port = info[0]
			ip = info[1]
			self.zkpa = ZKPubAnalyser(ip, port, task_id)
			print "*****************"
			print "Sending teststatus signal to %s : %s" %(ip,port)			
			
			while True:
				(stat, r)=self.zkpa.do_req_resp('teststatus', tout_60s)
				if r=='stopping':
					break
				time.sleep(1)
			print "Done waiting"
			print "Getting stats" 
			(status, resp) = self.zkpa.do_req_resp('getstats', tout_60s)
#			print resp

			for threads_id in resp.keys():
				if threads_id != 'successfull_threads':

					list=[str(i) for i in resp[threads_id].strip('{}').split(',')]
					dict = {}
#					print list
					for d in list:
						dict[d.split(":")[0].strip(" '")] = float(d.split(":")[1])
#					print dict
					self.results['total_write'].append(dict['total_write_latency/ms'])
					self.results['total_read'].append(dict['total_read_latency/ms'])
					self.results['min_write'].append(dict['min_write_latency/ms'])
					self.results['max_write'].append(dict['max_write_latency/ms'])
					self.results['95_write'].append(dict['95_write_percentile'])
					self.results['write_rate'].append(dict['write_rate/ms'])
					self.results['median_write'].append(dict['median_write'])
					self.results['mean_write'].append(dict['mean_write'])
					self.results['90_write'].append(dict['90_write_percentile'])


			print "************"
#			print obj
		for x in self.results.keys():
			print "***"
			print "%s : %s" %(x, self.results[x])
#		sys.stdout = open('test.txt', 'w')

#			self.results['%s:%s'%(ip,port)] = resp
#			print "Successful threads on %s:%s is %s" %(ip,port,self.results['%s:%s'%(ip,port)]['successfull_threads'])
#			print "******************"
#			print self.results['%s:%s'%(ip,port)]
		print "******************"

	def launch_zk_pub(self):
		"""
		Function to launch zookeeper stress app.
		"""
		print ("Launching the Zookeeper stress app")
		max_threads_per_client = 9
		if self.options.client_count > max_threads_per_client:
#			threads_per_client = max_threads_per_client
			client_count = math.ceil(self.options.client_count / float(max_threads_per_client))
			print "Clients to launch : %s" %int(client_count)
			threads_per_client = int(math.ceil(self.options.client_count / client_count))
			print "Threads per client : %s" %threads_per_client
			
			
		else:
			threads_per_client = self.options.client_count
		self.create_binary_app(name=self.zk_pub_app_id, app_script='./src/zk_stress.py %s %s %s %s'
									  % (self.options.znode_creation_count,
									  	 self.options.znode_data,
									  	 self.options.znode_deletion_count,
									  	 threads_per_client),
	                               cpus=0.01, mem=50, ports=[0])
		if self.options.client_count > max_threads_per_client:
#			tt= self.options.client_count /float( max_threads_per_client )
#			print tt
#			client_count = math.ceil(self.options.client_count / float(max_threads_per_client))
#			l.info("Number of Zookeeper-Stress Clients to launch = %s" % (client_count))
			self.scale_and_verify_app(self.zk_pub_app_id, client_count)
#			print client_count, self.options.client_count, max_threads_per_client
			print "Done scaling !"
#		time.sleep(20)
class RunTest(object):
	def __init__(self, argv):
        	usage = ('python %prog --znode_creation_count=<Znodes count>'
                	 '--client_count=<Total clients to launch>'
                	 '--znode_data=<Desired data you want to store in a znode>'
                	 '--znode_deletion_count=<Number of znodes to delete to trigger watches>')

        	parser = OptionParser(description='zookeeper scale test master',
        	                      version="0.1", usage=usage)
		parser.add_option("--znode_creation_count", dest='znode_creation_count', type='int')
		parser.add_option("--client_count", dest='client_count', type='int')
		parser.add_option("--znode_data", dest='znode_data', type='str')
		parser.add_option("--znode_deletion_count", dest='znode_deletion_count', type='int')
		(options, args) = parser.parse_args()
		if ((len(args) != 0)):
			parser.print_help()
			sys.exit(1)



		print options
#		print time.time()
#		print time.clock()
#		num_msgs = int(argv[1])
#		client_count = int(argv[2])
#		zk_server_ip = argv[3] 

		r = ZK(options)

		r.start_appserver()

		r.run_test()


#	        print ("About to sleep for 15")
#       time.sleep(15)
		r.delete_all_launched_apps()
		r.stop_appserver()

if __name__ == "__main__":
	RunTest(sys.argv)
