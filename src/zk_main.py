#!/usr/bin/env python
from threading import Thread
from ast import literal_eval
import re
import json
from influxdb import InfluxDBClient
import datetime
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
		self.zk_reader_app_id = self.format_appname("/zk-reader")
		self.zk_stress_app_id = self.format_appname("/zk-stress")
		self.zk_reader_task_ip = None
		self.zk_stress_task_ip = None
		self.zk_reader_cmd_port = None
		self.zk_stress_cmd_port = None
		self.zksa = None  # Stress Analyzer
		self.zkra = None	# Reader Analyzer		
		self.add_appid(self.zk_stress_app_id)
		self.add_appid(self.zk_reader_app_id)
		self.test_info = {'test_signal':'no signal', 'stress_status': 'stopped', 'reader_status': 'stopped', 'threads_count': {}}		

	def run_test(self):
		"""
		Function which actually runs
		"""
		self.start_init()
		self.launch_zk_reader()
		self.launch_zk_stress()
		self.influxdb_reset()
		self.post_run(self.test_info['test_signal'], self.options)
#		self.influxdb()
	
	def input(self):
		if self.test_info['test_signal'] == 'no signal': 
			self.test_info['test_signal'] = raw_input('Enter the desired action: ')
			print "Processing input!...."
		else:
			pass				
			
			
	def post_run(self, test_signal, options):
		self.test_info['test_signal'] = test_signal
		self.options = options
		self.started_stress_task_list = []
		info_reader = self.apps[self.zk_reader_app_id]['ip_port_map'].values()
		reader_task_id = self.apps[self.zk_reader_app_id]['ip_port_map'].keys()
		reader_port = info_reader[0][0]
		reader_ip = info_reader[0][1]
#		input_thread = Thread( target=input)
#		input_thread.start()
		print ("Reader is running on %s : %s" %(reader_ip, reader_port))
		self.zkra = ZKPubAnalyser(reader_ip, reader_port, reader_task_id)
		
		while True:
			self.input()
			if self.test_info['test_signal'] == 'startreader':
				print "Sending startreader signal to %s : %s" %(reader_ip,reader_port)
				(status, resp) = self.zkra.do_req_resp('startreader', tout_60s)
				if status != 'ok':
					print "Something went wrong, status no okay"
				else:
					self.test_info['reader_status'] = 'starting'	
				
				self.test_info['test_signal'] = 'no signal'
#				time.sleep(1)
				
				
			elif self.test_info['test_signal'] == 'stopreader':
				print "Sending stopreader signal to %s : %s" %(reader_ip,reader_port)
				(status, resp) = self.zkra.do_req_resp('stopreader', tout_60s)
				if status != 'ok':
					print "Something went wrong, status no okay"
				else:
					self.test_info['reader_status'] = 'stopping'	
				
				self.test_info['test_signal'] = 'no signal'
				
				
#			elif self.test_info['test_signal'] == 'startstress':
				#stress_task_list = self.all_task_ids[self.zk_stress_app_id]
#				info_stress = self.apps[self.zk_stress_app_id]['ip_port_map'].values()
#				stress_port = info_stress[0][0]
#				stress_ip = info_stress[0][1]
#				stress_task_id = self.apps[self.zk_stress_app_id]['ip_port_map'].keys()[0]
#				print stress_task_id
#				print type(stress_task_id)
#				print ("Initial stress Client running on %s : %s"%(stress_ip, stress_port))
#				self.zksa = ZKPubAnalyser(stress_ip, stress_port, stress_task_id)
#				print "Sending startstress signal to %s : %s" %(stress_ip,stress_port)
#				(status, resp) = self.zksa.do_req_resp('startstress', tout_60s)			
#				if status != 'ok':
#					print "Something went wrong, status no okay"
#				else:
#					self.test_info['stress_status'] = 'starting'	
				
#				self.started_stress_task_list.append(stress_task_id)
#				self.test_info['threads_count'][stress_task_id] = 1
#				self.test_info['test_signal'] = 'no signal'
										
			elif self.test_info['test_signal'] == 'startstress':
				try:
					stress_task_list = self.all_task_ids[self.zk_stress_app_id]
					print stress_task_list
					print self.test_info['threads_count']
					count = input("Enter number of tasks: ")
					print "Scaling app: %s to count: %s" %(self.zk_stress_app_id, count)
					self.scale_and_verify_app(self.zk_stress_app_id, count)
					print ("Successfully scaled")
					stress_task_list = self.all_task_ids[self.zk_stress_app_id]
					for task_id in stress_task_list:
						self.test_info['threads_count'][task_id] = 0
						print self.test_info['threads_count']
					while True:
						min_stress_task = min(self.test_info['threads_count'], key=self.test_info['threads_count'].get)
						threads_count = self.test_info['threads_count'][min_stress_task]
						if  threads_count == 5:
							self.test_info['test_signal']='stopreader'
							break
						info_stress = self.apps[self.zk_stress_app_id]['ip_port_map'][min_stress_task]
						stress_port = info_stress[0]
						stress_ip = info_stress[1]
						self.zksa = ZKPubAnalyser(stress_ip, stress_port, min_stress_task)
						print "Sending increasestress signal to %s : %s" %(stress_ip,stress_port)
						(status, resp) = self.zksa.do_req_resp('startstress', tout_60s)			
						if status != 'ok':
							print "Something went wrong, status no okay"
						else:
							self.test_info['stress_status'] = 'increasing'
						self.test_info['test_signal'] = 'no signal'	
						self.test_info['threads_count'][min_stress_task] += 1
						time.sleep(2)
				except KeyboardInterrupt:
					pass								
												
			elif self.test_info['test_signal'] == 'getstats':
				
				self.zkra = ZKPubAnalyser(reader_ip, reader_port, reader_task_id)
				(status, resp) = self.zkra.do_req_resp('getstats', tout_60s)
				print resp
				self.results(resp)
				break
				
			elif self.test_info['test_signal'] == 'stopstress':
				stress_task_list = self.all_task_ids[self.zk_stress_app_id]
				for stress_task_id in stress_task_list:
					info_stress = self.apps[self.zk_stress_app_id]['ip_port_map'][stress_task_id]
					stress_ip = info_stress[1]
					stress_port = info_stress[0]
					print "Sending stopstress signal to %s : %s" %(stress_ip,stress_port)
					self.zksa = ZKPubAnalyser(stress_ip, stress_port, stress_task_id)
					(status, resp) = self.zksa.do_req_resp('stopstress', tout_60s)			
					if status != 'ok':
						print "Something went wrong, status no okay"
					else:
						self.test_info['stress_status'] = 'stopping'
							
				self.test_info['test_signal'] = 'no signal'						
				
			else:
				print "No signal received"
				time.sleep(1)
					
							
	def results(self, resp):
		for stats_key in resp.keys():
			print "************"
			dict={stats_key:{}}
			list=resp[stats_key].strip('[]').split(',')
			for d in list:
				
				dict[stats_key][float(d.strip().strip('{}').split(":")[0])] = float(d.strip().strip('{}').split(":")[1])
			print dict
			json_body = [{"measurement" : "hZookeeper_stats", "tags":{}, "time":'', "fields" : {}}]
			for k in dict[stats_key].keys():
				time_db = datetime.datetime.fromtimestamp(float(k)/1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')
				json_body[0]["time"] = time_db
				json_body[0]["fields"][stats_key] = float(dict[stats_key][k])
				print json_body
				self.influxdb(json_body)


	def influxdb(self, json_body):
		client = InfluxDBClient(host='10.10.0.73', port=8086, username='root', password='root', database='hZookeeper')
		dbs = client.get_list_database()
#		print dbs
		try:
			t=dbs[1]
		except:
#			print "NO DB"
			client.create_database('hZookeeper')
#		print type(json_body)
		client.write_points(json_body)
		print "done writing data"
#		client.drop_database('hZookeeper')		

	def influxdb_reset(self):
                client = InfluxDBClient(host='10.10.0.73', port=8086, username='root', password='root', database='hZookeeper')
                dbs = client.get_list_database()
#               print dbs
		client.drop_database('hZookeeper')

	def launch_zk_reader(self):
		print "Launching reader app"
		print self.options.zk_server_ip
		threads_per_client = 5
		self.create_binary_app(name=self.zk_reader_app_id, app_script='./src/zk_reader.py %s'
									% (self.options.zk_server_ip),	
					cpus=0.1, mem=128, ports=[0])
						
	
		
	def launch_zk_stress(self):
		"""
		Function to launch zookeeper stress app.
		"""
		print ("Launching the Zookeeper stress app")
		max_threads_per_client = 5

		self.create_binary_app(name=self.zk_stress_app_id, app_script='./src/zk_stress_write.py  %s'
										 % self.options.zk_server_ip,
	                               cpus=0.1, mem=128, ports=[0])
		print "Stress app started succesffully"

#		time.sleep(20)
class RunTest(object):
	def __init__(self, argv):
        	usage = ('python %prog --zk_server_ip=<ZK ip>')

        	parser = OptionParser(description='zookeeper scale test master',
        	                      version="0.1", usage=usage)
#		parser.add_option("--znode_creation_count", dest='znode_creation_count', default=1000, type='int')
#		parser.add_option("--client_count", dest='client_count', default=5, type='int')
#		parser.add_option("--znode_data", dest='znode_data', default='Muneeb', type='str')
#		parser.add_option("--znode_modification_count", dest='znode_modification_count', default=10, type='int')
#		parser.add_option("--stress_reader", dest='stress_reader', default='no', type='str')
		parser.add_option("--zk_server_ip", dest='zk_server_ip', default='10.10.0.73:2181', type='str')
		(options, args) = parser.parse_args()
		if ((len(args) != 0)):
			parser.print_help()
			sys.exit(1)


		
		print options

		r = ZK(options)

		r.start_appserver()

		r.run_test()


#	        print ("About to sleep for 15")
#       time.sleep(15)
#		r.delete_all_launched_apps()
		r.stop_appserver()

if __name__ == "__main__":
	RunTest(sys.argv)
