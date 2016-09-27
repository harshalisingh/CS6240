#Author: Vishal Mehta, Harshali Singh

from subprocess import call
import sys
import time
import json

with open('Clusterid.txt') as fp:
	for i, line in enumerate(fp):
		if i == 1:
			words = line.split(':')
			clusterId = words[1]
			clusterId = clusterId[2:-2]
			# loop
			while 1:
				#f.close()
				f = open("Stats.json", "w+")
				time.sleep(20)
				call(["aws", "emr", "describe-cluster", "--cluster-id", clusterId],  stdout=f)
				f.close()
				with open('Stats.json', 'r') as myfile:
				    string=myfile.read().replace('\n', '')
				data = json.loads(string)
				status = data["Cluster"]["Status"]["State"]
				if status == "TERMINATED":
					#print ("OK")
					sys.exit()
				time.sleep(20)
				print ("Waiting for job completion...")
				f.close()
