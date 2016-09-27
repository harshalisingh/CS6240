#Author: Vishal Mehta, Harshali Singh

from subprocess import call
import sys
import boto3
import math
import paramiko

bucket_name = "cs6240-viha"
## Open the file with read only permit
f = open('dns.txt', "r")

## use readlines to read all lines in the file
line = f.readline()
dnslist = list()

while line:
	if line.strip() != '':
		dnslist.append(line.strip())	
	line = f.readline()
f.close()

numProc = len(dnslist)
s3 = boto3.resource('s3')
files = list()

#retrieve all files from S3 bucket and add those to the list
for bucket in s3.buckets.all():
	for obj in bucket.objects.filter(Prefix='input/'):
		filename = '{0}'.format(obj.key).split("/")[1]
		if filename != "":
			files.append(filename)


#Divide the entire list of files, seq into chunks of equal/unequal sizes based on number of instances
def chunkIt(seq, num):
  avg = len(seq) / float(num)
  last = 0.0
  out = []
  while last < len(seq):
    out.append(seq[int(last):int(last + avg)])
    last += avg

  return out

ssh = paramiko.SSHClient()
paramiko.util.log_to_file("logfile.log")

#connect to every instance through ssh and transfer the chunks of file to each instance.
def putec2(part, dns):
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	ssh.connect(dns,username='ec2-user',key_filename='EC2_KP.pem')
	transport = ssh.get_transport()
	transport.set_keepalive(30)
	
	partlength = len(part)
	print ("Transferring chunks of data to " + dns)
	for i in range(partlength):
		chan = transport.open_session()
		cmd1="mkdir input"
		cmd2="mkdir output"	
		cmd3="aws s3 cp s3://" + bucket_name + "/input/" + part[i] + " ~/input"	
		chan.exec_command(cmd1 + "\n" + cmd2 + "\n" + cmd3)
		exit_code = chan.recv_exit_status()

chunks = chunkIt(files, numProc)

i=0
for dns in dnslist:	
	putec2(chunks[i], dns)
	i=i+1
ssh.close()
