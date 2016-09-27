A8 - README
Team Members: Harshali Singh, Vishal Mehta, Akanksha Mishra, Saahil Singla

Question
********************************************************************
Distributed sort.
Fine print: (0) Groups of four. 
(1) You will build two java programs, a client and a sort node. Multiple copies of the sort node will run on Amazon EC2 instances, and the client will connect to them to issue sort tasks. 
(2) Write scripts to automate creation and destruction of your cluster as well as execution and timing of sort jobs. Use this system to sort the data at s3://cs6240sp16/climate numerically on the “Dry Bulb Temp” field. 
(3) Your report should include execution time for 2 instances and 8 instances, as well as a list of the top 10 values in the data set (show wban #, date, time, and temperature).
Simplified sample run:
$ ./start-cluster 4
Cluster with 4 nodes started.
$ ./sort “Dry Bulb Temp” s3://cs6240sp16/climate s3://your-bucket/output
$ aws s3 ls s3://your-bucket/output
output-0000
output-0001
output-0002
output-0003
$ ./stop-cluster
4 nodes stopped

(4) Suggested Design: (a) Sample Sort: Each node reads a similar sized chunk of the input data. Since there are more files than nodes in the data, it’s sufficient to partition the input files among the nodes - no need for nodes to read partial files. Randomly sample many values from the data, and broadcast to all nodes. Each node calculates the partitioning of data between nodes, and sends its initial input records to the appropriate nodes for sorting.  Each node then sorts its partition and outputs the results to a numbered output file. As in map-reduce, concatenating the output files alphabetically should give the final output “data file” in globally sorted order. (b) Cluster management: Write your node list to a local file when creating instances. Copy this file to each instance with the node jar.  (5) Requirements:  (a) EC2 automation: Script to create a cluster of EC2 linux nodes and install your server node software. Script to destroy your EC2 cluster. (b)  Distributed Java sorting system: A node program will run on each EC2 instance. A client program will accept commands and communicate with the nodes. You must use a build tool that automatically fetches dependencies. You must generate a stand-alone JAR for the node program that can be copied to EC2 instances and executed. (c) Test case: Should be able to input and sort s3://cs6240sp16/climate, a directory of gzipped CSV files. (d) Report: Comparison of 2 and 8 node execution time. Top 10 values in data set. Discussion of design decisions and challenges. Description of which team members did what. (6) Submit source code, PDF, but no data/binaries.

Instructions to set up and run the program.

*********************************************************************
PRE-REQUISITES:

1. Install python.
	sudo add-apt-repository ppa:fkrull/deadsnakes-python2.7
	sudo apt-get update 	
	sudo apt-get install python2.7

2. Install python packages: pip, boto3, paramiko
	sudo apt-get install pip
	pip install boto3
	pip install paramiko

3. Install jq. 
	sudo apt-get install jq.

4. A linux environment with working ssh in it.

5. Key-pair and Security group
	a. Connecting to ec2 instances through ssh requires a key pair which can be created by Amazon aws CLI given below. Make sure to run 'chmod' command for key pair.
		aws ec2 create-key-pair --key-name EC2_KP
		chmod 400 EC2_KP
	
	b. Create a security group which allows SSH to all the instances and allows all incoming traffic by adding rules to your security group. Use below Amazon aws CLI to create a security group:
		aws ec2 create-security-group --group-name my-sg --description "My security group"
	
6. Create an IAM role to allow access to S3 bucket from your ec2 instances using the AWS Console by follwoing the below steps.
	a. In the IAM console, in the navigation pane, choose Policies, and then choose Create Policy. (If a Get Started button appears, choose it, and then choose Create Policy.)
	b. Next to Create Your Own Policy, choose Select.
	c. In the Policy Name box, type 'S3-Permissions'.
	d. In the Policy Document box, paste the following after placing your <bucket_name>:
	{
	    "Version": "2012-10-17",
	    "Statement": [
		{
		    "Effect": "Allow",
		    "Action": [
		        "s3:ListBucket"
		    ],
		    "Resource": [
		        "arn:aws:s3:::<bucket_name>"
		    ]
		},
		{
		    "Effect": "Allow",
		    "Action": [
		        "s3:*"
		    ],
		    "Resource": [
		        "arn:aws:s3:::<bucket_name>/*"
		    ]
		}
	    ]
	}

	e. Choose Create Policy.
	f. In the navigation pane, choose Roles, and then choose Create New Role	
	g. In the Role Name box, give the IAM instance profile a name like 's3access', and then choose Next Step.
	h. On the Select Role Type page, next to Amazon EC2, choose Select.
	i. On the Attach Policy page, select the box next to S3-Permissions, and then choose Next Step.
	j. Choose Create Role.

**********************************************************************
CHANGES FOR BUCKET NAME

1. Type in your bucket name in line 4 of sendtoS3.sh script.
2. Change the bucket_name in line 9 of partition.py file.
***********************************************************************

STEPS TO RUN THE PROGRAM on ec2 instances:

1) make jar
2) make mode
3) ./start-cluster.sh 2 
4) python partition.py x
5) time ./sort.sh
6) ./stop-cluster.sh
7) aws s3 ls s3://your-bucket/output


Note: If you are not able to do ssh to any instance in one attempt, please try 3-4 times as your network connection might not be that strong.


