A8 - README
Team Members: Harshali Singh, Vishal Mehta, Akanksha Mishra, Saahil Singla

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


