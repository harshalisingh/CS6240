#Author: Harshali Singh	
#!/bin/bash

#fetch the instance IP
while read line; do
	ssh -i EC2_KP.pem -o StrictHostKeyChecking=no ec2-user@${line} "./findmyip.sh" & < /dev/null
done < dns.txt

#ssh to each instance and execute the program 
while read line; do
	ssh -i EC2_KP.pem -o StrictHostKeyChecking=no ec2-user@${line} "java -jar node.jar input output" & < /dev/null
done < dns.txt

#Send output to S3 bucket
while read line; do
	ssh -i EC2_KP.pem -o StrictHostKeyChecking=no ec2-user@${line} "./sendtoS3.sh" & < /dev/null
done < dns.txt



