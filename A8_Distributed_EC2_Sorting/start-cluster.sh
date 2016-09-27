#Author: Vishal Mehta
#!/bin/bash
# Spawn N instances

cmds="sudo yum install java-1.7.0-openjdk java-1.7.0-openjdk-devel -y; export JAVA_HOME=/usr/lib/jvm/jre-1.7.0-openjdk.x86_64"

if [ -z "$1" ]; then 
	echo "Usage: ./start.sh N<number of nodes>"
else 
#create instances
aws ec2 run-instances --image-id ami-c229c0a2 --iam-instance-profile Name="s3access" --count $1 --instance-type t2.medium --key-name EC2_KP --security-groups my-sg

echo "Waiting for instances to start and ssh on it..."
sleep 90

aws ec2 describe-instances --filters "Name=instance-type,Values=t2.medium" | jq -r ".Reservations[].Instances[].NetworkInterfaces[0].Association.PublicIp" > iplist.txt

#fetch publicDNS of all instances and store them in an array.
aws ec2 describe-instances --filters "Name=instance-type,Values=t2.medium" | jq -r ".Reservations[].Instances[].PublicDnsName" > dns.txt
publicdns=$(aws ec2 describe-instances --filters "Name=instance-type,Values=t2.medium" | jq -r ".Reservations[].Instances[].PublicDnsName")
publicdnsarr=( $publicdns )

dns_len=${#publicdnsarr[@]}

#ssh to and transfer files to each instance.
for (( i=0; i<${dns_len}; i++ ));
do
	echo "Attempting ssh to " ${publicdnsarr[i]}
	scp -i EC2_KP.pem sendtoS3.sh ec2-user@${publicdnsarr[i]}:~
	scp -i EC2_KP.pem findmyip.sh ec2-user@${publicdnsarr[i]}:~ 
	ssh -i EC2_KP.pem -o StrictHostKeyChecking=no ec2-user@${publicdnsarr[i]} "${cmds}; chmod 777 sendtoS3.sh; chmod 777 findmyip.sh"
	scp -i EC2_KP.pem iplist.txt ec2-user@${publicdnsarr[i]}:~
	scp -i EC2_KP.pem node.jar ec2-user@${publicdnsarr[i]}:~
done
fi
