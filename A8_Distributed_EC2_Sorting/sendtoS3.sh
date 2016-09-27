#Author: Vishal Mehta, Harshali Singh	
#!/bin/bash

bucket_name=
DIR="~/output"
# init
i=0
maxtries=10
# look for empty dir 
while true 
do
	if [ "$(ls -A $DIR)" ]; then	
	     aws s3 cp --recursive ~/output s3://${bucket_name}/output
	     exit 
	else 		
		if [ $i -gt $maxtries ]; then
		 	exit				
		fi		
		sleep 60
		echo "waiting for output from instances"
		i=`expr $i + 1`	
	fi 
done


