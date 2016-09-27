#Author: Vishal Mehta , Harshali Singh

bucket_name = 

hstart:
	start-dfs.sh
	start-yarn.sh
	mr-jobhistory-daemon.sh start historyserver

hstop:
	mr-jobhistory-daemon.sh stop historyserver
	stop-yarn.sh
	stop-dfs.sh

delete:
	hadoop fs -rm -r -f /user/hduser/output
	rm -rf output

compile:
	#MissedConnection
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-2.6.0.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.6.0.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.6.0.jar:/usr/local/hadoop/share/hadoop/utility/commons-cli-1.2.jar missed/*.java
	jar cvf MissedConnection.jar missed/*.class

pseudo:
	hadoop jar MissedConnection.jar missed.MissedConnection input output
	hadoop fs -get output
	cat output/part* > finalOutput

script:
	chmod 777 script.sh
	./script.sh
	
clean:
	rm -rf *.class
	rm -rf *.jar
	rm -rf *.txt

emr:
	#aws s3 mb s3://${bucket_name}
	aws s3 cp MissedConnection.jar s3://${bucket_name}/job/
	#aws s3 cp all s3://${bucket_name}/input/ --recursive
	aws s3 rm s3://${bucket_name}/output --recursive
	aws emr create-cluster \
	--name "Test Cluster_A5" \
	--release-label emr-4.3.0 \
	--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=c1.medium \
                      InstanceGroupType=CORE,InstanceCount=2,InstanceType=c1.medium \
	--steps Type=CUSTOM_JAR,Name="Test_A5 JAR Step",ActionOnFailure=CONTINUE,Jar=s3://${bucket_name}/job/MissedConnection.jar,MainClass=missed.MissedConnection,Args=[s3://${bucket_name}/input,s3://${bucket_name}/output] \
	--auto-terminate \
	--log-uri s3://${bucket_name}/logs \
	--service-role EMR_DefaultRole \
	--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-west-2a \
	--enable-debugging > Clusterid.txt

output:
	#Check the status of the cluster
	time python test.py
	# get the output
	aws s3 sync s3://${bucket_name}/output output 
	cat output/part* > finalOutput
	#rm -rf *.txt

run: emr output 

report:
	Rscript -e "rmarkdown::render('Report_A5.Rmd')"
