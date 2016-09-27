format:
	rm -rf /tmp/hadoop*/*
	hdfs namenode -format

hstart:
	start-dfs.sh
	start-yarn.sh
	mr-jobhistory-daemon.sh start historyserver

hstop:
	mr-jobhistory-daemon.sh stop historyserver
	stop-yarn.sh
	stop-dfs.sh

hduser:
	hadoop fs -mkdir -p /user/hduser
	hadoop fs -mkdir -p /user/hduser/input

hdfs:
	hadoop fs -put all /user/hduser/input/

delete:
	#hadoop fs -rm -r -f /user/hduser/output
	rm -rf output
	
compile:
	rm -rf ClusterAnalysis.jar
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-2.6.0.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.6.0.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.6.0.jar:/usr/local/hadoop/share/hadoop/utility/commons-cli-1.2.jar:/usr/local/hadoop/share/hadoop/utility/commons-csv-1.2.jar ClusterAnalysis.java 
	jar cvf ClusterAnalysis.jar *.class
	
run:
	hadoop ClusterAnalysis /user/hduser/input/all output
	hadoop fs -get output
	cat output/part* > finalOutput
	R < markdown.R --no-save
	sudo xdg-open plot.png
	firefox Report.html

pseudo:	hstop format hstart hduser delete compile run hstop

cluster:
	aws s3 rm s3://cs6240-viha/output --recursive
	aws emr create-cluster \
	--name "CLI Test Cluster" \
	--release-label emr-4.3.0 \
	--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge \
                      InstanceGroupType=CORE,InstanceCount=2,InstanceType=m3.xlarge \
	--steps Type=CUSTOM_JAR,Name="CLI Test JAR Step",ActionOnFailure=CONTINUE,Jar=s3://cs6240-viha/job/ClusterAnalysis.jar,MainClass=ClusterAnalysis,Args=[s3://cs6240-viha/input/all,s3://cs6240-viha/output] \
	--auto-terminate \
	--log-uri s3://cs6240-viha/logs \
	--service-role EMR_DefaultRole \
	--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-west-2a\
	--enable-debugging

getawsdata:
	aws s3 cp s3://cs6240-viha/output result  --recursive  
	cat result/part* > finalOutput
	R < markdown.R --no-save
	sudo xdg-open plot.png
	firefox Report.html
	
emr: compile cluster getsawsdata
 
