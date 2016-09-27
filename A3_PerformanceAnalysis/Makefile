#Author: Vishal Mehta , Harshali Singh

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

delete:
	hadoop fs -rm -r -f /user/hduser/output
	rm -rf output
	
singlec:
	#Mean
	javac MeanSingle.java 
	#Median
	javac MedianSingle.java
	#FastMedian
	javac FastMedianSingle.java

multic:
	#Mean
	javac MeanMulti.java
	#Median
	javac MedianMulti.java
	#FastMedian
	javac FastMedianMulti.java

pseudoc:#pseudo
	#Mean
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-2.6.3.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.6.3.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.6.3.jar:/usr/local/hadoop/share/hadoop/utility/commons-cli-1.2.jar:/usr/local/hadoop/share/hadoop/utility/commons-csv-1.2.jar MeanComparison.java
	jar cvf MeanComparison.jar *.class
	#Median
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-2.6.3.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.6.3.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.6.3.jar:/usr/local/hadoop/share/hadoop/utility/commons-cli-1.2.jar:/usr/local/hadoop/share/hadoop/utility/commons-csv-1.2.jar MedianComparison.java
	jar cvf MedianComparison.jar *.class
	#FastMedian
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-2.6.3.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.6.3.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.6.3.jar:/usr/local/hadoop/share/hadoop/utility/commons-cli-1.2.jar:/usr/local/hadoop/share/hadoop/utility/commons-csv-1.2.jar FastMedianComparison.java
	jar cvf FastMedianComparison.jar *.class
	
clean:
	rm -rf *.class
	rm -rf *.jar
	rm -rf *.txt
	rm -rf *.csv
	
compile: singlec multic pseudoc 

header:
	echo "MODE,CONF,TIME" > benchmarking.csv

mean:delete
	echo -n "MEAN,SINGLE_THREADED,">> benchmarking.csv
	java MeanSingle -input=all > outputmeans.txt
	head -n 10 outputmeans.txt
	cat outputmeans.txt | tail -n 1 >> benchmarking.csv
	echo -n "MEAN,MULTI_THREADED,">> benchmarking.csv
	java MeanMulti -input=all > outputmeanm.txt
	head -n 10 outputmeanm.txt
	cat outputmeanm.txt | tail -n 1 >> benchmarking.csv
	echo -n "MEAN,PSEUDO,">> benchmarking.csv
	hadoop MeanComparison /user/hduser/input/ output
	hadoop fs -get output
	mv output/part-r-00000 part.txt
	head -n 10 part.txt
	cat timemean.txt | tail -n 1 >> benchmarking.csv
	echo -n "MEAN,SCALA,">> benchmarking.csv
	cat MeanScala/testmean.txt | tail -n 1 >> benchmarking.csv
	#meancluster

median:delete
	echo -n "MEDIAN,SINGLE_THREADED," >> benchmarking.csv
	java MedianSingle -input=all > outputmedians.txt
	head -n 10 outputmedians.txt
	cat outputmedians.txt | tail -n 1 >> benchmarking.csv
	echo -n "MEDIAN,MULTI_THREADED,">> benchmarking.csv
	java MedianMulti -input=all > outputmedianm.txt
	head -n 10 outputmedianm.txt
	cat outputmedianm.txt | tail -n 1 >> benchmarking.csv	
	echo -n "MEDIAN,PSEUDO,">> benchmarking.csv
	hadoop MedianComparison /user/hduser/input/ output
	hadoop fs -get output
	mv output/part-r-00000 part.txt
	head -n 10 part.txt
	cat timemedian.txt | tail -n 1 >> benchmarking.csv
	cat MedianScala/testmedian.txt | tail -n 1 >> benchmarking.csv
	#mediancluster

fastmedian:delete
	echo -n "FAST_MEDIAN,SINGLE_THREADED," >> benchmarking.csv
	java FastMedianSingle -input=all > outputfasts.txt
	head -n 10 outputfasts.txt
	cat outputfasts.txt | tail -n 1 >> benchmarking.csv
	echo -n "FAST_MEDIAN,MULTI_THREADED,">> benchmarking.csv
	java FastMedianMulti -input=all > outputfastm.txt
	head -n 10 outputfastm.txt
	cat outputfastm.txt | tail -n 1 >> benchmarking.csv
	echo -n "FAST_MEDIAN,PSEUDO,">> benchmarking.csv
	hadoop MedianComparison /user/hduser/input/ output
	hadoop fs -get output
	mv output/part-r-00000 part.txt
	head -n 10 part.txt
	cat timefast.txt | tail -n 1 >> benchmarking.csv
	cat FastMedianScala/testfast.txt | tail -n 1 >> benchmarking.csv
	#fastmediancluster

run:	header mean median fastmedian r clean

runcluster: meancluster mediancluster fastmediancluster
	
r:
	R < markdown.R --no-save

pseudo:	hstop hstart sleep 

sleep:
	sleep 30

meancluster:
	aws s3 rm s3://harshali/output --recursive
	aws emr create-cluster \
	--name "CLI Test Cluster" \
	--release-label emr-4.3.0 \
	--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=c1.medium \
                      InstanceGroupType=CORE,InstanceCount=2,InstanceType=c1.medium \
	--steps Type=CUSTOM_JAR,Name="CLI Test JAR Step",ActionOnFailure=CONTINUE,Jar=s3://harshali/job/MeanComparison.jar,MainClass=MeanComparison,Args=[s3://harshali/input,s3://harshali/output] \
	--auto-terminate \
	--log-uri s3://harshali/logs \
	--service-role EMR_DefaultRole \
	--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-west-2a \
	--enable-debugging

mediancluster:
	aws s3 rm s3://harshali/output --recursive
	aws emr create-cluster \
	--name "CLI Test Cluster" \
	--release-label emr-4.3.0 \
	--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=c1.medium \
                      InstanceGroupType=CORE,InstanceCount=2,InstanceType=c1.medium \
	--steps Type=CUSTOM_JAR,Name="CLI Test JAR Step",ActionOnFailure=CONTINUE,Jar=s3://harshali/job/MedianComparison.jar,MainClass=MedianComparison,Args=[s3://harshali/input,s3://harshali/output] \
	--auto-terminate \
	--log-uri s3://harshali/logs \
	--service-role EMR_DefaultRole \
	--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-west-2a \
	--enable-debugging

fastmediancluster:
	aws s3 rm s3://harshali/output --recursive
	aws emr create-cluster \
	--name "CLI Test Cluster" \
	--release-label emr-4.3.0 \
	--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=c1.medium \
                      InstanceGroupType=CORE,InstanceCount=2,InstanceType=c1.medium \
	--steps Type=CUSTOM_JAR,Name="CLI Test JAR Step",ActionOnFailure=CONTINUE,Jar=s3://harshali/job/FastMedianComparison.jar,MainClass=FastMedianComparison,Args=[s3://harshali/input,s3://harshali/output] \
	--auto-terminate \
	--log-uri s3://harshali/logs \
	--service-role EMR_DefaultRole \
	--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-west-2a \
	--enable-debugging
	
 
