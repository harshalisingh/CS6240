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

hduser:
	hadoop fs -mkdir -p /user/hduser
	hadoop fs -mkdir -p /user/hduser/input

hdfs:
	hadoop fs -put a7history/* /user/hduser/input/a7history/

delete:
	hadoop fs -rm -r -f /user/hduser/models
	hadoop fs -rm -r -f /user/hduser/output
	hadoop fs -rm -r -f /user/hduser/intermediate_output
	hadoop fs -rm -r -f /user/hduser/optimal
	rm -rf output
	rm -rf out

jar:
	rm -rf target
	rm -rf project/project
	rm -rf project/target
	sbt assembly
	cp target/Routing-*.jar Routing.jar

pseudo:
	hadoop jar Routing.jar input output
	hadoop fs -get output
	hadoop fs -get optimal
	cat output/part* > testOutput
	cat optimal/part* > confusion/optimalOutput

clean:
	rm -rf out derby.log metastore_db project target
	rm -rf *.class
	rm -rf *.jar
	rm -rf *.txt

emr:
	#aws s3 mb s3://${bucket_name}
	aws s3 cp Routing.jar s3://${bucket_name}/job/
	aws s3 rm s3://${bucket_name}/output --recursive
	aws emr create-cluster \
	--name "Routing Cluster_A7" \
	--release-label emr-4.3.0 \
	--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=c1.medium \
                      InstanceGroupType=CORE,InstanceCount=2,InstanceType=c1.medium \
	--steps Type=CUSTOM_JAR,Name="Routing_A7 JAR Step",ActionOnFailure=CONTINUE,Jar=s3://${bucket_name}/job/Routing.jar,MainClass=Routing,Args=[s3://${bucket_name}/input,s3://${bucket_name}/output] \
	--auto-terminate \
	--log-uri s3://${bucket_name}/logs \
	--service-role EMR_DefaultRole \
	--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-west-2a \
	--enable-debugging > Clusterid.txt

output:
	#Check the status of the cluster
	python test.py
	# get the output
	aws s3 sync s3://${bucket_name}/output output 
	aws s3 sync s3://${bucket_name}/optimal optimal
	cat output/part* > testOutput
	cat optimal/part* > optimalOutput
	#rm -rf *.txt

run: emr output

report:
	Rscript -e "rmarkdown::render('Report_A7Routing.Rmd')"

weka:
	export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:`echo /home/hduser/Downloads/weka/*.jar | sed 's/ /:/g'`
