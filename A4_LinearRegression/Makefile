#Author: Vishal Mehta , Harshali Singh

compile:
	#LinearRegression
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-2.6.3.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.6.3.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.6.3.jar:/usr/local/hadoop/share/hadoop/utility/commons-cli-1.2.jar:/usr/local/hadoop/share/hadoop/utility/commons-csv-1.2.jar LinearRegression.java
	jar cvf LinearRegression.jar *.class

split:
	chmod 777 split.sh
	./split.sh

clean:
	rm -rf *.class
	rm -rf *.jar
	rm -rf *.txt

emr:
	aws s3 rm s3://{bucket_name}/output --recursive
	aws emr create-cluster \
	--name "Test Cluster_A4" \
	--release-label emr-4.3.0 \
	--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=c1.medium \
                      InstanceGroupType=CORE,InstanceCount=2,InstanceType=c1.medium \
	--steps Type=CUSTOM_JAR,Name="Test_A4 JAR Step",ActionOnFailure=CONTINUE,Jar=s3://{bucket_name}/job/LinearRegression.jar,MainClass=LinearRegression,Args=[s3://{bucket_name}/input,s3://{bucket_name}/output] \
	--auto-terminate \
	--log-uri s3://{bucket_name}/logs \
	--service-role EMR_DefaultRole \
	--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-west-2a \
	--enable-debugging > Clusterid.txt

output:
	#Check the status of the cluster
	python test.py
	# get the output
	aws s3 sync s3://{bucket_name}/output output 
	cat output/part* > finalOutput
	rm -rf *.txt

run: emr output

report:
	echo "Generating 26 Plots..."
	Rscript -e "rmarkdown::render('Report_A4.Rmd')"
	convert *.png plot.pdf
	rm -rf *.png
