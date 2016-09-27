#Author: Vishal Mehta, Harshali Singh

bucket_name = ${bucket_name}

jar:
	sbt package
	cp target/scala-*/missed-connection_*.jar MissedConnection.jar

run: jar
	rm -rf out
	sbt "run all out"
	cat out/part* > finalOutput

clean:
	rm -rf out derby.log metastore_db project target
	rm -rf *.class
	rm -rf *.jar
	#rm -rf *.txt

script:
	chmod 777 script.sh
	./script.sh

emrspark:
	#aws s3 mb s3://${bucket_name}
	aws s3 cp MissedConnection.jar s3://${bucket_name}/job/
	#aws s3 cp all s3://${bucket_name}/input/ --recursive
	aws s3 rm s3://${bucket_name}/output --recursive
	aws emr create-cluster --name "Spark Cluster" --ami-version 3.11.0 --applications Name=Spark \
	--instance-type c1.medium --log-uri s3://{bucket_name}/logs --service-role EMR_DefaultRole \
 	--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-west-2a --enable-debugging --instance-count 3 \ 		--auto-terminate > Clusterid.txt
	aws emr add-steps --cluster-id $(cat Clusterid.txt| jq -r .ClusterId) --steps Type=Spark,Name="Spark JAR",Args=[--deploy-mode,cluster,--class,MissedConnection,--verbose,s3://{bucket_name}MissedConnection.jar,s3://{bucket_name}/input,s3://{bucket_name}/output]
	
output:
	#Check the status of the cluster
	python test.py
	# get the output
	aws s3 sync s3://${bucket_name}/output out 
	cat out/part* > finalOutput

report:
	Rscript -e "rmarkdown::render('Report_A6.Rmd')"

awsspark: jar emrspark output

