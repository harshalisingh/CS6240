Author: Harshali Singh, Vishal Mehta

Instructions to Build and execute the code :
-------------------------------------------
I. To run the program on EMR. 

	** Type aws configure in command prompt.

	** Set your Amazon credentials and default output format as json in prompted fields in command prompt as shown below:
		AWS Access Key ID : 
		AWS Secret Access Key : 
		Default region name : us-west-2
		Default output format : json

	** Upload the required Jar files of Routing.jar in the job folder of S3 bucket after compiling the JAVA file.
	** Make sure you have the input files uploaded in input folder in your bucket.
	** Replace '{bucket_name}' with your bucket name in line 3 of Makefile.
	** Make sure HADOOP_CLASSPATH is set correctly before running any command.
		Command: export HADOOP_CLASSPATH=.:`hadoop classpath`

	**Make sure to run this command:
		export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:`echo /home/hduser/Downloads/weka/*.jar | sed 's/ /:/g'`

		1. Program Dependencies  
		   - Java 1.7
		   - hadoop-annotations-2.6.0.jar
		   - hadoop-common-2.6.0.jar
		   - hadoop-mapreduce-client-core-2.6.0.jar
	           - weka-3.7.3.jar
		NOTE: Change the hadoop jars version in line 5 of Makefile, according to your machine before compiling the code.

		2. Commands to run on AWS:
			make jar
			make run

		3. Commands to run on Pseudo:
			make delete
		        make jar
			make pseudo

		4. Command to run Confusion Matrix: 
                  (Make sure to have a7validate file in confusion folder)
                  (cd to confusion folder and run following commands)
                        make run
                        make script
			
			
NOTE: If EMR is not working for you, try to check the parameters in Makefile, from line 16 to line 33.













