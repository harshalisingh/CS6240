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

	** Make sure you have the input files uploaded in input folder in your bucket.
	** Enter your bucket name in line 3 of Makefile.
	** Make sure HADOOP_CLASSPATH is set correctly before running any command.
		Command: export HADOOP_CLASSPATH=.:`hadoop classpath`

		1. Program Dependencies  
		   - Java 1.7
		   - hadoop-annotations-2.6.0.jar
		   - hadoop-common-2.6.0.jar
		   - hadoop-mapreduce-client-core-2.6.0.jar
		NOTE: Change the hadoop jars version in line 19 of Makefile, according to your machine before compiling the code.

		2. Commands to run for EMR:
			make compile
			make run
			make script
			make report
		
		3. Commands to run on pseudo:
			make compile
			make pseudo
			make script
			make report
		
			
NOTE: If EMR is not working for you, try to check the parameters in Makefile, from line 38 to line 52.

II. Final output is in output.tmp file which has total number of connections, missed connections and percentage of missed connections per airline per year.

Output format:
1st column - Carrier code
2nd column - Year
3rd column - Total number of connections
4th column - Total Number of missed connections
5th column - percentage of missed connections 

III. Attached is the report in HTML which contains implementation details,considerations and conclusion. 













