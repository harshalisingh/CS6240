dAuthor: Harshali Singh, Vishal Mehta

Pre-Requisites:
-------------------

Extract Singh_Mehta_A3.tar.gz and Navigate to the folder "Singh_Mehta_A3" containing the Makefile and all files.

I. To run the benchmarking harness for Single-Threaded and Multi-Threaded configurations
-- The all folder containing *.csv.gz should be present where the MeanSingle, MedianSingle, FastMedianSingle, MeanMulti, MedianMulti, FastMedianMulti java files are present.

II. To run the benchmarking harness for Psuedo-distributed mode
** Make sure HADOOP_CLASSPATH is set correctly before running any command in terminal.

Command: export HADOOP_CLASSPATH=.:`hadoop classpath`

** Make sure all *.csv.gz is already present in HDFS within /user/hduser/input
** Make sure output is not already present in HDFS and localfile system
** We are not performing hadoop namenode -format

1. Command to run:

make compile
make run

2. The java program has following dependencies:
   - Java 1.7
   - hadoop-annotations-2.6.0.jar
   - hadoop-common-2.6.0.jar
   - hadoop-mapreduce-client-core-2.6.0.jar

3. Sample Input : /user/hduser/input (already mentioned in makefile)
   Sample Output: output

4. Pre-requisite libraries to run RScript:
   - R
   - ggplot2
   - rmarkdown  
   - pandoc version 1.12.3 or higher

5. The final output(csv file) from all the reducers should be at the same directory level as the markdown.R script.

III. Scala

-- Threee scala folders:
MeanScala, MedianScala, FastMedianScala

--Please navigate to corresponding folders to find the mean, median or fastmedian scala files in the command prompt.
-- Make sure all - data folder is present in each folder to execute the files.
--Command:
make run

** Make sure you do not have out folder

1. Prerequisites:
   - Scala
   - SBT

IV. To run on AWS

AWSAccessKeyId=AKIAJMCJGR5DWIFW4U2A
AWSSecretKey=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

NOTE: We are not pinging the cluster everytime to get the cluster ID in order to download the output.


--Upload the 3 Jar files of Mean, Median and FastMedian in S3 management console in separate clusters.

Then run the following command:
-- Command:
make runcluster 

*********************OUTPUT******************************
The output is a Report.pdf file which explains our implementation and the plot.







