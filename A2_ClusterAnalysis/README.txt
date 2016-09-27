Pre-Requisites:
-------------------

Extract Singh_Mehta_A2.tar.gz and Navigate to the folder "Singh_Mehta_A2" containing the Makefile.

## CREDENTIALS FOR AWS ##

ACCESS KEY ID : AKIAJP3NENLAAURSSRKA
SECRET ACCESS KEY : 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

I. To run locally on Linux

** Make sure HADOOP_CLASSPATH is set correctly before running command.
Command: export HADOOP_CLASSPATH=.:`hadoop classpath`

** Make sure folder "all" containing *.csv.gz is already present in HDFS within /user/hduser/input/

** We are not performing hadoop namenode -format

1. RUN the following in Terminal (This will run the program on a Single-Node cluster running in Psuedo-distributed mode):
   make psuedo 
-- This command first stops already running hadoop services
-- Starts hadoop services
-- Creates an input folder for hduser 
-- Deletes any existing output folder (local or Hadoop filesystems) 
-- Compiles the ClusterAnalysis.java program (It checks for External Jars in the hadoop )
-- Runs the ClusterAnalysis.jar file created after compile step
-- Gets the output from HDFS to local file system
-- Merges the output to "finalOutput" 
-- Opens the Topten.png
-- Runs RScript markdown.r to create a dynamically generated HTML file Report.html.  

2. The ClusterAnalysis.java program has following dependencies:
   - Java 1.7
   - hadoop-annotations-2.6.0.jar
   - hadoop-common-2.6.0.jar
   - hadoop-mapreduce-client-core-2.6.0.jar
3. Sample Input : /user/hduser/input/all (The all folder within input must contain all *.csv.gz files)
   Sample Output: output

4. ouput directory should NOT be already present on Local FileSystem or Hadoop FileSystem.

5. Pre-requisite libraries to run RScript:
   - ggplot2
   - rmarkdown  
   - Pandoc version 1.12.3 or higher should be installed to generate the HTML output.
6. The final ouput from all the reducers should be at the same directory level as the markdown.R script.


II. To run on AWS

NOTE: We are not pinging the cluster everytime to get the cluster ID in order to download the output. We wait for the cluster steps to finish and then download the output files which will be fed into the R script file.


1. RUN the following in Terminal (This will run the program on Multiple clusters on AWS):
   make emr

This command:
-- Compiles the ClusterAnalysis.java
-- Creates a cluster on AWS, uploads the ClusterAnalysis.jar, Sets the Input and Output path of the buckets on S3
-- Gets the output from AWS to local output folder
-- Merges the output to "finalOutput" 
-- Runs RScript markdown.r to create a dynamically generated HTML file Report.html


*********************OUTPUT******************************
The output is a Report.html which describes our implementation and will be opened in Firefox on Linux. 




