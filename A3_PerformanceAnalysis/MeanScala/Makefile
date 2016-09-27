#!/bin/bash
# Author: Nat Tuck
# Modified by: Vishal Mehta,Harshali Singh

jar:
	sbt package
	cp target/scala-*/meancomparison_*.jar MeanComparison.jar

run: 	jar
	rm -rf out
	#sbt run
	sbt "run all out"

hadoop: jar
	rm -rf out
	HADOOP_CLASSPATH=`perl cp.pl` hadoop jar MeanComparison.jar

clean:
	rm -rf project target *.jar out
	
