#!/bin/bash
# Author: Nat Tuck
# Modified by: Vishal Mehta

jar:
	sbt package
	cp target/scala-*/mediancomparison_*.jar MedianComparison.jar

run: 	jar
	rm -rf out
	sbt "run all out"

hadoop: jar
	rm -rf out
	HADOOP_CLASSPATH=`perl cp.pl` hadoop jar MedianComparison.jar

clean:
	rm -rf project target *.jar out
