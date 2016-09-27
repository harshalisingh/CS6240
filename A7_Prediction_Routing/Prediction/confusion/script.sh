#!/bin/bash
# Author: Harshali Singh, Vishal Mehta

awk '{ one+=$1; two+=$2; three+=$3; four+=$4} END {print one"\t "two"\t "three"\t "four"\t "(one+two)*100/(one+two+three+four)"%"}' matrix > accuracy.tmp

