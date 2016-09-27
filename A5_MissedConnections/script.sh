#!/bin/bash
# Author: Harshali Singh, Vishal Mehta

awk '{ a[$1", "$2]+=$4; b[$1", "$2]+=$5} END {for (i in a) {print i"\t "a[i]"\t "b[i]"\t "(b[i] * 100)/a[i]}}' finalOutput | sort > output.tmp

