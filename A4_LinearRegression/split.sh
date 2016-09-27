#!/bin/bash
# Author: Harshali Singh, Vishal Mehta

#awk -v path=output/ '{f= path $1".txt"}
#{print >> f}' output/finalOutput


awk '{print >> ($1".txt")}' finalOutput
