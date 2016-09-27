#Author: Vishal Mehta, Harshali Singh
#!/bin/bash
#finds the instance IP

curl http://169.254.169.254/latest/meta-data/public-ipv4 > myip.txt
