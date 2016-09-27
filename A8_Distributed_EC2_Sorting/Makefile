#Author: Vishal Mehta , Harshali Singh

jar:
	rm -rf target
	rm -rf project/project
	rm -rf project/target
	sbt assembly
	cp target/Node-*.jar node.jar

clean:
	rm -rf out derby.log metastore_db project target
	rm -rf *.class
	rm -rf *.jar

mode:
	chmod 777 sendtoS3.sh
	chmod 777 findmyip.sh
	chmod 777 start-cluster.sh
	chmod 777 stop-cluster.sh
	chmod 777 sort.sh

report:
	Rscript -e "rmarkdown::render('Report_A8.Rmd')"
