#Author: Vishal Mehta, Harshali Singh

jar:
	sbt package
	cp target/scala-*/confusion_*.jar Confusion.jar

run: jar
	rm -rf out
	sbt run
	cat out/part* > matrix

clean:
	rm -rf out derby.log metastore_db project target
	rm -rf *.class
	rm -rf *.jar
	#rm -rf *.txt

script:
	chmod 777 script.sh
	./script.sh

