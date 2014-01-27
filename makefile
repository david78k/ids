#jars = mtj-1.0.1.jar:hadoop-core-1.0.3.jar:jsoup-1.7.3.jar:commons-math3-3.2.jar
jars = hadoop-core-1.0.3.jar:jsoup-1.7.3.jar:commons-math3-3.2.jar

all: run upload

compile:
	javac -classpath $(jars) -d PageRank PageRank.java
	#javac -classpath mtj-1.0.1.jar:hadoop-core-1.0.3.jar:jsoup-1.7.3.jar PageRank.java

compemr:
	javac -classpath HADOOP HOME/hadoop-HADOOP VERSION-core.jar -d PageRank PageRank.java
	#2. create jar:
	jar -cvf PageRank.jar -C PageRank/ .
	#3. run in local:
	hadoop jar PageRank.jar {main-class PageRank.PageRank input output

run: compile jar hadoop
	#java -classpath .:$(jars) PageRank
	#java -classpath .:mtj-1.0.1.jar:hadoop-core-1.0.3.jar:jsoup-1.7.3.jar PageRank

hadoop: 
	#hadoop-1.0.3/bin/hadoop jar PageRank.jar PageRank input output
	hadoop-1.0.3/bin/hadoop jar PageRank.jar PageRank.PageRank input output
	#hadoop-1.0.3/bin/hadoop jar PageRank.jar --main-class PageRank.PageRank input output

jar:
	jar -cvf PageRank.jar -C PageRank/ . -C commons-math/commons-math3-3.2/commons-math3-3.2/ .
	#jar -cvf PageRank.jar -C PageRank/ .
	#jar -cvf PageRank.jar PageRank commons-math3-3.2.jar
	#jar -cvf PageRank.jar -C PageRank/ . commons-math3-3.2.jar

emr:
	elastic-mapreduce --create --name "PageRank" --ami-version 2.4.2 \
	--instance-type <type> --num-instances <num> \
	--jar s3n://your-bucket-name/job/PageRank.jar \
	--main-class PageRank.PageRank \
	--arg your-bucket-name

upload:
	git add .
	git commit -a -m update
	git push
