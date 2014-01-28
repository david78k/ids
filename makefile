#jars = mtj-1.0.1.jar:hadoop-core-1.0.3.jar:jsoup-1.7.3.jar:commons-math3-3.2.jar
jars = hadoop-core-1.0.3.jar:jsoup-1.7.3.jar:commons-math3-3.2.jar

all: run upload

run: compile jar hadoop
	#java -classpath .:$(jars) PageRank
	#java -classpath .:mtj-1.0.1.jar:hadoop-core-1.0.3.jar:jsoup-1.7.3.jar PageRank

compile:
	javac -classpath $(jars) -d PageRank PageRank/PageRank.java
	#javac -classpath $(jars) -d WordCount WordCount/WordCount.java
	#javac -classpath mtj-1.0.1.jar:hadoop-core-1.0.3.jar:jsoup-1.7.3.jar PageRank.java

jar:
	#jar -cf WordCount.jar -C WordCount/ . 
	jar -cf PageRank.jar -C PageRank/ . 
	#jar -cf PageRank.jar -C PageRank/ . -C commons-math/commons-math3-3.2/commons-math3-3.2/ .
	#jar -cvf PageRank.jar -C PageRank/ . -C commons-math/commons-math3-3.2/commons-math3-3.2/ . -C jsoup/ .
	#jar -cvf PageRank.jar PageRank commons-math3-3.2.jar
	#jar -cvf PageRank.jar -C PageRank/ . commons-math3-3.2.jar

hadoop: 
	rm -rf david78k-ids/results
	#hadoop-1.0.3/bin/hadoop dfs -rmr david78k-ids/results
	hadoop-1.0.3/bin/hadoop jar PageRank.jar PageRank.PageRank david78k-ids 
	#hadoop-1.0.3/bin/hadoop jar PageRank.jar PageRank input output
	#hadoop-1.0.3/bin/hadoop jar PageRank.jar --main-class PageRank.PageRank input output

wordcount:
	javac -classpath $(jars) -d WordCount WordCount/WordCount.java
	jar -cf WordCount.jar -C WordCount/ . 
	hadoop-1.0.3/bin/hadoop dfs -rmr output
	hadoop-1.0.3/bin/hadoop jar WordCount.jar WordCount data/100.xml output
	hadoop-1.0.3/bin/hadoop dfs -cat output/part-00000 | tail

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

compemr:
	javac -classpath HADOOP HOME/hadoop-HADOOP VERSION-core.jar -d PageRank PageRank.java
	#2. create jar:
	jar -cvf PageRank.jar -C PageRank/ .
	#3. run in local:
	hadoop jar PageRank.jar {main-class PageRank.PageRank input output

