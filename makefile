all: run upload

compile:
	javac -classpath mtj-1.0.1.jar:hadoop-core-1.0.3.jar:jsoup-1.7.3.jar PageRank.java
	#javac -classpath mtj-1.0.1.jar hadoop-core-1.0.3.jar PageRank.java
	#javac PageRank.java

compemr:
	javac -classpath HADOOP HOME/hadoop-HADOOP VERSION-core.jar -d PageRank PageRank.java
	#2. create jar:
	jar -cvf PageRank.jar -C PageRank/ .
	#3. run in local:
	hadoop jar PageRank.jar {main-class PageRank.PageRank input output

run: compile
	java -classpath .:mtj-1.0.1.jar:hadoop-core-1.0.3.jar:jsoup-1.7.3.jar PageRank
	#java PageRank

jar:
	jar -cvf PageRank.jar .

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
