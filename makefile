mainclass = PageRank
jar = PageRank.jar
#jars = mtj-1.0.1.jar:hadoop-core-1.0.3.jar:jsoup-1.7.3.jar:commons-math3-3.2.jar:mahout-integration-0.8.jar:cloud9-1.5.0.jar
jars = hadoop-core-1.0.3.jar:jsoup-1.7.3.jar:commons-math3-3.2.jar:commons-io.jar:mahout-examples-0.3.jar
bucket = david78k-ids
basedir = $(bucket)/results
instance_type = m1.small
#instance_type = m2.4xlarge # max 20
instance_type = m1.xlarge # max 20
#instance_type = c1.xlarge # max 20
#instance_type = hi1.4xlarge # max 2
#instance_type = hs1.8xlarge # max 2
num_instances = 10
#num_instances = 3
#num_instances = 20 # total max quota 20
#num_instances = 2

#instance_type = c3.4xlarge # not supported, max 20
#instance_type = i2.2xlarge # not supported, max 8
#instance_type = i2.4xlarge # not supported, max 4
#instance_type = i2.8xlarge # not supported, max 2
#instance_type = c3.8xlarge # not supported
#instance_type = cr1.8xlarge # not supported
#instance_type = m3.2xlarge # not supported
#instance_type = m3.xlarge # not supported

all: run upload

run: compile jar hadoop2
	#java -classpath .:$(jars) PageRank
	#java -classpath .:mtj-1.0.1.jar:hadoop-core-1.0.3.jar:jsoup-1.7.3.jar PageRank

runemr: compile jar s3 emr

hadoop2: 
	#rm -rf david78k-ids/results/PageRank.inlink.out
	#hadoop-1.0.3/bin/hadoop dfs -rmr $(bucket)/results
	#hadoop/bin/hadoop jar PageRank.jar PageRank.PageRank $(bucket) data/enwiki.xml
	#hadoop/bin/hadoop jar PageRank.jar PageRank.PageRank $(bucket) data/5000000.xml
	hadoop/bin/hadoop jar PageRank.jar PageRank.PageRank $(bucket) data/1000000.xml
	#hadoop/bin/hadoop jar PageRank.jar PageRank.PageRank $(bucket) data/1000.xml
	hadoop/bin/hadoop dfs -cat $(bucket)/results/part-00000 | tail

emr:
	emr-cli/elastic-mapreduce --create --name "PageRank" --ami-version 2.4.2 \
	--instance-type $(instance_type) --num-instances $(num_instances) \
	--jar s3n://$(bucket)/job/PageRank.jar \
	--main-class PageRank.PageRank \
	--log-uri s3n://$(bucket)/logs \
	--arg $(bucket) 
	#--arg $(bucket) \
	#--arg s3://$(bucket)/input/5000000.xml 
	#--arg s3://$(bucket)/input/1000000.xml 
	#--arg s3://$(bucket)/input/1000.xml 

	#--arg s3://$(bucket)/input/100.xml 
	#--args $(bucket),s3://$(bucket)/input/100.xml

s3:
	s3cmd put PageRank.jar s3://$(bucket)/job/PageRank.jar 

upload:
	git add .
	git commit -a -m update
	git push

tar:
	tar cvf PageRank.tar report.txt PageRank/PageRank.java PageRank.jar
	#tar cvf PageRank.tar report.txt PageRank/*.java PageRank.jar

show:
	hadoop fs -cat $(bucket)/results/PageRank.n.out
	hadoop fs -cat $(basedir)/PageRank.iter1.out | head | nl
	hadoop fs -cat $(basedir)/PageRank.iter8.out | head | nl

compile:
	javac -classpath $(jars) -d PageRank PageRank/PageRank.java
	#javac -classpath $(jars) -d WordCount WordCount/WordCount.java
	#javac -classpath mtj-1.0.1.jar:hadoop-core-1.0.3.jar:jsoup-1.7.3.jar PageRank.java

jar:
	#jar -cf WordCount.jar -C WordCount/ . 
	jar -cf PageRank.jar -C PageRank/ . 
	#jar -cf PageRank.jar -C PageRank/ . -C commons-math/commons-math3-3.2/commons-math3-3.2/ .
	#jar -cvf PageRank.jar -C PageRank/ . commons-math3-3.2.jar

wordcount:
	javac -classpath $(jars) -d WordCount WordCount/WordCount.java
	jar -cf WordCount.jar -C WordCount/ . 
	hadoop-1.0.3/bin/hadoop dfs -rmr output
	hadoop-1.0.3/bin/hadoop jar WordCount.jar WordCount data/100.xml output
	hadoop-1.0.3/bin/hadoop dfs -cat output/part-00000 | tail

