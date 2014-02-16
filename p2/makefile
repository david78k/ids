mainclass = PageRank
jar = PageRank.jar
jars = hadoop-core-1.0.3.jar:jsoup-1.7.3.jar:commons-io.jar:mahout-examples-0.3.jar
bucket = david78k-ids
basedir = $(bucket)/results
instance_type = m1.small
#instance_type = m2.4xlarge # max 20
#instance_type = m1.medium # max 20
#instance_type = m1.xlarge # max 20
#instance_type = c1.xlarge # max 20
#instance_type = hi1.4xlarge # max 2
#instance_type = hs1.8xlarge # max 2
num_instances = 10
#num_instances = 3
#num_instances = 20 # total max quota 20

run: compile jar hadoop2

runemr: compile jar s3 emr

enron:

netflix:

create:
	#sed -i "s/^M//" data/enron.100/enron.100.refined.tab
	#tr -d $'\r' < data/enron.100.tab > data/enron.100/enron.100.refined.tab
	hive -f data/create.enron.sql

show:
	hive -e 'select * from enron'

show1:
	hive -e 'select * from enron limit 1'

eid:
	hive -e 'select eid from enron'

count:
	hive -e 'select count(*) from enron'

hadoop2: 
	#rm -rf david78k-ids/results/PageRank.inlink.out
	#hadoop-1.0.3/bin/hadoop dfs -rmr $(bucket)/results
	#hadoop jar PageRank.jar PageRank.PageRank $(bucket) data/enwiki.xml
	#hadoop jar PageRank.jar PageRank.PageRank $(bucket) data/5000000.xml
	#hadoop jar PageRank.jar PageRank.PageRank $(bucket) data/1000000.xml
	hadoop jar PageRank.jar PageRank.PageRank $(bucket) data/10000.xml
	#hadoop jar PageRank.jar PageRank.PageRank $(bucket) data/1000.xml
	#hadoop/bin/hadoop dfs -cat $(bucket)/results/part-00000 | tail

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
	tar cvf Project2.tar report.pdf presentation.pdf src/*

show:
	hadoop fs -cat $(bucket)/results/PageRank.n.out

showmore:
	hadoop fs -cat $(bucket)/results/PageRank.n.out
	hadoop fs -cat $(basedir)/PageRank.iter1.out | head -20 | nl

compile:
	javac -classpath $(jars) -d PageRank PageRank/PageRank.java
	#javac -classpath $(jars) -d WordCount WordCount/WordCount.java

jar:
	#jar -cf WordCount.jar -C WordCount/ . 
	jar -cf PageRank.jar -C PageRank/ . 

wordcount:
	javac -classpath $(jars) -d WordCount WordCount/WordCount.java
	jar -cf WordCount.jar -C WordCount/ . 
	hadoop-1.0.3/bin/hadoop dfs -rmr output
	hadoop-1.0.3/bin/hadoop jar WordCount.jar WordCount data/100.xml output
	hadoop-1.0.3/bin/hadoop dfs -cat output/part-00000 | tail

