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
	hive -f src/create.enron.sql
	#hive -e "load data local inpath 'data/enron.refined.tab' overwrite into table enron"
	#hive -e 'select * from enron limit 5' | nl
	#hive -e 'select count(*) from enron'

create100:
	#sed -i "s/^M//" data/enron.100/enron.100.refined.tab
	#tr -d $'\r' < data/enron.100.tab > data/enron.100/enron.100.refined.tab
	hive -f src/create.enron100.sql
	#hive -e "load data local inpath 'data/enron.100.refined.tab' overwrite into table enron100"
	#hive -e 'select * from enron100 limit 5' | nl
	#hive -e 'select count(*) from enron100'

show:
	hive -e 'select * from enron'

show1:
	hive -e 'select * from enron limit 1'

eid:
	hive -e 'select eid from enron'

count:
	hive -e 'select count(*) from enron'

tar:
	tar cvf Project2.tar report.pdf presentation.pdf src/*

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

showmore:
	hadoop fs -cat $(bucket)/results/PageRank.n.out
	hadoop fs -cat $(basedir)/PageRank.iter1.out | head -20 | nl

