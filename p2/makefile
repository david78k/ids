#https://s3.amazonaws.com/spring-2014-ds/movie_dataset/movie_ratings/movie_ratings.csv
movie_ratings=/spring-2014-ds/movie_dataset/movie_ratings/movie_ratings.csv
#movie_titles

bucket = david78k-ids
basedir = $(bucket)/input
outdir = $(bucket)/results

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

run: compile 

runemr: compile s3 emr

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

count:
	hive -e 'select count(*) from enron'

tar:
	cp dropbox/report.pdf dropbox/presentation.pdf ./
	tar cvf Project2.tar report.pdf presentation.pdf src/*
	cp Project2.tar dropbox/

emr: s3
	../p1/emr-cli/elastic-mapreduce --create --name "Hive" --hive-script \
	s3n://$(basedir)/most_similar_pairs.q \
	--instance-type $(instance_type) --num-instances $(num_instances) \
	--args -d,INPUT=s3n://$(basedir)/movie_ratings.30_40m/,\
	-d,OUTPUT=s3n://$(outdir)/


emr-test:
	../p1/emr-cli/elastic-mapreduce --create --name "Hive" --hive-script \
	s3n://elasticmapreduce/samples/hive-ads/libs/model-build.q \
	--args -d,LIBS=s3n://elasticmapreduce/samples/hive-ads/libs,\
	-d,INPUT=s3n://elasticmapreduce/samples/hive-ads/tables,\
	-d,OUTPUT=s3n://$(bucket)/hive-ads/output/

	#--arg $(bucket) \
	#--arg s3://$(bucket)/input/5000000.xml 
	#--arg s3://$(bucket)/input/1000000.xml 
	#--arg s3://$(bucket)/input/1000.xml 

	#--arg s3://$(bucket)/input/100.xml 
	#--args $(bucket),s3://$(bucket)/input/100.xml

s3:
	s3cmd put src/netflix/most_similar_pairs.q s3://david78k-ids/input/most_similar_pairs.q

upload:
	git add .
	git commit -a -m update
	git push

showmore:
	hadoop fs -cat $(bucket)/results/PageRank.n.out
	hadoop fs -cat $(basedir)/PageRank.iter1.out | head -20 | nl

