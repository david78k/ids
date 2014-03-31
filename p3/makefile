#https://s3.amazonaws.com/spring-2014-ds/movie_dataset/movie_ratings/movie_ratings.csv
movie_ratings=/spring-2014-ds/movie_dataset/movie_ratings/movie_ratings.csv
#movie_titles

bucket = david78k-ids
basedir = $(bucket)/input
outdir = $(bucket)/results

instance_type = m1.small
num_instances = 10
#num_instances = 3
#num_instances = 20 # total max quota 20

run: compile 

runemr: compile s3 emr

create:
	hive -f src/create.enron.sql
	#hive -e "load data local inpath 'data/enron.refined.tab' overwrite into table enron"
	#hive -e 'select * from enron limit 5' | nl
	#hive -e 'select count(*) from enron'

show1:
	hive -e 'select * from enron limit 1'

count:
	hive -e 'select count(*) from enron'

tar:
	cp dropbox/Report.pdf ./
	tar cvf Project3_Kang_TaeSeung.tar Report.pdf Task1 Task2
	cp Project3_Kang_TaeSeung.tar dropbox/

upload:
	git add .
	git commit -a -m update
	git push

