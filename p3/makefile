
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
	tar cvf Project3_Kang_TaeSeung.tar Report.pdf Task1/src/pagerank.c Task1/src/makefile Task1/src/facebook_combined.txt Task2/src/reducer.cpp Task2/src/makefile Task2/src/100000_key-value_pairs.csv
	cp Project3_Kang_TaeSeung.tar dropbox/

upload:
	git add .
	git commit -a -m update
	git push

