default:
	gcc -fopenmp -o pagerank.o pagerank.c

test:
	g++ -fopenmp -o test.o test.cpp

reduction:
	gcc -fopenmp -o pi-private-reduction.o pi-private-reduction.c

worksharing:
	gcc -fopenmp -o pi-worksharing.o pi-worksharing.c

parallel:
	gcc -fopenmp -o pi-parallel.o pi-parallel.c

critical:
	gcc -fopenmp -o pi-critical.o pi-critical.c

tar:
	cp dropbox/Report.pdf ./
	tar cvf Project3_Kang_TaeSeung.tar Report.pdf Task1 Task2
	cp Project3_Kang_TaeSeung.tar dropbox/

upload:
	git add .
	git commit -a -m update
	git push

