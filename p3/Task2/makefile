test:
	mpic++ -o test.o test.cpp
	#g++ -fopenmp -o test.o test.cpp
	mpirun -np 16 ./test.o
	#mpirun ./test.o

reduction:
	gcc -fopenmp -o pi-private-reduction.o pi-private-reduction.c

worksharing:
	gcc -fopenmp -o pi-worksharing.o pi-worksharing.c

parallel:
	gcc -fopenmp -o pi-parallel.o pi-parallel.c

critical:
	gcc -fopenmp -o pi-critical.o pi-critical.c
