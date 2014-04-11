SHELL := /bin/bash

default: compile run validate

run:
	#time mpirun -np 4 ./reducer.o 100000_key-value_pairs.csv
	#time mpirun -np 3 ./reducer.o
	#time mpirun -np 4 ./reducer.o
	#time mpirun -np 8 ./reducer.o
	#time mpirun -np 9 ./reducer.o
	#time mpirun -np 11 ./reducer.o
	time mpirun -np 16 ./reducer.o
	#time mpirun -np `nproc` ./reducer.o
	#time mpirun ./reducer.o
	#time ./reducer.o

compile:
	mpic++ -o reducer.o reducer.cpp -std=c++0x
	#mpicc -std=c99 -o reducer.o reducer.cpp

validate:
	wc -l out
	diff Output_Task2-single_processor.txt out
