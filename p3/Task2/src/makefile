SHELL := /bin/bash

default: compile run

compile:
	mpic++ -o reducer.o reducer.cpp -std=c++0x
	#mpicc -std=c99 -o reducer.o reducer.cpp

run:
	time mpirun -np 3 ./reducer.o
	#time mpirun -np 4 ./reducer.o
	#time mpirun -np `nproc` ./reducer.o
	#time mpirun ./reducer.o
	#time ./reducer.o
validate:
	diff Output_Task2-single_processor.txt out
