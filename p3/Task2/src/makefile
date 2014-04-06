SHELL := /bin/bash

default: compile run

compile:
	mpic++ -o reducer.o reducer.cpp
	#mpicc -std=c99 -o reducer.o reducer.cpp
	#mpicc -fopenmp -o pagerank.o pagerank.c -lm

run:
	#time ./reducer.o
	time mpirun ./reducer.o

