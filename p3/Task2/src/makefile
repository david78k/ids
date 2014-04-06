SHELL := /bin/bash

default: compile run

compile:
	mpicc -fopenmp -o pagerank.o pagerank.c -lm
	#gcc -fopenmp -o pagerank.o pagerank.c -lm

run:
	time ./pagerank.o


