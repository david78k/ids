SHELL := /bin/bash

export OMP_NUM_THREADS=4

default: compile run validate

run:
	#time ./pagerank.o facebook_combined.txt
	time ./pagerank.o

compile:
	mpicc -fopenmp -o pagerank.o pagerank.c -lm
	#gcc -fopenmp -o pagerank.o pagerank.c -lm

validate:
	cat answer.txt
	head out
