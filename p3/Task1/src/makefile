SHELL := /bin/bash

default: compile run

run:
	time ./pagerank.o facebook_combined.txt
	#time ./pagerank.o

compile:
	mpicc -fopenmp -o pagerank.o pagerank.c -lm
	#gcc -fopenmp -o pagerank.o pagerank.c -lm

