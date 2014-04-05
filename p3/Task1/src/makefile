SHELL := /bin/bash

default: compile run

compile:
	gcc -fopenmp -o pagerank.o pagerank.c -lm

run:
	time ./pagerank.o


