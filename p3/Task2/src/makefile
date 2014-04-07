SHELL := /bin/bash

default: compile run

compile:
	mpic++ -o reducer.o reducer.cpp
	#mpicc -std=c99 -o reducer.o reducer.cpp

run:
	#time ./reducer.o
	time mpirun ./reducer.o

