#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <iostream>
#include <fstream>
#include <string>
using namespace std;

void init();
void readFile();
void reduce();
void reduceMPI();

string infile ("100000_key-value_pairs.csv");

int main(int argc, char **argv) {
	readFile();
	init();
	reduce();

	return EXIT_SUCCESS;
}

void reduce() {

}

void reduceMPI() {
	int i, num_procs, ID, left, right, Nsteps = 100;

	readFile();

	MPI_Status status;

	MPI_Request req_recv, req_send;

//	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &ID);
	MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

	MPI_Finalize();
}

void init() {

}

void readFile() {
  string line;
  //ifstream myfile ("example.txt");
  ifstream myfile (infile.c_str());

  if (myfile.is_open())
  {
    while ( getline (myfile,line) )
    {
      cout << line << '\n';
    }
    myfile.close();
  }

  else cout << "Unable to open file";
}

void writeFile() {
	ofstream myfile;
	myfile.open ("example.txt");
	myfile << "Writing this to a file.\n";
	myfile.close();
}

