#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <sstream>
#include <vector>
#include <iterator>

using namespace std;

unordered_map<int, int> table;

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
	// partitioned table for each processor
	unordered_map<int, int> partable;

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

// read pairs from the file and calculate the number of pairs
void readFile() {
  //ifstream myfile ("example.txt");
  ifstream myfile (infile.c_str());
  string line;

  if (myfile.is_open())
  {
	char *token;
    while ( getline (myfile,line) )
    {
	char *cstr = new char[line.length() + 1];
	strcpy(cstr, line.c_str());
	token = strtok(cstr, ",");
        int key = atoi(token);
        int value = atoi(strtok(NULL, "\0"));
	delete [] cstr;

	cout << key << ", " << value << '\n';
     // cout << line << '\n';
	/*
	vector<int> tokens;
	 istringstream iss(line);
    copy(istream_iterator<int>(iss),
             istream_iterator<int>(),
	back_inserter<vector<int> >(tokens));	
	for (vector<int>::iterator it = tokens.begin(); it != tokens.end(); ++it)
		cout << ' ' << *it;
	*/
	//cout << '\n';
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

