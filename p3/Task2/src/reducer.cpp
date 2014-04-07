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

#define INFILE "100000_key-value_pairs.csv"
//string infile ("100000_key-value_pairs.csv");
#define OUTFILE "Output_Task2.txt"

unordered_map<int, int> table;
string lines[];

void init();
void init(int argc, char **argv);
void readFile();
void writeFile();
void reduce();
void single();
void multiple();
void multiple(int argc, char **argv);
void assign();

int main(int argc, char **argv) {
	// for single processor
	//single();
	
	// MPI with multiprocessors
	multiple(argc, argv);

	return EXIT_SUCCESS;
}

void single() {
	// for single processor
	readFile();
	writeFile();
}

void multiple(int argc, char **argv) {
	init(argc, argv);
	assign();
	//reduce();
	//writeFile();
}

void reduce() {
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

// assign lines to processors
void assign() {
	// partition lines
	// 1-(n-1)th processors: N/n
	// nth processor: N - (n - 1)*N/n
	for(line: lines) {
		
	}
}

// multi-processors
// read pairs from the file and calculate the number of pairs
// assign lines to processors
void init(int argc, char **argv) {
  ifstream myfile (INFILE);
  string line;

  if (myfile.is_open())
  {
	int i, num_procs, ID, left, right, Nsteps = 100;
	char *token;

	MPI_Status status;
	MPI_Request req_recv, req_send;
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &ID);
	MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

    while ( getline (myfile,line) )
    {
	lines.add(line);	

	char *cstr = new char[line.length() + 1];
	strcpy(cstr, line.c_str());
	token = strtok(cstr, ",");
        int key = atoi(token);
        int value = atoi(strtok(NULL, "\0"));
	delete [] cstr;

//	cout << key << ", " << value << endl;
	table[key] += value;
	
    }
    myfile.close();

	MPI_Finalize();

	cout << table.size() << endl;
  }

  else cout << "Unable to open file";
}

// single processor
// read pairs from the file and calculate the number of pairs
void readFile() {
  ifstream myfile (INFILE);
  //ifstream myfile (infile.c_str());
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

//	cout << key << ", " << value << endl;
	table[key] += value;
	
	/*
	vector<int> tokens;
	 istringstream iss(line);
    copy(istream_iterator<int>(iss),
             istream_iterator<int>(),
	back_inserter<vector<int> >(tokens));	
	for (vector<int>::iterator it = tokens.begin(); it != tokens.end(); ++it)
		cout << ' ' << *it;
	*/
    }
    myfile.close();
	
	cout << table.size() << endl;
  }

  else cout << "Unable to open file";
}

void writeFile() {
	ofstream myfile;
	myfile.open (OUTFILE);
	//myfile << "Writing this to a file.\n";
		
	for (auto it = table.begin(); it != table.end(); ++it) 
		myfile << it->first << '\t' << it->second << endl;
	//for (int i = 0; i < table.size(); i ++) 
		//myfile << i << ' ' << table[i] << endl;

	myfile.close();
}

