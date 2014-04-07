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
//int pairs[]; // key, value pairs
//vector<int[2]> pairs; // key, value pairs
vector<vector<int>> pairs; // key, value pairs
vector<string> lines;
int nprocs, myrank, blocksize; 

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
	MPI_Status status;
	MPI_Request req_recv, req_send;
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
	MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
	cout << "nprocs = " << nprocs << ", myrank = " << myrank << endl;

	multiple(argc, argv);

	MPI_Finalize();

	return EXIT_SUCCESS;
}

void single() {
	// for single processor
	readFile();
	writeFile();
}

void multiple(int argc, char **argv) {
	// only master runs this
	if(myrank == 0) {
		init(argc, argv);
		assign();
	}
	
	//cout << "line size = " << lines.size() << endl;
	//cout << "Block size = " << blocksize << endl;
	reduce();
	//writeFile();
}

void reduce() {
	// partitioned table for each processor
	unordered_map<int, int> partable;

	//string line;
	//char line[100];
	//char data[100];
	blocksize = 100001/nprocs;
	//blocksize = 100000/nprocs;
	int data[blocksize][2];

	//cout << "line size = " << lines.size() << endl;
	//cout << "Block size = " << blocksize << endl;
	//cout << "nprocs = " << nprocs << endl;
	//MPI_Recv(data, 100, MPI_CHAR, 0, 1, MPI_COMM_WORLD, NULL);
	MPI_Recv(&data[0][0], blocksize*2, MPI_INT, 0, 1, MPI_COMM_WORLD, NULL);
	
	cout << "Processor " << myrank << " received " << data << endl;
	//cout << "Processor " << myrank << " received " << line << endl;
/*
	MPI_Status status;

	MPI_Request req_recv, req_send;

//	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
	MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

	MPI_Finalize();
*/
}

// assign lines to processors
void assign() {
	// partition and send lines processors
	// 1-(n-1)th processors: N/n
	// nth processor: N - (n - 1)*N/n
	// nproc; // number of processors
	int begin, end;
	int N = lines.size();
	blocksize = N/nprocs;

	for(int i = 0; i < nprocs; i++) {
		begin = i * blocksize;
		end = begin + blocksize - 1; 
		// send the rest to the last proc
		if (i == nprocs - 1) {
			end = N - 1;	
		}
		cout << "Processor " << i << ": " << begin << "-" << end << endl;		
		// int MPI_Send(void *buf, int count, MPI_Datatype datatype, int dest,
		//     int tag, MPI_Comm comm)
		//cout << "MPI_send " << lines[i] << endl;
		char *cstr = new char[lines[i].length() + 1];
		strcpy(cstr, lines[i].c_str());
		cout << "MPI_send " << cstr << endl;

		//MPI_Send(&pairs[begin][0], blocksize*2, MPI_INT, i, 1, MPI_COMM_WORLD);
		//MPI_Send(cstr, strlen(cstr), MPI_CHAR, i, 1, MPI_COMM_WORLD);
		MPI_Send(&lines[i], lines[i].size(), MPI_CHAR, i, 1, MPI_COMM_WORLD);
	//	cout << "MPI message " << cstr << endl;
	//	cout << cstr;
	//	cout << " sent to processor " << i << endl;
		//cout << "Message " << cstr << " sent to processor " << i << endl;
		//cout << "Message " << lines[i] << " sent to processor " << i << endl;
		delete [] cstr;
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
	int i, Nsteps = 100;
	char *token;

	while ( getline (myfile,line) )
    	{
		lines.push_back(line);	

//	for (auto it = lines.begin(); it != lines.end(); ++it) {
		//myfile << it->first << '\t' << it->second << endl;
	//	line = *it;
		char *cstr = new char[line.length() + 1];
		strcpy(cstr, line.c_str());
		token = strtok(cstr, ",");
        	int key = atoi(token);
        	int value = atoi(strtok(NULL, "\0"));
		delete [] cstr;

	//	cout << key << ", " << value << endl;
		table[key] += value;

		vector<int> pair;
		pair.push_back(key);
		pair.push_back(value);
		pairs.push_back(pair);
	}

	cout << "number of lines = " << lines.size() << endl;
	cout << "number of keys = " << table.size() << endl;
	myfile.close();
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
	
	cout << "number of keys = " << table.size() << endl;
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

