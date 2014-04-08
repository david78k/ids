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
#define NPAIRS 10

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

	//multiple(argc, argv);
	
  	ifstream myfile (INFILE);
  	string line;

  	if (myfile.is_open())
  	{
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
			//cout << "pair: "  << pair[0] << ", " << pair[1] << endl;
			pairs.push_back(pair);
		}

		cout << "number of lines = " << lines.size() << endl;
		cout << "number of keys = " << table.size() << endl;
		cout << "number of pairs = " << pairs.size() << endl;
		cout << "lines[0]: "  << lines[0] << endl;
		//cout << "pairs[1]: "  << pairs[1] << endl;
		cout << "pairs[1][0]: "  << pairs[1][0] << endl;

		myfile.close();
	
		cout << "number of keys = " << table.size() << endl;
	} else cout << "Unable to open file";
	
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
	cout << "\nreduce" << endl;
	reduce();
	//writeFile();
}

void reduce() {
	// partitioned table for each processor
	unordered_map<int, int> partable;

	//char line[100];
//	blocksize = 100001/nprocs;
	//char data[100];
	double b[128][32];
	int i, j, blocksize, nlines;

	cout << "line size = " << lines.size() << endl;
	cout << "nprocs = " << nprocs << endl;
	//MPI_Recv(data, 100, MPI_CHAR, 0, 1, MPI_COMM_WORLD, NULL);
	MPI_Recv(&nlines, 1, MPI_INT, 0, 1, MPI_COMM_WORLD, NULL);
	cout << "num of lines = " << nlines << endl;

	int data[nlines][2];

	//MPI_Recv(&data[0][0], NPAIRS*2, MPI_INT, 0, 1, MPI_COMM_WORLD, NULL);
	MPI_Recv(&data[0][0], nlines*2, MPI_INT, 0, 1, MPI_COMM_WORLD, NULL);
	MPI_Recv(&data[0][0], nlines*2, MPI_INT, 0, 1, MPI_COMM_WORLD, NULL);
	//MPI_Recv(&b[0][0], 128*32, MPI_DOUBLE, 0, 1, MPI_COMM_WORLD, NULL);
	
	//cout << "Processor " << myrank << " received " << b << endl;
	cout << "Processor " << myrank << " received " << data << endl;
	//cout << "Processor " << myrank << " received " << line << endl;
	
	//for(int i = 0; i < nlines; i ++) {
	for(int i = 0; i < 10; i ++) {
		cout << data[i][0] << " " << data[i][1] << endl;
		//cout << b[i][0] << " " << b[i][1] << endl;
	}
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
	double a[128][32];
	int i, j;

	//MPI_Bcast(&blocksize, 1, MPI_INT, 0, MPI_COMM_WORLD);
	/* Generate the part of the matrix held by the current process
        * so that each matrix element is unique   */
 	for (i = 0; i < 128; i++)
        	for (j = 0; j < 32; j++)
        	    a[i][j] = 1000 * i + j;
        	    //a[i][j] = 1000 * i + j + 32 * rank;

	int data[N][2];
	i = 0;
	for (auto it = pairs.begin(); it != pairs.end(); ++it) {
		vector<int> v = *it;
		data[i][0] = v[0];
		data[i][1] = v[1];
		i ++;
	}
		
	cout << "Block size = " << blocksize << endl;
	cout << "number of pairs = " << pairs.size() << endl;
	cout << "assign data[1][0] = " << data[1][0] << endl;
	cout << "assign data[1][1] = " << data[1][1] << endl;
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
		cout << "pairs[begin + 1][0]: "  << pairs[begin + 1][0] << endl;

		int nlines = end - begin + 1;
		//nlines = 20; // max lines: 160375 (32760 integers)
		nlines = 16375; // max lines: 16376 
		MPI_Send(&nlines, 1, MPI_INT, i, 1, MPI_COMM_WORLD);
		//MPI_Send(&a[0][0], 128*32, MPI_DOUBLE, i, 1, MPI_COMM_WORLD);
		MPI_Send(&data[begin][0], nlines*2, MPI_INT, i, 1, MPI_COMM_WORLD);
		MPI_Send(&data[begin + nlines][0], nlines*2, MPI_INT, i, 1, MPI_COMM_WORLD);
		//MPI_Send(&data[begin + 1][0], NPAIRS*2, MPI_INT, i, 1, MPI_COMM_WORLD);
		//MPI_Send(&pairs[begin + 1][0], NPAIRS*2, MPI_INT, i, 1, MPI_COMM_WORLD);
		//MPI_Send(&pairs[begin][0], nlines*2, MPI_INT, i, 1, MPI_COMM_WORLD);
		//MPI_Send(cstr, strlen(cstr), MPI_CHAR, i, 1, MPI_COMM_WORLD);
		//MPI_Send(&lines[i], lines[i].size(), MPI_CHAR, i, 1, MPI_COMM_WORLD);
	//	cout << "MPI message " << cstr << endl;
	//	cout << cstr;
		cout << "Message sent to processor " << i << endl;
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
		//cout << "pair: "  << pair[0] << ", " << pair[1] << endl;
		pairs.push_back(pair);
	}

	cout << "number of lines = " << lines.size() << endl;
	cout << "number of keys = " << table.size() << endl;
	cout << "number of pairs = " << pairs.size() << endl;
	cout << "lines[0]: "  << lines[0] << endl;
	//cout << "pairs[1]: "  << pairs[1] << endl;
	cout << "pairs[1][0]: "  << pairs[1][0] << endl;
	cout << "pairs[1][1]: "  << pairs[1][1] << endl;
	cout << "pairs[last][0]: "  << pairs[pairs.size()-1][0] << endl;
	cout << "pairs[last][1]: "  << pairs[pairs.size()-1][1] << endl;
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

