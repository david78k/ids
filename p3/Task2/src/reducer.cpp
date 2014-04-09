#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <vector>
#include <iterator>

using namespace std;

#define INFILE "100000_key-value_pairs.csv"
//string infile ("100000_key-value_pairs.csv");
#define OUTFILE "Output_Task2.txt"
#define NPAIRS 10

map<int, int> table;
vector<vector<int>> pairs; // key, value pairs
vector<string> lines;
int nprocs, myrank, blocksize; 

int main(int argc, char **argv) {
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
	MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
	cout << "nprocs = " << nprocs << ", myrank = " << myrank << endl;

	/******************** INITIALIZE: read file and insert pairs into a table *****************/
  	ifstream infile;
  	string line;

	if(argc > 1) { 
		cout << "argc = " << argc << endl;
		for(int i = 0; i < argc; i++) 
			cout << "argv[" << i << "] = " << argv[i] << endl; 
		infile.open(argv[1]);		
	} else {
  		infile.open(INFILE);
	}

  	if (!infile.is_open()) {
		cout << "Unable to open file";
		exit(1);
	}

	int min, max, range; // to calculate range of keys
	max = 0;
	char *token;
	while ( getline (infile,line) ) {
		if (line.find("key,value") != string::npos) continue;
		lines.push_back(line);	

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

		if (key < min) min = key;
		if (key > max) max = key;
	}

	infile.close();
	
	range = max - min;
	
	if(myrank == 0) {
		cout << "number of lines = " << lines.size() << endl;
		cout << "number of keys = " << table.size() << endl;
		cout << "number of pairs = " << pairs.size() << endl;
		cout << "key range = " << range << " (" << min << ", " << max << ")" << endl;
	}

//	MPI_Barrier(MPI_COMM_WORLD);
	cout << endl;
	
	/*********************** FIRST STEP: partition and local reduce on own table ***********************/
	// partition lines for each processor
	// 1-(n-1)th processors: N/n
	// nth processor: N - (n - 1)*N/n
	// nprocs; // number of processors
	int begin, end;
	int N = lines.size();
	blocksize = N/nprocs;
	int i, j;

	// partitioned table for each processor
	map<int, int> partable;

	begin = myrank * blocksize;
	end = begin + blocksize; 
	// assign the rest to the last proc
	if (myrank == nprocs - 1) {
		end = N;	
	}
	cout << "[Proc" << myrank << "] lines assigned: " << begin << "-" << end << endl;		
	cout << "[Proc" << myrank << "] lines[0]: "  << lines[0] << endl;
	cout << "[Proc" << myrank << "] pairs[begin][0]: "  << pairs[begin][0] << endl;

//	MPI_Barrier(MPI_COMM_WORLD);
	cout << endl;

	// calculate the sum of pairs
	// and split key range
	int key, value;
	vector<int> results[nprocs];

	for(i = begin; i < end; i ++) {
		key = pairs[i][0];
		partable[key] += pairs[i][1];
	}

	cout << "[Proc" << myrank << "] Paritioned table size = " << partable.size() << endl;

//	MPI_Barrier(MPI_COMM_WORLD);
	cout << endl;

	/********************** SECOND STEP: send the results of local reduction ********************/
	// send the selective key-value pairs to corresponding processors
	// and receive the messages from others
	// split key range
	int keyrange = range/nprocs;

	// map pairs to corresponding processor
	// and insert into an arraylist (vector)
	for (auto it = partable.begin(); it != partable.end(); ++it) {
		key = it->first;
		value = it->second;

		for (j = 0; j < nprocs; j ++) {
			int first = min + j * keyrange;
			int last = first + keyrange - 1;
			if (j == nprocs - 1) last = range;
			if (first <= key && key <= last)
				results[j].push_back(key);
		}	
	}

	for (i = 0; i < nprocs; i ++) {
		cout << "[Proc" << myrank << "] Map size for proc " << i << " = " << results[i].size() << endl;
	}
		
	// send pairs to corresponding processors
	for (i = 0; i < nprocs; i ++) {
		//if (i != myrank || i == myrank) {
		if (i != myrank) {
			// convert pairs into array
			int size = results[i].size();
		//	size = 10;
			int data[size][2];
			int recvsize;
			j = 0;

			for (auto it = results[i].begin(); it != results[i].end(); ++it) {
				key = *it;
				data[j][0] = key;	
				data[j][1] = partable[key];	
				j++;
			}
	
			cout << "[Proc" << myrank << "] to proc " << i << ": data[0][0] = " << data[0][0] << 
				", data[0][1] = " << data[0][1] << endl;

			// to identify the incoming message size
			MPI_Sendrecv(&size, 
				1,
				MPI_INT, i, 1,
				&recvsize,
				1,
				MPI_INT, i, 1,
				MPI_COMM_WORLD, NULL	
			);

			int recv[recvsize][2];

			MPI_Sendrecv(&data[0][0], 
				size * 2,
				MPI_INT, i, 1,
				&recv[0][0],
				recvsize * 2,
				MPI_INT, i, 1,
				MPI_COMM_WORLD, NULL	
			);
				
			cout << "[Proc" << myrank << "] from proc " << i << ": recv[0][0] = " << recv[0][0] << 
				", recv[0][1] = " << recv[0][1] << endl;
			
			// merge the received pairs into the current table
			for (j = 0; j < recvsize; j ++) 
				partable[recv[j][0]] += recv[j][1];
		}
	}
	
	/********************** FINAL STEP: second local reduction and write into file ********************/
	// write only my pairs into file
	// need synchronization by taking turn
	ofstream outfile;
	ofstream outrank;
	stringstream ss;
	ss << myrank << ".log";
	string filename; 
	filename = ss.str();

	// starting from rank 0 to increase by one for each processor
	int turn = 0; 

	// delete output file
	if(myrank == 0 && remove( OUTFILE ) != 0 ) {
		perror( "Error deleting file" );
	//	exit(1);
	}

	//MPI_Barrier(MPI_COMM_WORLD);
	int first = min + myrank * keyrange;
	int last = first + keyrange - 1;
	if (myrank == nprocs - 1) last = range;

	while(turn < nprocs) {
		MPI_Barrier(MPI_COMM_WORLD);
		if(turn == myrank) {
			cout << "[Proc" << myrank << "] turn = " << myrank << ": pratable.begin()->first = " << partable.begin()->second << endl;
			outfile.open (OUTFILE, ios::app);
		
			for (auto it = partable.begin(); it != partable.end(); ++it) {
				key = it->first;
				value = it->second;

				if (first <= key && key <= last) {
					outfile << key << '\t' << value << endl;
				}
			}
	
			outfile.close();
		}
		turn ++;
	}

	MPI_Finalize();

	return EXIT_SUCCESS;
}
