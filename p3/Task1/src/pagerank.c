#include <stdio.h>
#include <stdlib.h>

#define NUM_THREADS 4

static long num_steps = 100000;
static double damp = 0.85;
int N = 0;

//char *input = "data/facebook";
char *input = "data/facebook_combined.txt";

void pagerank();

void main() {
	pagerank();
}

void readFiles() {
	FILE * fp;
       char * line = NULL;
       size_t len = 0;
       ssize_t read;
	int lineno = 0;

	printf("%s\n", input);

       fp = fopen(input, "r");
       if (fp == NULL)
           exit(EXIT_FAILURE);

       while ((read = getline(&line, &len, fp)) != -1) {
           printf("[%d] Retrieved line of length %zu : ", lineno, read);
           printf("%s", line);
	   lineno ++;
       }

       if (line)
           free(line);
}

void pagerank() {
	// master reads files
	#pragma omp master
	{
		readFiles();
	}

	// calculate the number of nodes N
	
	// initialize to a normalized identity vector
	
	// R = (1-d)/N + AR
	
	// rank edges circles
}

void test() {
	int i; // global variable i
	double pi, sum[NUM_THREADS] = {0};
	double delta = 1.0/num_steps;

	//omp_set_num_threads(NUM_THREADS);

	#pragma omp parallel
	{
		int i; // local variable i
		int ID = omp_get_thread_num();
		int nthreads = omp_get_num_threads();
		printf("ID = %d, nthreads = %d\n", ID, nthreads);

		double x;
		for (i = ID; i < num_steps; i = i + nthreads){
			x = (i + 0.5)*delta;
			sum[ID] += 4/(1 + x*x);	
		}	
	}

	for(i = 0, pi = 0; i < NUM_THREADS; i ++)
		pi += delta * sum[i];
	printf("pi = %lf\n", pi);

	int th_id, nthreads;
	#pragma omp parallel private(th_id) shared(nthreads)
	{
		th_id = omp_get_thread_num();
		#pragma omp critical
		{
      			printf("Hello World from thread\n");
    		}
		#pragma omp barrier

		#pragma omp master
		{
      			nthreads = omp_get_num_threads();
	      		printf("There are %d threads\n", nthreads);
   		}	
	}
}

