#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <math.h>
#include <omp.h>

#define NUM_THREADS 4

static double d = 0.85;
//static double epsilon = 0.001; // 9-11 iter, 8 iter without omp
static double epsilon = 1e-7; // K-K iter, 87 iter without omp

char *input = "facebook_combined.txt";

int **mat;
double **A;
double *R;
double *R_prev;
int N = 0;

void pagerank();
void readFiles();

struct number {
	int val;
	struct number *next;	
};

struct number *head = NULL;
struct number *curr = NULL;
struct number* create_list(int val);
struct number* add_to_list(int val, bool add_to_end);
struct number* search_in_list(int val, struct number **prev);
int delete_from_list(int val);
void print_list(void);
void init();
void compute();
void sort();

void main(int argc, char **argv) {
	// master reads files
	// calculate the number of unique nodes N
	#pragma omp master
	{
		FILE * fp;
        	char * line = NULL;
       		size_t len = 0;
       		ssize_t read;
		int lineno = 0;
		int i;

		if(argc > 1) {
	        	fp = fopen(argv[1], "r");
			for(i = 0; i < argc; i++)
                        	printf("argv[%d] = %s\n", i, argv[i]);	
		} else {
	        	fp = fopen(input, "r");
			printf("%s\n", input);
		}

       		if (fp == NULL)
        		exit(EXIT_FAILURE);

		int data[lineno][2];	

		lineno = 0;
		int size = 0;
	
		struct number *ptr = NULL;
		int row[2];
		int src, dest; // source and destination node
		char *token;
       		while ((read = getline(&line, &len, fp)) != -1) {
			token = strtok(line, " ");
			src = atoi(token);
			dest = atoi(strtok(NULL, "\0"));

			// calculate the number of unique nodes N
			ptr = search_in_list(src, NULL);
        		if(NULL == ptr)
        		{
		  		add_to_list(src, true);
				size ++;
        		}
			ptr = search_in_list(dest, NULL);
        		if(NULL == ptr)
        		{
		  		add_to_list(dest, true);
				size ++;
	        	}

	   		lineno ++;
       		}
	
		fclose(fp);
       		if (line)
			free(line);

		N = size;
		printf("total number of nodes = %d\n", N);
	}

//	print_list();

	pagerank();
}

void pagerank() {
	// initialize to a normalized identity vector
	init();	

	// rank edges circles
	// R = (1-d)/N + AR
	compute();
	
	// sort
	//sort();
	
	// write output to file
	
}

void sort() {
	printf("\nsorting ...\n");
}

void init() {
	printf("\ninitializing ...\n");

	FILE * fp;

	// iterate over the linked list 
	// to initialize the matrix A and vector R
	A = malloc(N*N*sizeof(double));
	R = malloc(N*sizeof(double));
	R_prev = malloc(N*sizeof(double));
	int i, j;
	char x[100];
	double R0 = 1.0/N;
	double colsum[N];

	fp=fopen("R.vec", "wb");
	for (i = 0; i < N; i++) {
		R_prev[i] = R[i] = R0;
		sprintf(x, " %f\n", R[i]);
		fputs(x, fp);

		A[i] = malloc(N*sizeof(double));
		for (j = 0; j < N; j++) {
			A[i][j] = 0;
		}
		colsum[i] = 0;
	}
	fclose(fp);

       	char * line = NULL;
       	size_t len = 0;
       	ssize_t read;
	int lineno = 0;

	printf("%s\n", input);

        fp = fopen(input, "r");
       	if (fp == NULL)
           exit(EXIT_FAILURE);

	int size = 0;
	
	int source, dest;
	char *token;
	
	// insert link edge info into matrix and vector
       while ((read = getline(&line, &len, fp)) != -1) {
		token = strtok(line, " ");
		i = atoi(token);
		j = atoi(strtok(NULL, "\0"));
	
		A[i][j] = 1.0;
		A[j][i] = 1.0;
		colsum[i] += A[j][i];
		colsum[j] += A[i][j];

	   	lineno ++;
       }
	
	fclose(fp);
	
	//char x[] ="nodeid\tpagerank\n";
	fp=fopen("A0.mat", "wb");
	FILE *afp = fopen("A.mat", "wb");

	// column stochastic: normalize columns
	for (i = 0; i < N; i ++) {
		sprintf(x, "%d,%f\t", i, colsum[i]);
		fputs(x, fp);
		fputs(x, afp);
		for (j = 0; j < N; j ++) {
			if (A[i][j] > 0) {
				A[i][j] /= colsum[j];
				sprintf(x, " %d", j);
				fputs(x, fp);
				sprintf(x, " %f", A[i][j]);
				fputs(x, afp);
			}
		}
		fputs("\n", fp);
		fputs("\n", afp);
	}	
	fclose(fp);
	fclose(afp);
}

// perform pagerank iterations 
void compute() {
	int i, j; // row and column index
	double sum;
	double totalsum = 0;
	double l1sum = 0; // sum of L1 norm
	double squaresum = 0;
	//N = 11;
	int iter = 0;
	double diff;

	printf("\niterating ...\n");
	// |Rn+1 - Rn| < epsilon
	// fabs(R_prev[i] - R[i]) < epsilon
	while(1) {	
		totalsum = 0;
		l1sum = 0;
		squaresum = 0;

		// R = (1 - d)/N + d*A*R
		//#pragma omp parallel for default(none) \
			private(i,j,sum) shared(N, A, R, d, T) reduction(+:totalsum)
		for (i = 0; i < N; i ++) {
			//printf("Number of threads: %d\n", omp_get_num_threads());
			sum = 0.0;
			//#pragma omp critical 
			//#pragma omp ordered 
			{
				// A*R
				for (j = 0; j < N; j ++) {
					//printf("A[i][j] = %f, R[i] = %f\n", A[i][j], R[i]);
					//#pragma omp critical 
					sum += A[i][j]*R_prev[j];
				}
				//#pragma omp atomic
				//#pragma omp critical 
				//{
				//#pragma omp barrier
				R[i] = (1 - d)/N + d*sum;
				totalsum += R[i];
			}
			//printf("%f\t", R[i]);
		}
	
		//#pragma omp barrier
		printf ("iter = %d, sum of R[i] = %f, ", iter, totalsum);

		// normalize vector R
		for (i = 0; i < N; i ++) {
			//R[i] = ((1 - d)/N + d*R[i])/totalsum;
			R[i] = R[i]/totalsum;
			l1sum += fabs(R_prev[i] - R[i]);
			squaresum += pow(R_prev[i] - R[i], 2);
			//printf("R_prev[i] = %f, R[i] = %f\n", R_prev[i], R[i]);
			R_prev[i] = R[i];
		}

		// check convergence
		diff = sqrt(squaresum); // L2 norm
//		diff = l1sum; // L1 norm

		// display info every 100 iterations
		//if(iter%100 == 0) {
		printf("diff = %f, l1sum = %f, epsilon = %f\n", diff, l1sum, epsilon);
		//}
		
		if (diff < epsilon) {
			FILE *fp;
			fp=fopen("Output_Task1.txt", "wb");
			//char x[20]="nodeid pagerank\n";
			char x[] ="nodeid\tpagerank\n";
			fputs(x, fp);

			for (i = 0; i < N; i ++) {
				sprintf(x, "%d\t%f\n", i, R[i]);
				// write the results to file
				fputs(x, fp);
			}
			fclose(fp);
		//	printf("iter = %d, diff = %f, l1sum = %f, epsilon = %f\n", iter, diff, l1sum, epsilon);
			break;
		}
		//printf("iter = %d, diff = %f, epsilon = %f\n", iter, diff, epsilon);
		iter ++;
	}
}

/*************************** Test functions *********************************/
void test() {
	static long num_steps = 100000;
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

/*************************** Linked list functions *********************************/
struct number* create_list(int val)
{
    //printf("\n creating list with headnode as [%d]\n",val);
    struct number *ptr = (struct number*)malloc(sizeof(struct number));
    if(NULL == ptr)
    {
        printf("\n Node creation failed \n");
        return NULL;
    }
    ptr->val = val;
    ptr->next = NULL;

    head = curr = ptr;
    return ptr;
}

struct number* add_to_list(int val, bool add_to_end)
{
    if(NULL == head)
    {
        return (create_list(val));
    }

	/*
    if(add_to_end)
        printf("\n Adding node to end of list with value [%d]\n",val);
    else
        printf("\n Adding node to beginning of list with value [%d]\n",val);
	*/
    struct number *ptr = (struct number*)malloc(sizeof(struct number));
    if(NULL == ptr)
    {
        printf("\n Node creation failed \n");
        return NULL;
    }
    ptr->val = val;
    ptr->next = NULL;

    if(add_to_end)
    {
        curr->next = ptr;
        curr = ptr;
    }
    else
    {
        ptr->next = head;
        head = ptr;
    }
    return ptr;
}

struct number* search_in_list(int val, struct number **prev)
{
    struct number *ptr = head;
    struct number *tmp = NULL;
    bool found = false;

    //printf("\n Searching the list for value [%d] \n",val);

    while(ptr != NULL)
    {
        if(ptr->val == val)
        {
            found = true;
            break;
        }
        else
        {
            tmp = ptr;
            ptr = ptr->next;
        }
    }

    if(true == found)
    {
        if(prev)
            *prev = tmp;
        return ptr;
    }
    else
    {
        return NULL;
    }
}

int delete_from_list(int val)
{
    struct number *prev = NULL;
    struct number *del = NULL;

    printf("\n Deleting value [%d] from list\n",val);

    del = search_in_list(val,&prev);
    if(del == NULL)
    {
        return -1;
    }
    else
    {
        if(prev != NULL)
            prev->next = del->next;

        if(del == curr)
        {
            curr = prev;
        }
        else if(del == head)
        {
            head = del->next;
        }
    }

    free(del);
    del = NULL;

    return 0;
}

void print_list(void)
{
    struct number *ptr = head;

    printf("\n -------Printing list Start------- \n");
    while(ptr != NULL)
    {
        //printf("\n [%d] \n",ptr->val);
        printf("[%d] \n",ptr->val);
        ptr = ptr->next;
    }
    printf("\n -------Printing list End------- \n");

    return;
}

