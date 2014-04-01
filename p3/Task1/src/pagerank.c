#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

#define NUM_THREADS 4

static long num_steps = 100000;
static double damp = 0.85;

//char *input = "data/facebook";
char *input = "data/facebook_combined.txt";

int **mat;
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

void main() {
	pagerank();
}

void pagerank() {
	// master reads files
	// calculate the number of nodes N
	#pragma omp master
	{
		readFiles();
	}

	print_list();

	// initialize to a normalized identity vector
	
	// R = (1-d)/N + AR
	
	// rank edges circles
}


bool isunique() {
	struct number tmp;
		
}

// read file while counting the number of lines
// and the number of nodes
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

	// count the number of lines
	//while (EOF != (scanf("%*[^\n]"), scanf("%*c"))) 
    	//	++lineno;
	//int ch;
	//while (EOF != (ch=getchar()))
	//    if (ch=='\n')
       // 	++lineno;
/*       while ((read = getline(&line, &len, fp)) != -1) 
		lineno ++;
	fclose(fp);
*/
	int data[lineno][2];	

	lineno = 0;
	int size = 0;
	
	struct number *ptr = NULL;
	int row[2];
	char *token;
       while ((read = getline(&line, &len, fp)) != -1) {
		token = strtok(line, " ");
		row[0] = atoi(token);
		row[1] = atoi(strtok(NULL, "\0"));

		ptr = search_in_list(row[0], NULL);
        	if(NULL == ptr)
        	{
        		//printf("\n Search [val = %d] failed, no such element found\n",row[0]);
	  		add_to_list(row[0], true);
			size ++;
        	}
		ptr = search_in_list(row[1], NULL);
        	if(NULL == ptr)
        	{
        		//printf("\n Search [val = %d] failed, no such element found\n",row[1]);
	  		add_to_list(row[1], true);
			size ++;
        	}

		if (lineno == 1) {
           		printf("[%d] Retrieved line of length %zu : ", lineno, read);
           		printf("%s\n", line);
	   		printf("row data = %d %d\n", row[0], row[1]);
	   	}
	   	lineno ++;
       }
	
	fclose(fp);
       if (line)
           free(line);

	printf("total number of nodes = %d\n", size);
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

struct number* create_list(int val)
{
    printf("\n creating list with headnode as [%d]\n",val);
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
