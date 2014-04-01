#include <stdio.h>

#define NUM_THREADS 4

static long num_steps = 100000;

void main() {
	double pi, sum[NUM_THREADS] = {0};
	int i;
	double delta = 1.0/num_steps;

	omp_set_num_threads(NUM_THREADS);

	#pragma omp parallel
	{
		int ID = omp_get_thread_num();
		int nthreads = omp_get_num_threads();
		printf("ID = %d, nthreads = %d\n", ID, nthreads);

		double x;

		#pragma omp for
		for (i = 0; i < num_steps; i++){
			x = (i + 0.5)*delta;
			sum[ID] += 4/(1 + x*x);	
		}	
	}

	for(i = 0, pi = 0; i < NUM_THREADS; i ++)
		pi += delta * sum[i];
	printf("pi = %lf\n", pi);
}
