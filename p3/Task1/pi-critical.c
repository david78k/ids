#include <stdio.h>
#include <omp.h>

void main() {
	static long num_steps = 100000;
	double x, pi = 0, sum = 0;
	double delta = 1.0/num_steps;
	int i, ID, num_threads;

	//#pragma omp parallel for reduction(+:sum) private(x)
	#pragma omp parallel private(x, sum)
	{
		int ID = omp_get_thread_num();
		num_threads = omp_get_num_threads();
		printf("ID = %d, num_threads = %d\n", ID, num_threads);
		for (i = ID, sum = 0; i < num_steps; i += num_threads){
			x = (i + 0.5)*delta;
			sum += 4/(1 + x*x);	
		}	
		#pragma omp critical
		pi += sum;
	}

	pi = pi * delta;
	printf("pi = %lf\n", pi);
}
