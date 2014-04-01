#include <stdio.h>
#include <omp.h>

void main() {
	static long num_steps = 100000;
	double x, pi = 0, sum = 0;
	double delta = 1.0/num_steps;
	int i, ID, num_threads;

	#pragma omp parallel for reduction(+:sum) private(x)
	for (i = 1; i <= num_steps; i++){
		x = (i - 0.5)*delta;
		sum += 4/(1 + x*x);	
	}	
	pi = sum * delta;
	printf("pi = %lf\n", pi);
}
