#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

int main(int argc, char **argv) {
	int i, num_procs, ID, left, right, Nsteps = 100;

	MPI_Status status;

	MPI_Request req_recv, req_send;

	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &ID);
	MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

	MPI_Finalize();

	return EXIT_SUCCESS;
}
