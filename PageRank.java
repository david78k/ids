import no.uib.cipr.matrix.*;
import java.util.*;

class PageRank {

	double d = 0.85;
	int N = 0;
	int MAX_ITER = 8;

	Matrix R0;
	Matrix R;
	Matrix A;

	// Matrix mat
	//
	PageRank() {

	}


	public static void main (String[] args) {
		System.out.println("PageRank");
		PageRank pr = new PageRank();
		pr.start();
	}
	
	void start() {
		init();
		parse();
		rank();
	}

	/** initialize data from input file
	 *  and construct a matrix
	*/
	void init() {
		// read data from input file
		//
		Matrix mat = new DenseMatrix(2,2);
		System.out.println(mat);
		
		//DenseMatrix result = new DenseMatrix(matA.numRows(),matB.numColumns());
		//matA.mult(matB,result);
	}

	void parse() {

	}
	
	void rank() {

		for (int i = 0; i < MAX_ITER; i ++) {
			//PR(pi) = (1 - d)/N + d*(sum(PR(pj)/L(pj)));
			//R = R0 + d*A*R;
		}
	}

	void mapreduce() {
		map();
		reduce();
	}

	void map() {

	}

	void reduce() {

	}
}
