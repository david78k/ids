import no.uib.cipr.matrix.*;
import java.util.*;

class PageRank {

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

		//DenseMatrix result = new DenseMatrix(matA.numRows(),matB.numColumns());
		//matA.mult(matB,result);
	}

	void parse() {

	}
	
	void rank() {

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
