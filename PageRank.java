import no.uib.cipr.matrix.*;
import java.util.*;
import java.util.logging.Logger;
//import org.apache.log4j.Logger;
import org.apache.hadoop.*;

class PageRank {

	double d = 0.85;
	int N = 0;
	int MAX_ITER = 8;

	Matrix R0; // initially 1
	Matrix R;
	Matrix A;

	Logger logger = Logger.getLogger(PageRank.class.getName());

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

		for (int i = 1; i <= MAX_ITER; i ++) {
			//PR(pi) = (1 - d)/N + d*(sum(PR(pj)/L(pj)));
			//R = R0 + d*A*R;
			if (i == 1 || i == MAX_ITER) {
				//System.out.println(i);	
				logger.info("PageRank.iter" + i + ".out");
			}
			//normalize R
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
