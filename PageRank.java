import no.uib.cipr.matrix.*;
import java.io.*;
import java.util.*;
//import java.util.logging.Logger;
import java.util.logging.*;
import org.apache.hadoop.*;
import org.apache.hadoop.mapreduce.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

class PageRank {

	double d = 0.85;
	int N = 0;
	int MAX_ITER = 8;

	Matrix R0; // initially 1
	Matrix R;
	Matrix A;

	Logger logger = Logger.getLogger(PageRank.class.getName());
	Mapper<String, String, String, String> mapper;

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
		File input = new File("data/100.xml");
		try {
			Document doc = Jsoup.parse(input, "UTF-8");
			String title = doc.title();
			System.out.println("title: " + title);
			title = doc.title();
			System.out.println("title: " + title);
		} catch (IOException e) {}

		Matrix mat = new DenseMatrix(2,2);
		System.out.println(mat);
		
		//DenseMatrix result = new DenseMatrix(matA.numRows(),matB.numColumns());
		//matA.mult(matB,result);
	}

	void parse() {

	}
	
	class PlainFormatter extends java.util.logging.Formatter {
		public String format(LogRecord record) {
			return record.getMessage() + System.getProperty("line.separator");
		}
	}

	void rank() {

		for (int i = 1; i <= MAX_ITER; i ++) {
			//PR(pi) = (1 - d)/N + d*(sum(PR(pj)/L(pj)));
			//R = R0 + d*A*R;
			if (i == 1 || i == MAX_ITER) {
				try {
					//System.out.println(i);	
					String filename = "PageRank.iter" + i + ".out";
					FileHandler fhandler = new FileHandler(filename);
					fhandler.setFormatter(new PlainFormatter());
					logger.addHandler(fhandler);
					logger.info(filename);
					//logger.log(Level.INFO, filename);
					logger.removeHandler(fhandler);
				} catch (IOException e) {
					e.printStackTrace();
				}
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
