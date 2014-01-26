import java.io.*;
import java.util.*;
//import java.util.logging.Logger;
import java.util.logging.*;
import java.util.regex.*;

import org.apache.hadoop.*;
import org.apache.hadoop.mapreduce.*;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import no.uib.cipr.matrix.*;

import org.apache.commons.math3.linear.*;

class PageRank {

	double d = 0.85;
	int N = 0;
	int MAX_ITER = 8;

	//RealMatrix R0; // initially 1
	RealVector R0; // initially 1
	RealVector R;
	RealMatrix A;

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
		extract();
		rank();
	}

	/** initialize data from input file
	 *  and construct a matrix
	*/
	void extract() {
		// read data from input file
		File input = new File("data/100.xml");
		//File input = new File("data/1000.xml");
		//File input = new File("data/10000.xml");

		double[][] matdata;
		//ArrayList plist = new ArrayList(); // pagerank list for vector R
		Hashtable plist = new Hashtable(); // pagerank list for vector R
		ArrayList alist = new ArrayList(); // adjacency list for matrix A

		try {
			String filename = "PageRank.inlink.out";
			FileHandler fhandler = new FileHandler(filename);
			fhandler.setFormatter(new PlainFormatter());
			logger.addHandler(fhandler);

			Document doc = Jsoup.parse(input, "UTF-8");
	
			Elements pages = doc.select("page");
			for (Element page: pages) {
				String content = page.text();
				//System.out.println(content);
				//logger.info(content);

				String[] contents = content.split(" ");
				String title = contents[0];
				logger.info(title.replaceAll(" ", "_") + " ");
					
				//N = 0;
				int i = 0;
				char c = content.charAt(i);
				StringBuffer sb = new StringBuffer();
				ArrayList links = new ArrayList();
				//String[] links = content.split("\\[\\[");
				//logger.info(links.length + " ");
				//logger.info(links[0] + " ");
				//N += links.length;
				int len = content.length();

				while (i < len) {
					c = content.charAt(i ++);	
					if (c == '[') {
						if (i >= len) break;

						c = content.charAt(i);
						if (c == '[') {
							i ++;
							while(i < len && (c = content.charAt(i ++)) != '|') {
								if (c == ']') {
									if(i < len && (c = content.charAt(i ++)) != ']' && c != '|') {
										sb.append(c); 
									} else {
										break;
									}	
								} else {
									sb.append(c); 
									if (i < len && (c = content.charAt(i ++)) != ']' && c != '|') {
										sb.append(c); 
									} else break;
								}
							} 
							
							String link = sb.toString();
							links.add(link);
							plist.put(link, new ArrayList());
							//System.out.println("link " + N + ": " + link);
							logger.info(link.replaceAll(" ", "_") + "\t");
							N ++;
							sb = new StringBuffer();
						}
					}				
				}
				plist.put(title, links);
/*
				doc = Jsoup.parse(content);
				String forbiddenCharacters = "`~!@#$%^&*{}[\"':;,.<>?/|\\";
				String patternToMatch = "[\\\\!\"#$%&()*+,./:;<=>?@\\[\\^_{|}~]+";
				patternToMatch = "#$%&()*+,./:;<=>?@\\[\\^_{|}~";
				Pattern pattern = Pattern.compile("\\[\\[([A-Za-z0-9.]+)\\]\\]");
				//pattern = Pattern.compile("\\[\\[[\\w\\s\\|\\:\\/" + Pattern.quote(forbiddenCharacters) + ".]+\\]\\]");
				pattern = Pattern.compile("\\[\\[[\\w\\s\\|\\:\\/" + Pattern.quote(forbiddenCharacters) + Pattern.quote(patternToMatch) + ".]+\\]\\]");
				//Pattern pattern = Pattern.compile("\\[\\[(\\w.)+\\]\\]");
				//Pattern pattern = Pattern.compile("\\[\\[(.+?)\\]\\]");
				//Pattern pattern = Pattern.compile("\\[\\[(.+)\\]\\]");
				pattern = Pattern.compile("\\[\\["); // 1244
				pattern = Pattern.compile("\\[\\[[^].]+\\]\\]");
				pattern = Pattern.compile("\\[\\[[^]]+\\]\\]");
				//pattern = Pattern.compile("\\[\\[.+\\]\\]");
				//pattern = Pattern.compile("\\[\\[(^" + Pattern.quote("]") + ")+\\]\\]");
				//pattern = Pattern.compile("\\[\\[(" + Pattern.quote("^]") + ")+\\]\\]");
				Matcher matcher = pattern.matcher(content);
				//matcher.find();
				while(matcher.find()) {
					//System.out.print("Start index: " + matcher.start());
					//System.out.print(" End index: " + matcher.end() + " ");
					//System.out.println(matcher.group());
					//System.out.println(matcher.group(0));
					//logger.info(matcher.group() + "\t");
					N ++;
				}
*/				
				logger.info("\n");	
			}
	
			logger.removeHandler(fhandler);

			// write the total number of pages N
			// N=?
			filename = "PageRank.n.out";
			fhandler = new FileHandler(filename);
			fhandler.setFormatter(new PlainFormatter());
			logger.addHandler(fhandler);
			logger.info("N=" + N + "\n");
			logger.removeHandler(fhandler);

			//System.out.println(plist);
			for (Map.Entry entry: (Set<Map.Entry>)plist.entrySet()) {
				System.out.println(entry);
			}
			System.out.println(plist.size());
		} catch (IOException e) {}

		//Matrix mat = new DenseMatrix(2,2);
		//System.out.println(mat);
		//A = MatrixUtils.createRealMatrix(matdata);
		A = new Array2DRowRealMatrix(N, N);
		R0 = new ArrayRealVector(N);
	//	A = new RealMatrix(N, N);
	//	System.out.println(A);
		
		//Matrix R1 = new RealMatrix(N, 1);
		
	//	R = R0;
		
		// 1st iteration
		//R = (1 - d) + A.mult(R, R1);
	//	R = A.mult(R, R1);
		//R1 = (1 - d) + A.mult(R, R1);
	//	R1 = R;

		//DenseMatrix result = new DenseMatrix(matA.numRows(),matB.numColumns());
		//matA.mult(matB,result);
	}

	class PlainFormatter extends java.util.logging.Formatter {
		public String format(LogRecord record) {
			return record.getMessage(); 
			//return record.getMessage() + System.getProperty("line.separator");
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
