import java.io.*;
import java.util.*;
//import java.util.logging.Logger;
import java.util.logging.*;
import java.util.regex.*;

import no.uib.cipr.matrix.*;

import org.apache.hadoop.*;
import org.apache.hadoop.mapreduce.*;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

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
		try {
			String filename = "PageRank.inlink.out";
			FileHandler fhandler = new FileHandler(filename);
			fhandler.setFormatter(new PlainFormatter());
			logger.addHandler(fhandler);

			Document doc = Jsoup.parse(input, "UTF-8");
	
			int nlinks = 0;

			Elements pages = doc.select("page");
			for (Element page: pages) {
				String content = page.text();
				//System.out.println(content);
				//logger.info(content);

				String[] contents = content.split(" ");
				String title = contents[0];
				//System.out.println("title: " + title);
				logger.info(title + " ");
					
				//nlinks = 0;
				int i = 0;
				char c = content.charAt(i);
				StringBuffer sb = new StringBuffer();
				String[] links = content.split("\\[\\[");
				//logger.info(links.length + " ");
				//logger.info(links[0] + " ");
				//nlinks += links.length;
				int len = content.length();

				while (i < len) {
					c = content.charAt(i ++);	
					if (c == '[') {
						//System.out.println(c);
						if (i >= len) break;

						c = content.charAt(i);
						if (c == '[') {
							i ++;
							while(i < len && (c = content.charAt(i ++)) != '|') {
								if (c == ']') {

								} else {
									sb.append(c); 
									if (i < len && (c = content.charAt(i ++)) != ']' && c != '|') {
										sb.append(c); 
									} else break;
								}
							} 
							
							String link = sb.toString();
							System.out.println("link " + nlinks + ": " + link);
							nlinks ++;
							sb = new StringBuffer();
						}
					}				
				}

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
					nlinks ++;
				}
*/				
				//Elements links = doc.select("[[");
				logger.info("\n");	
			}
	
			/*	
			Elements titles = doc.select("title");
			for (Element elem: titles) {
				// write the results to PageRank.inlink.out
				System.out.println(elem.text() + " ");
				logger.info(elem.text());
				//System.out.println("title: " + elem.text() + " " + elem);
				//for (Object link: links) 
				//	System.out.println(elem.text() + " ");
				nlinks ++;
			}
			*/

			logger.removeHandler(fhandler);

			// write the total number of pages N
			// N=?
			filename = "PageRank.n.out";
			fhandler = new FileHandler(filename);
			fhandler.setFormatter(new PlainFormatter());
			logger.addHandler(fhandler);
			logger.info("N=" + nlinks + "\n");
			logger.removeHandler(fhandler);
		} catch (IOException e) {}

		Matrix mat = new DenseMatrix(2,2);
		System.out.println(mat);
		
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
