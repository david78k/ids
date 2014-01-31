package PageRank;

import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.text.*;
//import java.util.regex.*;

import org.apache.hadoop.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
//import org.apache.hadoop.mapreduce.*; // new API doesn't work for wordcount example
//import org.apache.hadoop.mapreduce.lib.input.*;
//import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapred.*; // old API

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import org.apache.commons.math3.linear.*;
import org.apache.commons.io.IOUtils;

import org.apache.mahout.classifier.bayes.XmlInputFormat;
//import org.apache.mahout.text.wikipedia.XmlInputFormat;

//import edu.umd.cloud9.collection.XMLInputFormatOld;
//import edu.umd.cloud9.collection.XMLInputFormat;
//import edu.umd.cloud9.collection.*;

//import no.uib.cipr.matrix.*;

public class PageRank {
	static String inputpath = "s3://spring-2014-ds/data/enwiki-latest-pages-articles.xml";
	static String bucketname = "";
	static String basedir = "s3://" + bucketname;
	String outputdir = basedir + "/results";
	String outputpath = outputdir;
	SimpleDateFormat dateFormat = new SimpleDateFormat("MMddyyyy_HHmmss");
	
	final static double d = 0.85;
	static int N = 0; // total number of pages
	final static int MAX_ITER = 8;
	private static int pid = 0; // unique page id

	final static String INLINKOUT = "PageRank.inlink.out";
	final static String NOUT = "PageRank.n.out";
	final static String ITER1 = "PageRank.iter1.out";
	final static String ITER8 = "PageRank.iter8.out";

	static Hashtable plist = new Hashtable(); // global page list

	RealVector R0; // initially 1
	RealVector R;
	RealMatrix A;
	Hashtable index = new Hashtable(); // page index for matrix and vector

	Logger logger = Logger.getLogger(PageRank.class.getName());
	Mapper<String, String, String, String> mapper;

	PageRank() {}

	PageRank(String bucketname) {
		outputdir = basedir + "/results";
		outputpath = outputdir + "/" + dateFormat.format(new Date());
	}

	public static void main (String[] args) throws Exception {
		//System.out.println("PageRank");
		bucketname = args[0];
		basedir = "s3://" + bucketname;

		// for local test
		for (String arg: args) {
			if(arg.endsWith("xml")) {
				inputpath = arg;
				// basedir for local test
				if (!inputpath.startsWith("s3"))
					basedir = bucketname;
			} 
		}

		PageRank pr = new PageRank(bucketname);
		System.out.println("bucket name = " + bucketname + ", basedir = " + basedir 
			+ "\ninputpath = " + inputpath 
			+ ", outputdir = " + pr.outputdir
			+ ", outputpath = " + pr.outputpath); 

		pr.pagerank();
		// secondary sort by PageRank scores with resulting files
		//pr.secsort(); 

		//pr.wordcount();
		//pr.merge();
	}
	
	/** secondary sort by values (PageRank score)
  	 *  while merging files
  	 */
	void secsort() throws Exception {
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("secondary sort");

		//conf.setOutputKeyClass(Text.class);
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);
		//conf.setOutputValueClass(IntWritable.class);

		//conf.setMapperClass(SortMapper.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(SortReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

                FileInputFormat.setInputPaths(conf, new Path(outputpath));
                //FileInputFormat.setInputPaths(conf, new Path(outputdir + "/test"));
                FileOutputFormat.setOutputPath(conf, new Path(outputpath + "/" + INLINKOUT));
                //FileOutputFormat.setOutputPath(conf, new Path(outputdir + "/" + INLINKOUT));

		conf.setNumReduceTasks(1);

		JobClient.runJob(conf);
	}

	//public static class SortReducer extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, IntWritable> {
	public static class SortReducer extends MapReduceBase implements Reducer<LongWritable, Text, Text, IntWritable> {
		//public void reduce(LongWritable key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		//public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, IntWritable> output, Reporter reporter) throws IOException {
		public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			//System.out.print(key.toString() + "\t");
			//System.out.println(key.toString() + ", " + values.toString());
			Text word = new Text();
			int sum = 0;
			String title;
			while (values.hasNext()) {
				String line = values.next().toString();
				StringTokenizer tok = new StringTokenizer(line);	
				//System.out.print(line + ",\t");
				title = tok.nextToken();
				word.set(title);
				output.collect(word, new IntWritable(Integer.parseInt(tok.nextToken())));
				//int count = Integer.parseInt(tok.nextToken());
		//		sum += count;
				//sum += values.next().get();
			}
			//System.out.println();
		//	output.collect(word, new IntWritable(sum));
		}
	}

	void pagerank() throws Exception {
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("pagerank");

		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Page.class);
		//conf.setOutputValueClass(Text.class);
		//conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(PageRankMapper.class);
		conf.setReducerClass(PageRankReducer.class);
		//conf.setMapperClass(WordCountMapper.class);
		//conf.setCombinerClass(Reduce.class);
		//conf.setReducerClass(WordCountReducer.class);

		//conf.setInputFormat(TextInputFormat.class);
		conf.setInputFormat(XmlInputFormat.class);
		//conf.setInputFormat(XMLInputFormat.class);
		//conf.setInputFormat(XMLInputFormatOld.class);
		conf.setOutputFormat(TextOutputFormat.class);
		//conf.setOutputFormat(Page.class);

                FileInputFormat.setInputPaths(conf, new Path(inputpath));
                FileOutputFormat.setOutputPath(conf, new Path(outputpath));
		//FileInputFormat.setInputPaths(conf, new Path(args[0]));
		//FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}

	//public static class PageRankMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	public static class PageRankMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Page> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		/** extract Page data structure */
		//public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		//public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		public void map(LongWritable key, Text value, OutputCollector<Text, Page> output, Reporter reporter) throws IOException {
			String xmlstr = value.toString();
			Page p;
				
			Document doc = Jsoup.parse(xmlstr);
	
			Element e = doc.select("title").first();
			String title = e.text();
			e = doc.select("text").first();
			String text = doc.body().text();
			text = e.text();

			//System.out.println(title + "\t" + text);

			String content = text;
			int i = 0;
			char c = content.charAt(i);
			StringBuffer sb = new StringBuffer();
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
								} else break;
							} else {
								sb.append(c); 
								if (i < len && (c = content.charAt(i ++)) != ']' && c != '|') {
									sb.append(c); 
								} else break;
							}
						} 
							
						String link = sb.toString().replaceAll(" ", "_");
						if(!link.startsWith("#top") 
							/*
							&& !link.contains(":") 
							//!link.startsWith("#section name") && 
							&& !link.contains("#") 
							&& !link.contains("/")
							*/
						)
							output.collect(new Text(title), new Page(pid ++, link, 1, new ArrayList()));
						//System.out.println(link);
					}
				}				
			}
		}
	}

	//public static class PageRankReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	public static class PageRankReducer extends MapReduceBase implements Reducer<Text, Page, Text, Text> {
		//public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		public void reduce(Text key, Iterator<Page> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			System.out.println(key.toString());

			int sum = 0;
			Text inlinks = new Text();
			StringBuffer sb = new StringBuffer();
			while (values.hasNext()) {
				Page p = values.next();
				//System.out.println(p.toString());
				//sum += values.next().get();
				sb.append(p.title + "\t");
				//sb.append(p.toString());
			}
			inlinks.set(sb.toString());

			output.collect(key, inlinks);
		}
	}

	static class Page implements Writable{
		int id;
		String title;
		double pagerank = 1;
		ArrayList<String> links = new ArrayList<String>();

		Page() {}

		Page (int id, String title, double pagerank, ArrayList links) {
			this.id = id;
			this.title = title;
			this.pagerank = pagerank;
			this.links = links;
		}
		
		public void write(DataOutput out) throws IOException {
         		out.writeInt(id);
			//out.writeBytes(title);
			Text.writeString(out, title);
			out.writeDouble(pagerank);
			out.writeInt(links.size());
			for(String link: links) 
				Text.writeString(out, link);
				//out.writeBytes(link);
	       	}

		public void readFields(DataInput in) throws IOException {
	        	id = in.readInt();
			//title = in.readLine();
			title = Text.readString(in);
//			System.out.println(title);
			pagerank = in.readDouble();
//			System.out.println(pagerank);
			int size = in.readInt();
			links = new ArrayList();
			for(int i = 0; i < size; i ++) {
				//String link = in.readLine();
				String link = Text.readString(in);
				links.add(link);
			}
		}

		public String toString() {
			return pagerank + "\t" + links.toString();
			//return title + "\t" + pagerank + "\t" + links.toString();
			//return id + ", " + pagerank + ", " + links.toString();
		}
	}

	void wordcount() throws Exception {
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("wordcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(WordCountMapper.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(WordCountReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

                FileInputFormat.setInputPaths(conf, new Path(inputpath));
                FileOutputFormat.setOutputPath(conf, new Path(outputpath));
		//FileInputFormat.setInputPaths(conf, new Path(args[0]));
		//FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
	
	public static class WordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			//System.out.println(key.toString() + ", " + value);
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}
		}
	}

	public static class WordCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			//System.out.println(key.toString() + ", " + values);
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	/** initialize data from input file
	 *  and construct the adjacency matrix A
	*/
	void extract() {
		// read data from input file
		//File input = new File("s3://spring-2014-ds/data/enwiki-latest-pages-articles.xml");
		File input = new File(inputpath);

		double[][] matdata;
		Hashtable plist = new Hashtable(); // pagerank list for vector R
		ArrayList alist = new ArrayList(); // adjacency list for matrix A

		int ind = 0;
		int nlinks = 0;

		try {
			String filepath = outputdir + "/" + INLINKOUT;
			new File(outputdir).mkdirs();

			FileHandler fhandler = new FileHandler(filepath);
			fhandler.setFormatter(new PlainFormatter());
			logger.addHandler(fhandler);

			Document doc = Jsoup.parse(input, "UTF-8");
	
			Elements pages = doc.select("page");
			for (Element page: pages) {
				String content = page.text();

				String[] contents = content.split(" ");
				String title = contents[0].replaceAll(" ", "_");
				logger.setUseParentHandlers(false);
				logger.info(title + " ");
					
				int i = 0;
				char c = content.charAt(i);
				StringBuffer sb = new StringBuffer();
				ArrayList links = new ArrayList();
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
									} else break;
								} else {
									sb.append(c); 
									if (i < len && (c = content.charAt(i ++)) != ']' && c != '|') {
										sb.append(c); 
									} else break;
								}
							} 
							
							String link = sb.toString().replaceAll(" ", "_");
							links.add(link);
							//ArrayList linklist = (ArrayList)(plist.get(link));
							//if(linklist == null)
							if((ArrayList)plist.get(link) == null) {
								plist.put(link, new ArrayList<String>());
								index.put(link, ind ++);
							} else {
							//	System.out.println("redundant: " + link);
							} 
							N ++;
							sb = new StringBuffer();
						}
					}				
				}
				
				ArrayList linklist = (ArrayList)plist.get(title);
				if(linklist != null) {
					linklist.addAll(links);
					plist.put(title, linklist);
				} else {
					plist.put(title, links);
					index.put(title, ind ++);
				}
			}
	
			nlinks = N;
			N = plist.size();
			//System.out.println("plist.size() = " + N);

			A = new Array2DRowRealMatrix(N, N);
			R0 = new ArrayRealVector(N, 1.0/N);
			R = new ArrayRealVector(N);

			int[] colsum = new int[N];

			// output inliks and create adjacency matrix
			for (Map.Entry entry: (Set<Map.Entry>)plist.entrySet()) {
				String title = entry.getKey().toString();
				logger.info(title + "\t");
				ArrayList values = (ArrayList)entry.getValue();
								
				int row = (Integer)index.get(title);				

				// create adjacency matrix
				for (String link: (ArrayList<String>)values) {
					logger.info(link + "\t");
					int col = (Integer)index.get(link);
					A.setEntry(row, col, 1);
					colsum[col] += 1;
				}
				logger.info("\n");
			}

			// normalize for column stochastic
			for(int col = 0; col < N; col ++)
				for (int row = 0; row < N; row ++) 
					if(colsum[col] != 0)
						A.setEntry(row, col, A.getEntry(row, col)/colsum[col]);

			//System.out.println(A);
			//System.out.println(index);
			System.out.println(index.size());
			//System.out.println(index.get());

			logger.removeHandler(fhandler);
			
			// write the total number of pages N
			// N=?
			filepath = outputdir + "/" + NOUT;
			fhandler = new FileHandler(filepath);
			fhandler.setFormatter(new PlainFormatter());
			logger.addHandler(fhandler);
			//logger.info("N=" + nlinks + "\n");
			logger.info("N=" + N + "\n");
			logger.removeHandler(fhandler);

		} catch (IOException e) {}
	}

	class PlainFormatter extends java.util.logging.Formatter {
		public String format(LogRecord record) {
			return record.getMessage(); 
			//return record.getMessage() + System.getProperty("line.separator");
		}
	}

	class ScoreComparator implements Comparator<Map.Entry> {
		public int compare(Map.Entry e1, Map.Entry e2) {
			double v1 = (Double)e1.getValue(); 
			double v2 = (Double)e2.getValue();
			return v1 > v2 ? 1: v1 == v2 ? 0:-1;
		}
	}

	Iterator valueIterator(TreeMap map) {
		Set set = new TreeSet(new Comparator<Map.Entry<String, Double>>() {
			public int compare(Map.Entry<String, Double> e1, Map.Entry<String, Double> e2) {
				return e1.getValue().compareTo(e2.getValue()) > 0 ? -1 : 1;
			}
		});
		set.addAll(map.entrySet());
		return set.iterator();
	}

	void rank() {
		R = R0;
		if(R == null) System.out.println("R = null");
		if(R0 == null) System.out.println("R0 = null");
		if(A == null) System.out.println("A = null");
		//System.out.println("N = " + N);

		for (int i = 1; i <= MAX_ITER; i ++) {
			//PR(pi) = (1 - d)/N + d*(sum(PR(pj)/L(pj)));
			//R = (1 - d)/N + d*A*R;
			//  = A*R*d + (1 - d)/N;
			R = A.operate(R).mapMultiply(d).mapAdd((1.0 - d)/N);
			if (i == 1 || i == MAX_ITER) {
				try {
					double[] scores = R.toArray();
					//sort R by page rank score
					TreeMap rank = new TreeMap();	
					for (Map.Entry e: (Set<Map.Entry>)index.entrySet()) {
						String title = (String)e.getKey();
						int ind = (Integer)e.getValue();
						// show pages only with score >= 5/N 
						double score = scores[ind];
						if (score >= 5.0/N)
							rank.put(title, score);
					}

					String filename = "PageRank.iter" + i + ".out";
					String filepath = outputdir + "/" + filename;
					FileHandler fhandler = new FileHandler(filepath);
					fhandler.setFormatter(new PlainFormatter());
					logger.addHandler(fhandler);

					//logger.info(rank.toString());
					//for (Map.Entry e: (Set<Map.Entry>)rank.entrySet()) 
					Iterator iter = valueIterator(rank);
					while(iter.hasNext())  {
						Map.Entry e = (Map.Entry)iter.next();
						logger.info(e.getKey() + "\t" + e.getValue() + "\n");
					}
					//logger.log(Level.INFO, filename);
					
					logger.removeHandler(fhandler);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			//normalize R: column stochastic. e.g., column sum = 1
			// compute the total column sum
			// for each column 
			// 	for each row
			// 		divide each entry by the column sum	
		//	for(A.getRows()) {

		//	}
		}
	}

/*
	void start() throws Exception {
		//JobConf conf = new JobConf(WordCount.class);
		Configuration conf = new Configuration();
		//Job job = new Job(WordCount.class);
 		Job job = new Job(conf, "pagerank");

		job.setJarByClass(PageRank.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                //job.setOutputValueClass(IntWritable.class);

                job.setMapperClass(PageRankMapper.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(PageRankReducer.class);

                //job.setInputFormatClass(TextInputFormat.class);
                job.setInputFormatClass(KeyValueTextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);

                FileInputFormat.setInputPaths(job, new Path(inputpath));
                FileOutputFormat.setOutputPath(job, new Path(outputpath));

                //JobClient.runJob(job);
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
*/
}

