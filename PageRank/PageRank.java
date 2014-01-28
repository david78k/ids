package PageRank;

import java.io.*;
import java.util.*;
import java.util.logging.*;
//import java.util.regex.*;

import org.apache.hadoop.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
//import org.apache.hadoop.mapreduce.*; // new API
//import org.apache.hadoop.mapreduce.lib.input.*;
//import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapred.*; // old API

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

//import no.uib.cipr.matrix.*;

import org.apache.commons.math3.linear.*;

public class PageRank {
	//String inputpath = "s3://spring-2014-ds/data/enwiki-latest-pages-articles.xml";
	String inputpath = "data/100.xml";

	String bucketname = "";
	String basedir = ".";
	String resultdir = basedir + "/results";
	String outputpath = resultdir;

	double d = 0.85;
	int N = 0;
	int MAX_ITER = 8;

	RealVector R0; // initially 1
	RealVector R;
	//SparseRealMatrix A;
	RealMatrix A;
	Hashtable index = new Hashtable(); // page index for matrix and vector

	Logger logger = Logger.getLogger(PageRank.class.getName());
	Mapper<String, String, String, String> mapper;

	PageRank() {}

	PageRank(String bucketname) {
		this.bucketname = bucketname;			
		basedir = bucketname;
		resultdir = basedir + "/results";
		outputpath = resultdir;
	}

	public static void main (String[] args) throws Exception {
		//System.out.println("PageRank");
		String bucketname = args[0];
		System.out.println(bucketname);

		//PageRank pr = new PageRank();
		PageRank pr = new PageRank(bucketname);
		//pr.start();
		pr.wordcount();
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
	
/*
	void wordcountnew() throws Exception {
		//JobConf conf = new JobConf(WordCount.class);
		Configuration conf = new Configuration();
		//Job job = new Job(WordCount.class);
		Job job = new Job(conf, "wordcount");
	
		job.setJarByClass(PageRank.class);

                job.setMapperClass(WordCountMapper.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(WordCountReducer.class);

                job.setOutputKeyClass(Text.class);
                //job.setOutputValueClass(Text.class);
                job.setOutputValueClass(IntWritable.class);

                //job.setMapOutputKeyClass(Text.class);
                //job.setMapOutputValueClass(IntWritable.class);

                //job.setInputFormatClass(KeyValueTextInputFormat.class);
                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                //job.setOutputFormatClass(NullOutputFormat.class); // error
                //job.setOutputFormatClass(KeyValueTextOutputFormat.class); // error
                //job.setOutputFormatClass(FileOutputFormat.class); // runtime InstantiationException

                FileInputFormat.setInputPaths(job, new Path(inputpath));
                FileOutputFormat.setOutputPath(job, new Path(outputpath));

		//job.submit();
                //JobClient.runJob(job);
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
*/
	public static class WordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
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
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	void start() throws Exception {
		//JobConf conf = new JobConf(WordCount.class);
		Configuration conf = new Configuration();
		//Job job = new Job(WordCount.class);
/*
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
*/
	}

	class Page {
		int id;
		String title;
		double pagerank = 1;
	}

/*
	//static class WordCounterMapper extends Mapper<Text, LongWritable, Text, IntWritable> {
	//static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	static class WordCountNewMapper extends Mapper<Object, Text, Text, IntWritable> {
	//static class WordCounterMapper extends Mapper<Text, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		Text word = new Text();

		//public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {	
		//public void map(Text key, Text value, Context context) throws IOException, InterruptedException {	
		//public void map(Text key, Text value, Context context) throws IOException, InterruptedException {	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {	
			StringTokenizer t = new StringTokenizer(value.toString());

			while(t.hasMoreTokens()) {
				word.set(t.nextToken());
				context.write(word, one);				
			}
		}	
	}

	static class WordCountNewReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, Context context) throws IOException, InterruptedException {	
			int sum = 0;
			
			while(values.hasNext()) {
				sum += values.next().get();
			}	
			System.out.println(key + " = " + sum);
			context.write(key, new IntWritable(sum));
		}	
	}
*/	
/*
	static class PageRankMapper extends Mapper<Text, Text, Text, Text> {
		Text title = new Text();
		Text links = new Text();
		//String[] links;

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {	
			String[] strings = value.toString().split(" ");

			for(String s: strings) {
				//title.set(key);
				links.set(s);
				context.write(key, links);				
			}
		}	
	}

	static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Text value, Context context) throws IOException, InterruptedException {	
			int sum = 0;
			
		}	
	}
*/	
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
			String filename = "PageRank.inlink.out";
			String filepath = resultdir + "/" + filename;
			new File(resultdir).mkdirs();

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
/*
				doc = Jsoup.parse(content);
				String forbiddenCharacters = "`~!@#$%^&*{}[\"':;,.<>?/|\\";
				String patternToMatch = "[\\\\!\"#$%&()*+,./:;<=>?@\\[\\^_{|}~]+";
				patternToMatch = "#$%&()*+,./:;<=>?@\\[\\^_{|}~";
				Pattern pattern = Pattern.compile("\\[\\[([A-Za-z0-9.]+)\\]\\]");
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
				while(matcher.find()) {
					//System.out.print("Start index: " + matcher.start());
					//System.out.print(" End index: " + matcher.end() + " ");
					//System.out.println(matcher.group(0));
					//logger.info(matcher.group() + "\t");
					N ++;
				}
*/				
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
			filename = "PageRank.n.out";
			filepath = resultdir + "/" + filename;
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
					String filepath = resultdir + "/" + filename;
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

}
