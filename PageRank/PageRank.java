package PageRank;

import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.text.*;
import java.util.regex.*;
import java.net.URI;

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

		System.out.println("Parsing the inlinks ...");
		pr.parse();
		System.out.println("Computing the number of total pages ...");
		pr.totalpages();
		//System.out.println("Computing the PageRanks ...");
		//pr.pagerank();
		// secondary sort by PageRank scores with resulting files
		//System.out.println("Sorting the PageRanks ...");
		//pr.secsort(); 
		System.out.println("All jobs complete.");

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
		conf.setJobName("page rank");
		//conf.setNumReduceTasks(1);
		
		conf.setOutputKeyClass(Text.class);
		//conf.setOutputValueClass(Page.class);
		//conf.setOutputValueClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(TotalPagesMapper.class);
		conf.setReducerClass(TotalPagesReducer.class);
		//conf.setCombinerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		//conf.setInputFormat(XmlInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

                FileInputFormat.setInputPaths(conf, new Path(outputpath));
                FileOutputFormat.setOutputPath(conf, new Path(outputpath + "/pagerank"));
		//FileInputFormat.setInputPaths(conf, new Path(args[0]));
		//FileOutputFormat.setOutputName(conf, "PageRank.n.out");

		JobClient.runJob(conf);
		
/*
		Path src = new Path(outputpath + "/n/part-00000");
		Path dest = new Path(outputpath + "/" + NOUT);
		FileSystem.get(new URI(outputpath), conf).rename(src, dest);
*/
	}

	public static class PageRankMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private static StringBuffer sb = new StringBuffer();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			//System.out.println(key.toString() + ", " + value);
			//Page p = value.get();
			// map phase: read N and compute column sum 
			// reduce phase: compute rank
			//p.pagerank = (1 - d)*N + d*sum(p.pagerank/p.columnsum())
			/*
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
			//	output.collect(word, one);
			}
			*/
			output.collect(new Text("N"), one);
		}
	}

	//public static class TotalPagesReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	public static class PageRankReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			//System.out.println(key.toString() + ", " + values);
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			key.set("N=" + sum);
			N = sum;
			output.collect(key, new Text());
			//output.collect(key, new IntWritable(sum));
		}
	}

	void totalpages() throws Exception {
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("compute total pages");
		conf.setNumReduceTasks(1);
		
		conf.setOutputKeyClass(Text.class);
		//conf.setOutputValueClass(Page.class);
		//conf.setOutputValueClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(TotalPagesMapper.class);
		conf.setReducerClass(TotalPagesReducer.class);
		//conf.setCombinerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		//conf.setInputFormat(XmlInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

                FileInputFormat.setInputPaths(conf, new Path(outputpath));
                FileOutputFormat.setOutputPath(conf, new Path(outputpath + "/n"));
		//FileInputFormat.setInputPaths(conf, new Path(args[0]));
		//FileOutputFormat.setOutputName(conf, "PageRank.n.out");

		JobClient.runJob(conf);
		
		Path src = new Path(outputpath + "/n/part-00000");
		Path dest = new Path(outputdir + "/" + NOUT);
		//Path dest = new Path(outputpath + "/" + NOUT);
		FileSystem.get(new URI(outputdir), conf).rename(src, dest);
		//FileSystem.get(new URI(outputpath), conf).rename(src, dest);
	}

	public static class TotalPagesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private static StringBuffer sb = new StringBuffer();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			output.collect(new Text("N"), one);
		}
	}

	//public static class TotalPagesReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	public static class TotalPagesReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			//System.out.println(key.toString() + ", " + values);
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			key.set("N=" + sum);
			output.collect(key, new Text());
			//output.collect(key, new IntWritable(sum));
		}
	}

	void parse() throws Exception {
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("parse adjacency graph");
		conf.setNumReduceTasks(1);

		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Page.class);

		conf.setMapperClass(ParseMapper.class);
		conf.setReducerClass(ParseReducer.class);
		//conf.setCombinerClass(Reduce.class);

		conf.setInputFormat(XmlInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

                FileInputFormat.setInputPaths(conf, new Path(inputpath));
                FileOutputFormat.setOutputPath(conf, new Path(outputpath));

		JobClient.runJob(conf);

		/*
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Page.class);

		conf.setMapperClass(ParseMapper.class);
		conf.setReducerClass(ParseReducer.class);
		//conf.setCombinerClass(Reduce.class);

		conf.setInputFormat(XmlInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		JobClient.runJob(conf);
		*/

		//Path src = new Path(outputpath + "/inlink/part-00000");
		Path src = new Path(outputpath + "/part-00000");
		Path dest = new Path(outputdir + "/" + INLINKOUT);
		//Path dest = new Path(outputpath + "/" + INLINKOUT);
		FileSystem.get(new URI(outputdir), conf).rename(src, dest);
		//FileSystem.get(new URI(outputpath), conf).rename(src, dest);
	}

	//public static class InlinkReducer extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, IntWritable> {
	public static class InlinkReducer extends MapReduceBase implements Reducer<LongWritable, Text, Text, Text> {
		//public void reduce(LongWritable key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		//public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, IntWritable> output, Reporter reporter) throws IOException {
		public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			//System.out.print(key.toString() + "\t");
			//System.out.println(key.toString() + ", " + values.toString());
			Text word = new Text();
			while (values.hasNext()) {
				String line = values.next().toString();
				//System.out.print(line + ",\t");
				//word.set(line);
				output.collect(word, new Text(line));
				//output.collect(word, new IntWritable(Integer.parseInt(tok.nextToken())));
			}
			//System.out.println();
		//	output.collect(word, new IntWritable(sum));
		}
	}

	//public static class PageRankMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	public static class ParseMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Page> {
		/** extract Page data structure */
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
			if (content == null || content.length() == 0) return;

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
						if(!link.startsWith("#top") // 3. table row
							&& !link.matches(".*:.+:.*") // 1. interwiki
							//&& !link.matchs("#section name")
							&& !link.contains("#") // 2. section
							&& !link.contains("/") // 4. subpage
						)
							output.collect(new Text(title.replaceAll(" ", "_")), new Page(pid ++, link, 1, new ArrayList()));

						//System.out.println(link);
						sb = new StringBuffer();
					}
				}				
			}
		}
	}

	//public static class PageRankReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	public static class ParseReducer extends MapReduceBase implements Reducer<Text, Page, Text, Text> {
		//public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		public void reduce(Text key, Iterator<Page> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			System.out.println(key.toString());

			int sum = 0;
			Set set = new HashSet();
			ArrayList list = new ArrayList();
			Text inlinks = new Text();
			StringBuffer sb = new StringBuffer();
			while (values.hasNext()) {
				Page p = values.next();
				//System.out.println(p.toString());
				//String title = (String)set.get(p.title);
				if(!list.contains(p.title) && !p.title.equals(key.toString())) {
					sb.append(p.title + "\t");
					//list.add(p);
					set.add(p.title);
				}	
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
			Text.writeString(out, title);
			out.writeDouble(pagerank);
			out.writeInt(links.size());
			for(String link: links) 
				Text.writeString(out, link);
	       	}

		public void readFields(DataInput in) throws IOException {
	        	id = in.readInt();
			title = Text.readString(in);
//			System.out.println(title);
			pagerank = in.readDouble();
//			System.out.println(pagerank);
			int size = in.readInt();
			links = new ArrayList();
			for(int i = 0; i < size; i ++) {
				String link = Text.readString(in);
				links.add(link);
			}
		}

		public String toString() {
			return pagerank + "\t" + links.toString();
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
}

