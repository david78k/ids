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

	final static String OUTLINKOUT = "PageRank.outlink.out";
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
		System.out.println("Computing the PageRanks ...");
		pr.pagerank();
		//pr.finalize();
		System.out.println("All jobs complete.");

		//pr.wordcount();
	}
	
	void finalize(JobConf conf) throws Exception {
		FileSystem fs = FileSystem.get(new URI(outputdir), conf);
		Path src = new Path(outputpath + "/" + OUTLINKOUT);
		Path dest = new Path(outputdir + "/" + OUTLINKOUT);

		try {
			fs.delete(dest, true);
			fs.rename(src, dest);

			src = new Path(outputpath + "/" + NOUT);
			dest = new Path(outputdir + "/" + NOUT);
			
			fs.delete(dest, true);
			fs.rename(src, dest);
		} catch (FileNotFoundException e) {}
	}
	
	public static class DescendingDoubleComparator extends DoubleWritable.Comparator {
		public int compare (byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare (b1, s1, l1, b2, s2, l2);
		}
		static {
			WritableComparator.define(DescendingDoubleComparator.class, new DoubleWritable.Comparator());
		}
	}

	/** secondary sort by values (PageRank score)
  	 *  while merging files
  	 */
	void sort(int iter) throws Exception {
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("secondary sort");

		Path src = new Path(outputpath + "/iter" + iter + "-sort/part-00000");
		//Path dest = new Path(outputpath + "/" + NOUT);
		Path dest = new Path(outputdir + "/" + ITER1);
		if(iter == MAX_ITER)
			dest = new Path(outputdir + "/" + ITER8);

		FileSystem fs = FileSystem.get(new URI(outputdir), conf);
		try {
			fs.delete(dest, true);
		} catch (FileNotFoundException e) {}

		//conf.setOutputKeyClass(Text.class);
		conf.setOutputKeyClass(DoubleWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(SortMapper.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(SortReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

                FileInputFormat.setInputPaths(conf, new Path(outputpath + "/iter" + iter));
                //FileOutputFormat.setOutputPath(conf, new Path(outputdir + "/" + ITER1));
                FileOutputFormat.setOutputPath(conf, new Path(outputpath + "/iter" + iter + "-sort"));

		conf.setNumReduceTasks(1);
		//conf.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		conf.setOutputKeyComparatorClass(DescendingDoubleComparator.class);

		JobClient.runJob(conf);
		
		fs.rename(src, dest);
	}

	/** filter scores >= 5.0/N and
  	 *  input: <file, line (title pagerank inliks)> = <LongWritable, Text>
 	 *  output: <score, title> = <DoubleWritable, Text>
  	 */
	public static class SortMapper extends MapReduceBase implements Mapper<LongWritable, Text, DoubleWritable, Text> {
		private Text word = new Text();
		private static StringBuffer sb = new StringBuffer();

		public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			StringTokenizer tok = new StringTokenizer(line);	
			//System.out.print(line + ",\t");
			String title = tok.nextToken();
			word.set(title);
			double score = Double.parseDouble(tok.nextToken());
			//if (score >= 5.0/N)
		//	if (score >= 0.2/N)
				output.collect(new DoubleWritable(score), word);
		}
	}

	/** sort by PageRank score
  	 *  while merging files
 	 *  input: <score, title list> = <DoubleWritable, Text>
 	 *  output: <title, score> = <Text, DoubleWritable>
  	 */
	public static class SortReducer extends MapReduceBase implements Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		private Text word = new Text();

		public void reduce(DoubleWritable key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			//System.out.println(key.toString() + ", " + values.toString());
			//System.out.println(key.toString());
			double score = key.get();
			String title;
			//sort R by page rank score
			TreeMap rank = new TreeMap();	

			while (values.hasNext()) {
				title = values.next().toString();
				rank.put(title, score);
				output.collect(new Text(title), new DoubleWritable (score));
				System.out.println(title + "\t" + score);
			}
		}
	}

	void pagerank() throws Exception {
		iterate(1);
		sort(1);
		for (int i = 2; i <= MAX_ITER; i ++) { 
			iterate(i);
		}	
		sort(MAX_ITER);
	}

	public static class CustomPathFilter implements PathFilter {
	    @Override
	    public boolean accept(Path path) {
	        return false; 
	    }
	}

	void iterate(int iter) throws Exception {
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("iterate" + iter);
		//conf.setNumReduceTasks(1);
		
		conf.setOutputKeyClass(Text.class);
		//conf.setOutputKeyClass(Page.class);
		conf.setOutputValueClass(Text.class);
		//conf.setOutputValueClass(Page.class);
		//conf.setOutputValueClass(DoubleWritable.class);

		conf.setMapperClass(PageRankMapper.class);
		conf.setReducerClass(PageRankReducer.class);
		//conf.setCombinerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

	        FileInputFormat.setInputPaths(conf, new Path(outputpath + "/iter" + (iter - 1)));
	        FileOutputFormat.setOutputPath(conf, new Path(outputpath + "/iter" + iter));
		if (iter == 1) {
                	//FileInputFormat.setInputPaths(conf, new Path(outputpath + "/" + OUTLINKOUT));
	                FileInputFormat.setInputPaths(conf, new Path(outputdir + "/" + OUTLINKOUT));
	                //FileOutputFormat.setOutputPath(conf, new Path(outputpath + "/pagerank"));
		} else if (iter == MAX_ITER){
/*
		Path src = new Path(outputpath + "/n/part-00000");
		Path dest = new Path(outputpath + "/" + NOUT);
		FileSystem.get(new URI(outputpath), conf).rename(src, dest);
*/

		} else {
	        //        FileInputFormat.setInputPaths(conf, new Path(outputpath + "/iter" + (iter - 1)));
		}

		try {
			JobClient.runJob(conf);
		} catch (IOException e) {}
	}

	/** input: <file, line containing title inlinks> = <LongWritable, Text>
	 *  output: <page, pagerank> = <Page, Double>
	 */
	public static class PageRankMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			//System.out.println(key.toString() + ", " + value);
			// map phase: read N and compute column sum 
			// reduce phase: compute rank

			int count = 0;
			StringBuffer sb = new StringBuffer();

			String line = value.toString();

			ArrayList<String> links = new ArrayList<String>();
			StringTokenizer tok = new StringTokenizer(line);
			String title = tok.nextToken();
			double pagerank = 1.0/N;
			
			while (tok.hasMoreTokens()) {
				String link = tok.nextToken();
				links.add(link);
				sb.append(link + "\t");
				count ++;
			}
			output.collect(new Text(title), new Text((pagerank/count) + "\tPageRankPageNode\t" + sb.toString()));
			
			double share = -1;
			int size = links.size();	
			if (size > 0)
				share = pagerank/size;
			for (String link: links) 
				output.collect(new Text(link), new Text(share + ""));
		}
	}

	/** input: <Page, pagerank list)> = <Page, Double>
	 *  output: <title, pagerank with inlinks> = <Text, Page>
	 */
	public static class PageRankReducer extends MapReduceBase implements Reducer<Text, Text, Text, Page> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Page> output, Reporter reporter) throws IOException {
			//System.out.print(key + " ");

			ArrayList<String> links = new ArrayList<String>();
			double sum = 0;
			boolean isPage = false;
			while (values.hasNext()) {
				Text text = values.next();
				// parse NPR (neighbor pagerank) and inliks
				StringTokenizer tok = new StringTokenizer(text.toString());
				// first parse npr
				double npr = Double.parseDouble(tok.nextToken());	
				if(tok.hasMoreTokens() && tok.nextToken().equals("PageRankPageNode"))
					isPage = true;
				// next part inlinks
				while(tok.hasMoreTokens())
					links.add(tok.nextToken());
				//System.out.print(npr + " ");
				if (npr != -1)
					sum += npr;
			}

			//p.pagerank = (1 - d)*N + d*sum(p.pagerank/p.columnsum())
			double pr = (1 - d)/N + d*sum;
			if(isPage)
				output.collect(key, new Page(key.toString(), pr, links));
		}
	}

	void totalpages() throws Exception {
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("compute total pages");
		conf.setNumReduceTasks(1);
		
		Path src = new Path(outputpath + "/n/part-00000");
		//Path dest = new Path(outputpath + "/" + NOUT);
		Path dest = new Path(outputdir + "/" + NOUT);
		FileSystem fs = FileSystem.get(new URI(outputdir), conf);
		try {
			fs.delete(dest, true);
		} catch (FileNotFoundException e) {}

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(TotalPagesMapper.class);
		conf.setReducerClass(TotalPagesReducer.class);
		//conf.setCombinerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

                FileInputFormat.setInputPaths(conf, new Path(outputdir + "/" + OUTLINKOUT));
                FileOutputFormat.setOutputPath(conf, new Path(outputpath + "/n"));

		JobClient.runJob(conf);
		
		fs.rename(src, dest);
		//FileSystem.get(new URI(outputpath), conf).rename(src, dest);

		//finalize(conf);
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
			N = sum;
			output.collect(key, new Text());
			//output.collect(key, new IntWritable(sum));
		}
	}

	void parse() throws Exception {
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("parse adjacency graph");
		conf.setNumReduceTasks(1);

		//Path src = new Path(outputpath + "/inlink/part-00000");
		Path src = new Path(outputpath + "/part-00000");
		Path dest = new Path(outputdir + "/" + OUTLINKOUT);
		//Path dest = new Path(outputpath + "/" + OUTLINKOUT);
		FileSystem fs = FileSystem.get(new URI(outputdir), conf);
		try {
			fs.delete(dest, true);
		} catch (FileNotFoundException e) {}

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

		fs.rename(src, dest);
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
							
						//String link = sb.toString().replaceAll(" ", "_");
						String link = sb.toString(); //.replaceAll(" ", "_");
						if(!link.startsWith("#top") // 3. table row
							&& !link.matches(".*:.+:.*") // 1. interwiki
							//&& !link.matchs("#section name")
							&& !link.contains("#") // 2. section
							&& !link.contains("/") // 4. subpage
							//&& !// no duplicate
							&& !link.equals(title)// not title
						)
							output.collect(new Text(title.replaceAll(" ", "_")), new Page(pid ++, link.replaceAll(" ", "_"), 1, new ArrayList()));
							//output.collect(new Text(title.replaceAll(" ", "_")), new Page(pid ++, link, 1, new ArrayList()));

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

	static class Page implements WritableComparable<Page> {
		int id;
		String title;
		double pagerank = 1;
		ArrayList<String> links = new ArrayList<String>();

		Page() {}

		Page (String title, double pagerank) {
			this.id = 0;
			this.title = title;
			this.pagerank = pagerank;
		}

		Page (String title, double pagerank, ArrayList links) {
			this.id = 0;
			this.title = title;
			this.pagerank = pagerank;
			this.links = links;
		}

		Page (int id, String title, double pagerank, ArrayList links) {
			this.id = id;
			this.title = title;
			this.pagerank = pagerank;
			this.links = links;
		}
		
		public int compareTo(Page p2) {
			return title.compareTo(p2.title);
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
			StringBuffer sb = new StringBuffer();
			for (String link: links)
				sb.append(link + "\t");
			return pagerank + "\t" + sb.toString();
			//return pagerank + "\t" + links.toString();
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

	static Iterator valueIterator(TreeMap map) {
		Set set = new TreeSet(new Comparator<Map.Entry<String, Double>>() {
			public int compare(Map.Entry<String, Double> e1, Map.Entry<String, Double> e2) {
				return e1.getValue().compareTo(e2.getValue()) > 0 ? 1 : -1;
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

