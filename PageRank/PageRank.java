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
	//final static int MAX_ITER = 1;
	final static int MAX_ITER = 8;
	final static double THRESHOLD = 5.0; // pagerank score threshold to show top ranked pages 
	private static int iter = 1; // current iteration

	final static String OUTLINKOUT = "PageRank.outlink.out";
	final static String NOUT = "PageRank.n.out";
	final static String ITER1 = "PageRank.iter1.out";
	final static String ITER8 = "PageRank.iter8.out";

	private static final String NOREDLINK = "*I-AM-NO-RED-LINK*";

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

		System.out.println("Parsing the outlinks ...");
		pr.parse();
		System.out.println("Filtering the red links ...");
		pr.filter();
		System.out.println("Computing the number of total pages ...");
		pr.totalpages();
		System.out.println("Computing the PageRanks of " + N + " pages ...");
		pr.pagerank();
		System.out.println("All jobs complete.");
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
	void sort() throws Exception {
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("secondary sort");

		Path src = new Path(outputpath + "/iter" + iter + "-sort/part-00000");
		Path dest = new Path(outputdir + "/" + ITER1);
		if(iter == MAX_ITER)
			dest = new Path(outputdir + "/" + ITER8);

		FileSystem fs = FileSystem.get(new URI(outputdir), conf);
		try {
			fs.delete(dest, true);
		} catch (FileNotFoundException e) {}

		conf.setOutputKeyClass(DoubleWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(SortMapper.class);
		conf.setReducerClass(SortReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

                FileInputFormat.setInputPaths(conf, new Path(outputpath + "/iter" + iter));
                FileOutputFormat.setOutputPath(conf, new Path(outputpath + "/iter" + iter + "-sort"));

		conf.setNumReduceTasks(1);
		conf.setOutputKeyComparatorClass(DescendingDoubleComparator.class);

		// read N from file
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(outputdir + "/" + NOUT))));
                String line = br.readLine();
                if (line != null){
			N = Integer.parseInt(line.trim().substring(2));
                        System.out.println("N = " + N + ", (" + line + ")");
			conf.set("N", N + "");
                }

		JobClient.runJob(conf);
		
		fs.rename(src, dest);
	}

	/** filter scores >= 5.0/N and
  	 *  input: <file, line (title pagerank inliks)> = <LongWritable, Text>
 	 *  output: <score, title> = <DoubleWritable, Text>
  	 */
	public static class SortMapper extends MapReduceBase implements Mapper<LongWritable, Text, DoubleWritable, Text> {
		private int N;
		private Text word = new Text();
		private static StringBuffer sb = new StringBuffer();

		public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();
			StringTokenizer tok = new StringTokenizer(line);	
			String title = tok.nextToken();
			word.set(title);
			double score = Double.parseDouble(tok.nextToken());

			if (score >= THRESHOLD/N)
				output.collect(new DoubleWritable(score), word);
		}

		public void configure(JobConf conf) {
			N = Integer.parseInt(conf.get("N"));
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
			double score = key.get();
			String title;

			while (values.hasNext()) {
				title = values.next().toString();
				output.collect(new Text(title), new DoubleWritable (score));
				System.out.println(title + "\t" + score);
			}
		}
	}

	void pagerank() throws Exception {
		System.out.println("First iteration ...");
		iterate();
		System.out.println("Sorting the first iteration ...");
		sort();
		//for (int i = 2; i <= MAX_ITER; i ++) { 
		for (iter = 2; iter <= MAX_ITER; iter ++) { 
			System.out.println(iter + "th iteration ...");
			iterate();
			if (iter == MAX_ITER) {
				System.out.println("Sorting the last iteration ...");
				sort();
			}
		}	
	}

	void iterate() throws Exception {
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("iterate" + iter);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(PageRankMapper.class);
		conf.setReducerClass(PageRankReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

	        FileInputFormat.setInputPaths(conf, new Path(outputpath + "/iter" + (iter - 1)));
	        FileOutputFormat.setOutputPath(conf, new Path(outputpath + "/iter" + iter));
		if (iter == 1) {
	                FileInputFormat.setInputPaths(conf, new Path(outputdir + "/" + OUTLINKOUT));
		} 

		// read N from NOUT file
		FileSystem fs = FileSystem.get(new URI(outputdir), conf);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(outputdir + "/" + NOUT))));
                String line = br.readLine();
                if (line != null){
			N = Integer.parseInt(line.trim().substring(2));
                        System.out.println("N = " + N + ", (" + line + ")");
			conf.set("N", N + "");
                }

		try {
			JobClient.runJob(conf);
		} catch (IOException e) {}
	}

	/** input: <file, line containing title outlinks> = <LongWritable, Text>
	 *  output: <page, pagerank> = <Page, Double>
	 */
	public static class PageRankMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private int N;

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			// map phase: read N and compute column sum 
			// reduce phase: compute rank
			StringBuffer sb = new StringBuffer();

			String line = value.toString();

			ArrayList<String> links = new ArrayList<String>();
			StringTokenizer tok = new StringTokenizer(line);
			String title = tok.nextToken();
		
			double pagerank = 1.0/N;
			
			if(iter > 1) {
				pagerank = Double.parseDouble(tok.nextToken());
			}
			
			while (tok.hasMoreTokens()) {
				String link = tok.nextToken().trim();
				if(!link.equals("")) {
					links.add(link);
					sb.append(link + "\t");
				}
			}

			// insert itself
			output.collect(new Text(title), new Text("0\tPageRankPageNode\t" + sb.toString()));
			
			// insert others
			double share = -1;
			int size = links.size();	
			if (size > 0) {
				share = pagerank/size;
				for (String link: links) 
					output.collect(new Text(link), new Text(share + ""));
			}
		}
		
		public void configure(JobConf conf) {
			N = Integer.parseInt(conf.get("N"));
		}
	}

	/** input: <Page, pagerank list)> = <Page, Double>
	 *  output: <title, pagerank with outlinks> = <Text, Page>
	 */
	public static class PageRankReducer extends MapReduceBase implements Reducer<Text, Text, Text, Page> {
		private int N;

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Page> output, Reporter reporter) throws IOException {
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
				// next part outlinks
				while(tok.hasMoreTokens())
					links.add(tok.nextToken());

				if (npr != -1)
					sum += npr;
			}

			//p.pagerank = (1 - d)*N + d*sum(p.pagerank/p.columnsum())
			double pr = (1 - d)/N + d*sum;
			if(isPage) {
				output.collect(key, new Page(key.toString(), pr, links));
			}
		}

		public void configure(JobConf conf) {
			N = Integer.parseInt(conf.get("N"));
		}
	}

	void totalpages() throws Exception {
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("compute total pages");
		conf.setNumReduceTasks(1);
		
		Path src = new Path(outputpath + "/n/part-00000");
		Path dest = new Path(outputdir + "/" + NOUT);
		FileSystem fs = FileSystem.get(new URI(outputdir), conf);
		try {
			fs.delete(dest, true);
		} catch (FileNotFoundException e) {}

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(TotalPagesMapper.class);
		conf.setReducerClass(TotalPagesReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

                FileInputFormat.setInputPaths(conf, new Path(outputdir + "/" + OUTLINKOUT));
                FileOutputFormat.setOutputPath(conf, new Path(outputpath + "/n"));

		JobClient.runJob(conf);
		
		fs.rename(src, dest);
	}

	public static class TotalPagesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private static StringBuffer sb = new StringBuffer();

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			output.collect(new Text("N"), one);
		}
	}

	public static class TotalPagesReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {
		private static int sum = 0;
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			//System.out.println(key.toString() + ", " + values);
		//	int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			key.set("N=" + sum);
			N = sum;
			
			output.collect(key, new Text());
		}
	}

	void parse() throws Exception {
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("parse adjacency graph");
		//conf.setNumReduceTasks(1);

		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Page.class);

		conf.setMapperClass(ParseMapper.class);
		conf.setReducerClass(ParseReducer.class);

		conf.setInputFormat(XmlInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

                FileInputFormat.setInputPaths(conf, new Path(inputpath));
                FileOutputFormat.setOutputPath(conf, new Path(outputpath));

		JobClient.runJob(conf);
	}

	// filter red links
	void filter() throws Exception {
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("filter red links");
		conf.setNumReduceTasks(1);

		Path src = new Path(outputpath + "/outlink/part-00000");
		Path dest = new Path(outputdir + "/" + OUTLINKOUT);

		FileSystem fs = FileSystem.get(new URI(outputdir), conf);
		try {
			fs.delete(dest, true);
		} catch (FileNotFoundException e) {}

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Page.class);

		conf.setMapperClass(RedlinkFilterMapper.class);
		conf.setReducerClass(RedlinkFilterReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

                //FileInputFormat.setInputPaths(conf, new Path(outputdir + "/PageRank.inlink.out"));
                FileInputFormat.setInputPaths(conf, new Path(outputpath));
                FileOutputFormat.setOutputPath(conf, new Path(outputpath + "/outlink"));

		JobClient.runJob(conf);

		fs.rename(src, dest);
	}

	/** parse mapper: parses <person> tag from input xml file and produces <title, page> pair
	 *  input: <file, person tag>
	 *  output: <title, page> = <Text, Page>
	 */
	public static class ParseMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Page> {
		/** extract Page data structure */
		public void map(LongWritable key, Text value, OutputCollector<Text, Page> output, Reporter reporter) throws IOException {
			String xmlstr = value.toString();
			Page p;
				
			Document doc = Jsoup.parse(xmlstr);
	
			Element e = doc.select("title").first();
			String title = e.text().replaceAll(" ", "_");
			Text titleText = new Text(title);

			output.collect(titleText, new Page("", 1));

			e = doc.select("text").first();
			String text = doc.body().text();
			text = e.text();

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
							&& !link.contains(":") // 1. interwiki
							//&& !link.matches(".*:.+:.*") // 1. interwiki
							//&& !link.matchs("#section name")
							&& !link.contains("#") // 2. section
							&& !link.contains("/") // 4. subpage
							&& !link.equals(title)// not title
							//&& !// no duplicate
						) 
							output.collect(titleText, new Page(link, 1));

						sb = new StringBuffer();
					}
				}				
			}
		}
	}

	/** 
 	*   input: <title, outlink page> = <Text, Page>
 	*   output: <title, string of outlink list> = <Text, Text>
 	*/ 
	public static class ParseReducer extends MapReduceBase implements Reducer<Text, Page, Text, Text> {

		public void reduce(Text key, Iterator<Page> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		//	int sum = 0;
			Set<String> set = new HashSet<String>(); // no duplicate tlink
			Text outlinks = new Text();
			StringBuffer sb = new StringBuffer();

			while (values.hasNext()) {
				Page p = values.next();
				//System.out.println(p.toString());
				if(p !=null && !p.title.equals("") && p.title != ""
					&& !p.title.equals(key.toString()) // no self-refrence link
					// no red links
				) {
					sb.append(p.title + "\t");
					set.add(p.title);
				}	
			}
			outlinks.set(sb.toString());

			output.collect(key, outlinks);
		}
	}

	/** 
 	*   input: <title, string of outlink list> = <Text, Text>
 	*   output: <outlink, title> = <Text, Text>
 	*/ 
	public static class RedlinkFilterMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Page> {
		/** extract Page data structure */
		public void map(LongWritable key, Text value, OutputCollector<Text, Page> output, Reporter reporter) throws IOException {
			String line = value.toString();
			StringTokenizer tok = new StringTokenizer(line);
			String title = tok.nextToken();
			Text titleText = new Text(title);
			
			//System.out.println(line + ", title = " + title);
			output.collect(titleText, new Page(NOREDLINK, 1));	

			while(tok.hasMoreTokens()) {
				output.collect(new Text(tok.nextToken()), new Page(title, 1));	
			}
		}				
	}

	/** 
 	*   input: <title, outlink page> = <Text, Page>
 	*   output: <title, string of outlink list> = <Text, Text>
 	*/ 
	public static class RedlinkFilterReducer extends MapReduceBase implements Reducer<Text, Page, Text, Text> {
		public void reduce(Text key, Iterator<Page> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			boolean isRedlink = true;
			String title = key.toString();

			// remove red link

			Set<String> set = new HashSet<String>(); // no duplicate tlink
			Text outlinks = new Text();
			StringBuffer sb = new StringBuffer();

			while (values.hasNext()) {
				Page p = values.next();
				//System.out.println(p.toString());
				if(p !=null && !p.title.equals(key.toString()) // no self-refrence link
					// no red links
				) {
					if(p.title.equals(NOREDLINK))
						isRedlink = false;
					else {
						String link = p.title;
						if( !link.contains(":") // 1. interwiki
							//&& !link.matchs("#section name")
							&& !link.contains("#") // 2. section
							&& !link.contains("/") // 4. subpage
							&& !link.equals(title)// not title
							//&& !// no duplicate
						) { 
							sb.append(p.title + "\t");
							set.add(p.title);
						}
					}
				}	
			}
			
			if(!isRedlink) {
				outlinks.set(sb.toString());
				output.collect(key, outlinks);
			}
		}
	}

	static class Page implements WritableComparable<Page> {
		String title;
		double pagerank = 1;
		ArrayList<String> links = new ArrayList<String>();

		Page() {}

		Page (String title, double pagerank) {
			this.title = title;
			this.pagerank = pagerank;
		}

		Page (String title, double pagerank, ArrayList links) {
			this.title = title;
			this.pagerank = pagerank;
			this.links = links;
		}

		public int compareTo(Page p2) {
			return title.compareTo(p2.title);
		}

		public void write(DataOutput out) throws IOException {
			Text.writeString(out, title);
			out.writeDouble(pagerank);
			out.writeInt(links.size());
			for(String link: links) 
				Text.writeString(out, link);
	       	}

		public void readFields(DataInput in) throws IOException {
			title = Text.readString(in);
			pagerank = in.readDouble();
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
		}
	}
}


