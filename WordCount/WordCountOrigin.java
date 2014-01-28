package org.myorg;
2.	
3.	import java.io.*;
4.	import java.util.*;
5.	
6.	import org.apache.hadoop.fs.Path;
7.	import org.apache.hadoop.filecache.DistributedCache;
8.	import org.apache.hadoop.conf.*;
9.	import org.apache.hadoop.io.*;
10.	import org.apache.hadoop.mapred.*;
11.	import org.apache.hadoop.util.*;
12.	
13.	public class WordCount extends Configured implements Tool {
14.	
15.	    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
16.	
17.	      static enum Counters { INPUT_WORDS }
18.	
19.	      private final static IntWritable one = new IntWritable(1);
20.	      private Text word = new Text();
21.	
22.	      private boolean caseSensitive = true;
23.	      private Set<String> patternsToSkip = new HashSet<String>();
24.	
25.	      private long numRecords = 0;
26.	      private String inputFile;
27.	
28.	      public void configure(JobConf job) {
29.	        caseSensitive = job.getBoolean("wordcount.case.sensitive", true);
30.	        inputFile = job.get("map.input.file");
31.	
32.	        if (job.getBoolean("wordcount.skip.patterns", false)) {
33.	          Path[] patternsFiles = new Path[0];
34.	          try {
35.	            patternsFiles = DistributedCache.getLocalCacheFiles(job);
36.	          } catch (IOException ioe) {
37.	            System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
38.	          }
39.	          for (Path patternsFile : patternsFiles) {
40.	            parseSkipFile(patternsFile);
41.	          }
42.	        }
43.	      }
44.	
45.	      private void parseSkipFile(Path patternsFile) {
46.	        try {
47.	          BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
48.	          String pattern = null;
49.	          while ((pattern = fis.readLine()) != null) {
50.	            patternsToSkip.add(pattern);
51.	          }
52.	        } catch (IOException ioe) {
53.	          System.err.println("Caught exception while parsing the cached file '" + patternsFile + "' : " + StringUtils.stringifyException(ioe));
54.	        }
55.	      }
56.	
57.	      public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
58.	        String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
59.	
60.	        for (String pattern : patternsToSkip) {
61.	          line = line.replaceAll(pattern, "");
62.	        }
63.	
64.	        StringTokenizer tokenizer = new StringTokenizer(line);
65.	        while (tokenizer.hasMoreTokens()) {
66.	          word.set(tokenizer.nextToken());
67.	          output.collect(word, one);
68.	          reporter.incrCounter(Counters.INPUT_WORDS, 1);
69.	        }
70.	
71.	        if ((++numRecords % 100) == 0) {
72.	          reporter.setStatus("Finished processing " + numRecords + " records " + "from the input file: " + inputFile);
73.	        }
74.	      }
75.	    }
76.	
77.	    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
78.	      public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
79.	        int sum = 0;
80.	        while (values.hasNext()) {
81.	          sum += values.next().get();
82.	        }
83.	        output.collect(key, new IntWritable(sum));
84.	      }
85.	    }
86.	
87.	    public int run(String[] args) throws Exception {
88.	      JobConf conf = new JobConf(getConf(), WordCount.class);
89.	      conf.setJobName("wordcount");
90.	
91.	      conf.setOutputKeyClass(Text.class);
92.	      conf.setOutputValueClass(IntWritable.class);
93.	
94.	      conf.setMapperClass(Map.class);
95.	      conf.setCombinerClass(Reduce.class);
96.	      conf.setReducerClass(Reduce.class);
97.	
98.	      conf.setInputFormat(TextInputFormat.class);
99.	      conf.setOutputFormat(TextOutputFormat.class);
100.	
101.	      List<String> other_args = new ArrayList<String>();
102.	      for (int i=0; i < args.length; ++i) {
103.	        if ("-skip".equals(args[i])) {
104.	          DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf);
105.	          conf.setBoolean("wordcount.skip.patterns", true);
106.	        } else {
107.	          other_args.add(args[i]);
108.	        }
109.	      }
110.	
111.	      FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
112.	      FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));
113.	
114.	      JobClient.runJob(conf);
115.	      return 0;
116.	    }
117.	
118.	    public static void main(String[] args) throws Exception {
119.	      int res = ToolRunner.run(new Configuration(), new WordCount(), args);
120.	      System.exit(res);
121.	    }
122.	}
