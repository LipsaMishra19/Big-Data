package ca.uwaterloo.cs451.a4;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import tl.lin.data.pair.PairOfObjectFloat;
import tl.lin.data.queue.TopScoredObjects;
import tl.lin.data.pair.PairOfInts;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;


public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);

  private static final String SOURCE_NODES = "node.src";

  private static class MyMapper extends
      Mapper<IntWritable, PageRankNode, PairOfInts, FloatWritable> {
    private ArrayList<TopScoredObjects<Integer>> topQueueList = new ArrayList<TopScoredObjects<Integer>>();
    private ArrayList<Integer> sources = new ArrayList<Integer>();

    @Override
    public void setup(Context context) throws IOException {
      
      String[] srcs = context.getConfiguration().getStrings(SOURCE_NODES, "");
      
      int n = context.getConfiguration().getInt("n", 100);
      
      String src;
      for(int i =0; i< srcs.length; i++){
        src= srcs[i];
        sources.add(Integer.valueOf(src));
      }
   
      for (int i = 0; i < sources.size(); i++) {
      	topQueueList.add(new TopScoredObjects<Integer>(n));
      }
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context) throws IOException,
        InterruptedException {
      for (int i = 0; i < sources.size(); i++) {
      	topQueueList.get(i).add(node.getNodeId(), node.getPageRank().get(i));
      	topQueueList.set(i, topQueueList.get(i));
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      PairOfInts key = new PairOfInts();
      FloatWritable value = new FloatWritable();

      for(int i = 0; i<sources.size(); i++){
      	for (PairOfObjectFloat<Integer> pair : topQueueList.get(i).extractAll()) {
          key.set(i, pair.getLeftElement());
          value.set(pair.getRightElement());
          context.write(key, value);
        }
        
      }
    }
  }

  private static class MyReducer extends
      Reducer<PairOfInts, FloatWritable, Text, Text> {
    private ArrayList<TopScoredObjects<Integer>> topQueueList = new ArrayList<TopScoredObjects<Integer>>();
    private ArrayList<Integer> sources = new ArrayList<Integer>();

    @Override
    public void setup(Context context) throws IOException {
      
      String[] srcs = context.getConfiguration().getStrings(SOURCE_NODES, "");

      int n = context.getConfiguration().getInt("n", 100);

      String src;
      for(int i = 0; i< srcs.length; i++){
        src = srcs[i];
        sources.add(Integer.valueOf(src));
      }
      
      for (int i = 0; i < sources.size(); i++) {
      	topQueueList.add(new TopScoredObjects<Integer>(n));
      }
    }

    @Override
    public void reduce(PairOfInts nid, Iterable<FloatWritable> iterable, Context context)
        throws IOException {
      Iterator<FloatWritable> iter = iterable.iterator();
      topQueueList.get(nid.getLeftElement()).add((int)nid.getRightElement(), iter.next().get());
      topQueueList.set(nid.getLeftElement(), topQueueList.get(nid.getLeftElement()));

      if (iter.hasNext()) {
        throw new RuntimeException();
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable key = new IntWritable();
      FloatWritable value = new FloatWritable();

      for(int i = 0; i<sources.size(); i++){
      	context.write(new Text("Source: " + sources.get(i)), new Text(""));
      	for (PairOfObjectFloat<Integer> pair : topQueueList.get(i).extractAll()) {
          key.set(pair.getLeftElement());
          value.set((float)StrictMath.exp(pair.getRightElement()));
          //Formatting the output
          context.write(new Text(String.format("%.5f", value.get())), new Text(String.valueOf(key)));
        }
       
        if (i < topQueueList.size() - 1) {
          context.write(new Text(""), new Text(""));
      	}
       
      }
    }
  }

  public ExtractTopPersonalizedPageRankNodes() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String TOP = "top";
  private static final String SOURCES = "sources";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("top n").create(TOP));
    options.addOption(OptionBuilder.withArgName("sources").hasArg()
    	.withDescription("sources").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || 
    	!cmdline.hasOption(TOP) || !cmdline.hasOption(SOURCES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int n = Integer.parseInt(cmdline.getOptionValue(TOP));
    String srcNode = cmdline.getOptionValue(SOURCES);

    LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
    LOG.info(" - input: " + inputPath);
    LOG.info(" - output: " + outputPath);
    LOG.info(" - top: " + n);
    LOG.info(" - sources: " + srcNode);

    Configuration conf = getConf();
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
    conf.setInt("n", n);
    conf.setStrings(SOURCE_NODES, srcNode);

    Job job = Job.getInstance(conf);
    job.setJobName(ExtractTopPersonalizedPageRankNodes.class.getName() + ":" + inputPath);
    job.setJarByClass(ExtractTopPersonalizedPageRankNodes.class);

    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(PairOfInts.class);
    job.setMapOutputValueClass(FloatWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    FileSystem fs = FileSystem.get(conf);

    // Delete the output directory if it exists already.
    fs.get(conf).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    Path path = new Path(outputPath + "/part-r-00000");
  	BufferedReader readbr = new BufferedReader(new InputStreamReader(fs.open(path)));
	  String readLine;
	  readLine = readbr.readLine();
	  while (readLine != null){
	    System.out.println(readLine);
	    readLine = readbr.readLine();
	  }
	
	  readbr.close();

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
    System.exit(res);
  }
}
