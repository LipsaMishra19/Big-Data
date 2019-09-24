package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfFloatInt;
import tl.lin.data.map.HMapStIW;
import tl.lin.data.map.HashMapWritable;

import java.util.StringTokenizer;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Iterator;
import java.util.Map;

import java.io.BufferedReader;
import java.io.InputStreamReader;


/** Implementation of Pointwise Mutual Information (PMI) - Function of two events X and Y
 *  Class : StripesPMI
 *  Output : The key should be a word (X), and the value should be a map, where each of the keys is a co-occurring word (Y), 
 *  and the value is a pair (PMI, count) denoting its PMI and co-occurrence count.
 **/

//Define class StripesPMI:
public class StripesPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);
  // First Mapper : Emits (Word, 1) for every word occurrence and keeps count of the no.of lines
  public static final class FirstPMIMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // Reuse objects to save overhead of object creation.
    private static final IntWritable ONE = new IntWritable(1);
    private static final Text WORD = new Text();
    //Counts the no.of lines
    public enum MyCounter { COUNT_LINES };

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

        Set<String> wordSet = new HashSet<String>();
        int count = 0;
        for (String word : Tokenizer.tokenize(value.toString())) {
          wordSet.add(word);
          count += 1;

          if (word.length()==0){
            continue;
          }
          // Considering the first 40 words on each line
          if (count > 40){
            break;
          }
        }

        List<String> newWords = new ArrayList<String>(wordSet);

        for(int i =0; i< newWords.size(); i++)
        {
          WORD.set(newWords.get(i));
          context.write(WORD,ONE);
        }
        Counter counter = context.getCounter(MyCounter.COUNT_LINES);
        counter.increment(1L);
    }
  }

  
  // First Reducer: sums up all the counts and emits (WORD,SUM)
  public static final class FirstPMIReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    // Reuse objects.
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  public static final class SecondPMIMapper extends Mapper<LongWritable, Text, Text, HMapStIW> {
    //Objects for reuse
    private static final HMapStIW MAP = new HMapStIW();
    private static final Text WORD = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      
      Set<String> wordSet = new HashSet<String>();
      int count = 0;

      for (String word : Tokenizer.tokenize(value.toString())) {
        wordSet.add(word);
        count += 1;

        if (count > 40){
          break;
        }
      }
      List<String> newWords = new ArrayList<String>(wordSet);

      String leftword, rightword = "";

      for (int i = 0; i < newWords.size(); i++) {
        leftword = newWords.get(i);
       
        for (int j = 0; j <newWords.size(); j++) {
          if (i == j) continue;
          rightword = newWords.get(j);
          
          if(!MAP.containsKey(rightword)){
            MAP.put(rightword,1);
        }//Each of the right words are stored in the map
        }
        WORD.set(leftword);
        context.write(WORD, MAP);
        MAP.clear();
      }
    }
  }

  public static final class PMICombiner extends Reducer<Text, HMapStIW, Text, HMapStIW> {

    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStIW> word = values.iterator();
      HMapStIW MAP = new HMapStIW();
      //Sum of each element of the map
      while (word.hasNext()) {
        MAP.plus(word.next());
      }

      context.write(key, MAP);
    }
  }


  // Second Reducer - Calculates the PMI and emits output as (X,{(Y0,(PMI,Count),(Y1,(PMI,Count)....})
  public static final class SecondPMIReducer extends Reducer<Text, HMapStIW, Text, HashMapWritable> {
    private static final Text KEY = new Text();
    private static final HashMapWritable MAP = new HashMapWritable();
    
    private static final Map<String, Integer> wordCount = new HashMap<String, Integer>();
    private static long linesCount;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      //Read from intermediate output of first job and count the number of lines
      Configuration conf = context.getConfiguration();
      linesCount = conf.getLong("counter", 0L);
      
      try{

      FileSystem fs = FileSystem.get(conf);
      FileStatus[] fstatus = fs.globStatus(new Path("tmp/part-r-*"));

   
     // Path interdata = new Path("./tmp/part-r-*");
      
     // if(!fs.exists(interdata)){
     // throw new IOException("Intermediate file is not present/created: " + interdata.toString());
     // }

      BufferedReader reader = null;
      for (FileStatus file : fstatus) {
        FSDataInputStream input = fs.open(file.getPath());
        InputStreamReader input_sr = new InputStreamReader(input);
        reader = new BufferedReader(input_sr);

        String line = reader.readLine();
        while (line != null) {
          String[] dataTokens = line.split("\\s+");
          if (dataTokens.length != 2) {
            LOG.info("Line does not have 2 tokens exactly: '" + line + "'");
          } else {
            wordCount.put(dataTokens[0], Integer.parseInt(dataTokens[1]));
          }
          line = reader.readLine();
        }
        reader.close();
      } 
      } catch (Exception e){
        throw new IOException("Intermediate File Does not EXIST !!!");
      }
    }



    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW map = new HMapStIW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      Configuration conf = context.getConfiguration();
      int threshold = conf.getInt("threshold", 0);
      
      String leftX = key.toString();
      KEY.set(leftX);
      MAP.clear();

      for (String rightY : map.keySet()) {
        //Check the threshold value
        if (map.get(rightY) >= threshold) {
          int sumOfPairs = map.get(rightY);
          //PMI Calculation
          double probPairXY = (double)sumOfPairs / (double)linesCount;
          double probX = (double)wordCount.get(leftX) / (double)linesCount;
          double probY = (double)wordCount.get(rightY) / (double)linesCount;

          float pmi = (float) Math.log10((double)probPairXY / ((double)probX * (double)probY));

          PairOfFloatInt PMI_PAIR = new PairOfFloatInt();
          PMI_PAIR.set(pmi, sumOfPairs);
          MAP.put(rightY, PMI_PAIR);
        }
      }
      if (MAP.size() > 0) {
        context.write(KEY, MAP);
      }
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private StripesPMI() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-threshold", metaVar = "[num]", usage = "threshold of sum of pairs")
    int threshold = 1;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    String sideDataPath = "tmp/";

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);
    LOG.info(" - threshold: " + args.threshold);

    Configuration conf = getConf();
    conf.set("sideDataPath", sideDataPath);
    conf.set("threshold", Integer.toString(args.threshold));
    Job job = Job.getInstance(conf);
    job.setJobName(StripesPMI.class.getSimpleName());
    job.setJarByClass(StripesPMI.class);

    job.setNumReduceTasks(args.numReducers);
    job.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");
    

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(sideDataPath));


    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(FirstPMIMapper.class);
    job.setReducerClass(FirstPMIReducer.class);

    // Delete the output directory if it exists already
    Path outputDir = new Path(sideDataPath);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    long count = job.getCounters().findCounter(FirstPMIMapper.MyCounter.COUNT_LINES).getValue();
    conf.setLong("counter", count);
    //PMI Job:
    Job jobPMI = Job.getInstance(conf);
    jobPMI.setJobName(StripesPMI.class.getSimpleName());
    jobPMI.setJarByClass(StripesPMI.class);

    jobPMI.setNumReduceTasks(args.numReducers);
    jobPMI.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    jobPMI.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    jobPMI.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    jobPMI.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    jobPMI.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    FileInputFormat.setInputPaths(jobPMI, new Path(args.input));
    FileOutputFormat.setOutputPath(jobPMI, new Path(args.output));

    jobPMI.setMapOutputKeyClass(Text.class);
    jobPMI.setMapOutputValueClass(HMapStIW.class);
    jobPMI.setOutputKeyClass(Text.class);
    jobPMI.setOutputValueClass(HashMapWritable.class);
    jobPMI.setOutputFormatClass(TextOutputFormat.class);

    jobPMI.setMapperClass(SecondPMIMapper.class);
    jobPMI.setCombinerClass(PMICombiner.class);
    jobPMI.setReducerClass(SecondPMIReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir1 = new Path(args.output);
    FileSystem.get(conf).delete(outputDir1, true);

    startTime = System.currentTimeMillis();
    jobPMI.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI(), args);
  }
}
