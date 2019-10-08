package ca.uwaterloo.cs451.a3;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfStringInt;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfWritables;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Partitioner;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

  private static final class BuildInvertedMapper extends Mapper<LongWritable, Text, PairOfStringInt, VIntWritable> {
    private static final PairOfStringInt PAIR = new PairOfStringInt();
    private static final VIntWritable VALUE = new VIntWritable();
    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<String>();

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(doc.toString());

      // Build a histogram of the terms.
      COUNTS.clear();
      for (String token : tokens) {
        COUNTS.increment(token);
      }

      // Emit postings.
      for (PairOfObjectInt<String> e : COUNTS) {
       PAIR.set(e.getLeftElement(), (int) docno.get());
       VALUE.set(e.getRightElement());
       context.write(PAIR, VALUE);

       PAIR.set(e.getLeftElement(), '*');
       VALUE.set(1);
       context.write(PAIR, VALUE);
      }
    }
  }

  protected static class CompressedPartitioner extends
    Partitioner<PairOfStringInt, VIntWritable> {
      @Override
      public int getPartition(PairOfStringInt pair, VIntWritable value,
       int numReduceTasks) {
        return (pair.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
      }
 }


  private static final class BuildInvertedReducer extends
      Reducer<PairOfStringInt, VIntWritable, Text, BytesWritable> {
        private static int df;
        private int tf;
        private int currdocNo;
        private int prevdocNo = 0;
        private int gap = 0;

        private static String pTerm;
        private String cTerm;
        private final static Text term = new Text();
        private static ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        private static DataOutputStream dataStream = new DataOutputStream(byteStream);
        private static Map<String, Integer> dfMap = new HashMap<String, Integer>();

        // private static final IntWritable DF = new IntWritable();
        @Override
        public void setup(Context context) {
          pTerm = null;
          cTerm = null;
          df = 0;
        }        

       @Override
       public void reduce(PairOfStringInt key, Iterable<VIntWritable> values, Context context)
          throws IOException, InterruptedException {
         Iterator<VIntWritable> iter = values.iterator();
         //ArrayListWritable<PairOfInts> postings = new ArrayListWritable<>();
         //To Calculate the document frequency and store it using HashMap
         if (key.getRightElement() == '*') {
         int df = 0;
         while (iter.hasNext()) {
           df = df + iter.next().get();
         
         }
         dfMap.put(key.getLeftElement(), df);
         }
         else {
           cTerm = key.getLeftElement();
           currdocNo = key.getRightElement();

           tf = 0;
           while (iter.hasNext())
           {
             tf = tf + iter.next().get();
           }

           if(pTerm == null || !cTerm.equals(pTerm)) {

             if(pTerm != null)
             {           
               
                 ByteArrayOutputStream outStream = new ByteArrayOutputStream();
                 DataOutputStream dataout = new DataOutputStream(outStream);

                 WritableUtils.writeVInt(dataout, dfMap.get(pTerm));
                 dataout.write(byteStream.toByteArray());

                 term.set(pTerm);
                 context.write(term, new BytesWritable(outStream.toByteArray()));
             }

               byteStream.flush();
               dataStream.flush();
               byteStream = new ByteArrayOutputStream();
               dataStream = new DataOutputStream(byteStream);

               prevdocNo = 0;
               gap = currdocNo - prevdocNo;
               WritableUtils.writeVInt(dataStream, gap);
               WritableUtils.writeVInt(dataStream, tf);

             } else if(cTerm.equals(pTerm)) {
                 gap = currdocNo - prevdocNo;
                 WritableUtils.writeVInt(dataStream, gap);
                 WritableUtils.writeVInt(dataStream, tf);
               }
             prevdocNo = currdocNo;
             pTerm = cTerm;

          
         }
         }
       
         @Override
         public void cleanup(Context context) throws IOException,
                  InterruptedException {
                    byteStream.flush();
                    dataStream.flush();
                    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
                    DataOutputStream outdata = new DataOutputStream(outStream);
                    //WritableUtils.writeVInt(outStream, df);
                    outdata.write(byteStream.toByteArray());
                    term.set(cTerm);
                    context.write(term, new BytesWritable(outStream.toByteArray()));
                    dataStream.close();
                    byteStream.close();
         }
       }
              
      

  //private BuildInvertedIndex() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", required = true, usage = "number of reducers")
    int numReducers = 1;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + BuildInvertedIndexCompressed.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info("-reducers: " + args.numReducers);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexCompressed.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStringInt.class);
    job.setMapOutputValueClass(VIntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(BuildInvertedMapper.class);
    job.setReducerClass(BuildInvertedReducer.class);
    job.setPartitionerClass(CompressedPartitioner.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexCompressed(), args);
  }
}
