package ca.uwaterloo.cs451.a4;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import tl.lin.data.array.ArrayListOfIntsWritable;
import tl.lin.data.array.ArrayListOfFloatsWritable;

import tl.lin.data.map.HMapIF;
import tl.lin.data.map.MapIF;

import java.util.ArrayList;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Iterator;



public class RunPersonalizedPageRankBasic extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(RunPersonalizedPageRankBasic.class);
  
  private static final String SRC_NODES = "sourceNodes";

  private static enum PgRank {
    nodes, edges, massMessages, massMessagesSaved, massMessagesReceived, missingStructure
  };


  // Mapper, no in-mapper combining.
  private static class MapClass extends
      Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {

    // The neighbor to which we're sending messages.
    private static final IntWritable neighbor = new IntWritable();

    // Contents of the messages: partial PageRank mass.
    private static final PageRankNode intermediateMass = new PageRankNode();

    // For passing along node structure.
    private static final PageRankNode intermediateStructure = new PageRankNode();

    private static final ArrayList<Integer> sources = new ArrayList<Integer>();

    @Override
    public void setup(Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode>.Context context) {
      //Get the source node from the context
      String[] srcs = context.getConfiguration().getStrings(SRC_NODES, "");
      if(srcs.length == 0){
        throw new RuntimeException("Please provide the source list. Cannot be empty!");
      }
      String src;
      for(int i = 0; i < srcs.length; i++){
        src = srcs[i];
        sources.add(Integer.valueOf(src));

       }

    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context)
        throws IOException, InterruptedException {
      // Pass along node structure.
      intermediateStructure.setNodeId(node.getNodeId());
      intermediateStructure.setType(PageRankNode.Type.Structure);
      intermediateStructure.setAdjacencyList(node.getAdjacencyList());

      context.write(nid, intermediateStructure);

      int massMessages = 0;

      // Distribute PageRank mass to neighbors (along outgoing edges).
      if (node.getAdjacencyList().size() > 0) {
        // Each neighbor gets an equal share of PageRank mass.
        ArrayListOfIntsWritable list = node.getAdjacencyList();
        ArrayListOfFloatsWritable pgRankList = new ArrayListOfFloatsWritable();
        
        for (int i = 0; i < sources.size(); i++) {
          pgRankList.add(node.getPageRank().get(i) - (float) StrictMath.log(list.size()));
        }

        context.getCounter(PgRank.edges).increment(list.size());

        // Iterate over neighbors.
        for (int i = 0; i < list.size(); i++) {
          neighbor.set(list.get(i));
          intermediateMass.setNodeId(list.get(i));
          intermediateMass.setType(PageRankNode.Type.Mass);
          //intermediateMass.setPageRank(mass);
          intermediateMass.setPageRank(pgRankList);

          // Emit messages with PageRank mass to neighbors.
          context.write(neighbor, intermediateMass);
          massMessages++;
        }
      }

      // Bookkeeping.
      context.getCounter(PgRank.nodes).increment(1);
      context.getCounter(PgRank.massMessages).increment(massMessages);
    }
  

      @Override
      public void cleanup(Context context) throws IOException {
        sources.clear();
    }
  }

  // Combiner is used to sums partial PageRank contributions and passes node structure along
  private static class CombinerClass extends
      Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {

        private static final PageRankNode intermediateMass = new PageRankNode();
        private static final ArrayList<Integer> sources = new ArrayList<Integer>();
        
        @Override
        public void setup(Context context) throws IOException {
          String[] srcs = context.getConfiguration().getStrings(SRC_NODES, "");
          if(srcs.length == 0){
            throw new RuntimeException("Please provide the source list..Cannot be empty");
          }
          
          String src;
          for(int i =0; i< srcs.length; i++) {
            src = srcs[i];
            sources.add(Integer.valueOf(src));
      }
    }


    @Override
    public void reduce(IntWritable nid, Iterable<PageRankNode> values, Context context)
        throws IOException, InterruptedException {
      int massMessages = 0;

      ArrayListOfFloatsWritable mass = new ArrayListOfFloatsWritable();

      // Remember, PageRank mass is stored as a log prob.
      //float[] mass = new float[sources.size()];
      for (int i = 0; i < sources.size(); i++) {
        mass.add(Float.NEGATIVE_INFINITY);
      }

      for (PageRankNode n : values) {
        if (n.getType() == PageRankNode.Type.Structure) {
          // Simply pass along node structure.
          context.write(nid, n);
        } else {
          // Accumulate PageRank mass contributions.
          for(int i =0; i < sources.size(); i++) {
          mass.set(i, sumLogProbs(mass.get(i), n.getPageRank().get(i)));
          }
          massMessages++;
        }
      }

      // Emit aggregated results.
      if (massMessages > 0) {
        intermediateMass.setNodeId(nid.get());
        intermediateMass.setType(PageRankNode.Type.Mass);
        intermediateMass.setPageRank(new ArrayListOfFloatsWritable(mass));

        context.write(nid, intermediateMass);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException {
      sources.clear();
    }
      }

  

  // Reduce: sums incoming PageRank contributions, rewrite graph structure.
  private static class ReduceClass extends
      Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    // For keeping track of PageRank mass encountered, so we can compute missing PageRank mass lost
    // through dangling nodes.
    // private float totalMass = Float.NEGATIVE_INFINITY;
    

    private static final ArrayList<Integer> sources = new ArrayList<Integer>();
    private static final ArrayList<Float> totalMass = new ArrayList<Float>();
    
    @Override
    public void setup(Context context) throws IOException {
      String[] srcs = context.getConfiguration().getStrings(SRC_NODES, "");
      String src;
      for(int i = 0; i<srcs.length; i++){
        src = srcs[i];
        sources.add(Integer.valueOf(src));
        totalMass.add(Float.NEGATIVE_INFINITY);
      }
    }

    @Override
    public void reduce(IntWritable nid, Iterable<PageRankNode> iterable, Context context)
        throws IOException, InterruptedException {
      Iterator<PageRankNode> values = iterable.iterator();

      // Create the node structure that we're going to assemble back together from shuffled pieces.
      PageRankNode node = new PageRankNode();

      node.setType(PageRankNode.Type.Complete);
      node.setNodeId(nid.get());

      int massMessagesReceived = 0;
      int structureReceived = 0;

      ArrayListOfFloatsWritable mass = new ArrayListOfFloatsWritable();

      for (int i = 0; i < sources.size(); i++) {
        mass.add(Float.NEGATIVE_INFINITY);
      }
      while (values.hasNext()) {
        PageRankNode n = values.next();

        if (n.getType().equals(PageRankNode.Type.Structure)) {
          // This is the structure; update accordingly.
          ArrayListOfIntsWritable list = n.getAdjacencyList();
          structureReceived++;

          node.setAdjacencyList(list);
        } else {
          // This is a message that contains PageRank mass; accumulate.
          for (int i = 0; i < sources.size(); i++) {
          mass.set(i, sumLogProbs(mass.get(i), n.getPageRank().get(i)));
          }
          massMessagesReceived++;
        }
      }

      // Update the final accumulated PageRank mass.
      node.setPageRank(mass);
      context.getCounter(PgRank.massMessagesReceived).increment(massMessagesReceived);

      // Error checking.
      if (structureReceived == 1) {
        // Everything checks out, emit final node structure with updated PageRank value.
        context.write(nid, node);

        // Keep track of total PageRank mass.
        for (int i = 0; i < sources.size(); i++) {
          totalMass.set(i, sumLogProbs(totalMass.get(i), mass.get(i)));
        }
      } else if (structureReceived == 0) {
        // We get into this situation if there exists an edge pointing to a node which has no
        // corresponding node structure (i.e., PageRank mass was passed to a non-existent node)...
        // log and count but move on.
        context.getCounter(PgRank.missingStructure).increment(1);
        LOG.warn("No structure received for nodeid: " + nid.get() + " mass: "
            + massMessagesReceived);
        // It's important to note that we don't add the PageRank mass to total... if PageRank mass
        // was sent to a non-existent node, it should simply vanish.
      } else {
        // This shouldn't happen!
        throw new RuntimeException("Multiple structure received for nodeid: " + nid.get()
            + " mass: " + massMessagesReceived + " struct: " + structureReceived);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      String taskId = conf.get("mapred.task.id");
      String path = conf.get("PageRankMassPath");

      Preconditions.checkNotNull(taskId);
      Preconditions.checkNotNull(path);

      // Write to a file the amount of PageRank mass we've seen in this reducer.
      FileSystem fs = FileSystem.get(context.getConfiguration());
      FSDataOutputStream out = fs.create(new Path(path + "/" + taskId), false);
      for (int i = 0; i < sources.size(); i++) {
        out.writeFloat(totalMass.get(i));
      }
      out.close();

      sources.clear();
      totalMass.clear();
    }
  }

  // Mapper that distributes the missing PageRank mass (lost at the dangling nodes) and takes care
  // of the random jump factor.
  private static class MapPageRankMassDistributionClass extends
      Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    //private float missingMass = 0.0f;
    private int nodeCnt = 0;
    private static final ArrayList<Integer> sources = new ArrayList<Integer>();
    private static final ArrayList<Float> missingMass = new ArrayList<Float>();

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();

      String[] missingList = conf.getStrings("MissingMass", "");
      String m;
      for(int i = 0; i<missingList.length; i++){
        m = missingList[i];
        missingMass.add(Float.valueOf(m));
      }
      nodeCnt = conf.getInt("NodeCount", 0);
      String[] srcs = conf.getStrings(SRC_NODES, "");
      String src;
      for(int i =0; i< srcs.length; i++){
        src = srcs[i];
        sources.add(Integer.valueOf(src));
      }
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context)
        throws IOException, InterruptedException {

        ArrayListOfFloatsWritable p = node.getPageRank();

        for (int i = 0; i < sources.size(); i++) {
          if (Integer.valueOf(sources.get(i)) == nid.get()) {
            float jump = (float) Math.log(ALPHA);
            float link = (float) Math.log(1.0f - ALPHA)
              + sumLogProbs(p.get(i), (float) Math.log(missingMass.get(i)));

            p.set(i, sumLogProbs(jump, link));
            } else {
              p.set(i, p.get(i) + (float) Math.log(1.0f - ALPHA));
            }
        }


      //p.get(i) = sumLogProbs(jump, link);
      node.setPageRank(p);

      context.write(nid, node);
    }
                                                  
    @Override
    public void cleanup(Context context) throws IOException {
      sources.clear();
      missingMass.clear();
    }
  }

  // Random jump factor.
  private static float ALPHA = 0.15f;
  private static NumberFormat formatter = new DecimalFormat("0000");

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new RunPersonalizedPageRankBasic(), args);
  }

  public RunPersonalizedPageRankBasic() {}

  private static final String BASE = "base";
  private static final String NUM_NODES = "numNodes";
  private static final String START = "start";
  private static final String END = "end";
  //private static final String COMBINER = "useCombiner";
  //private static final String INMAPPER_COMBINER = "useInMapperCombiner";
  //private static final String RANGE = "range";
  private static final String SOURCES = "sources";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

   // options.addOption(new Option(COMBINER, "use combiner"));
   // options.addOption(new Option(INMAPPER_COMBINER, "user in-mapper combiner"));
   // options.addOption(new Option(RANGE, "use range partitioner"));

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("base path").create(BASE));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("start iteration").create(START));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("end iteration").create(END));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of nodes").create(NUM_NODES));
    options.addOption(OptionBuilder.withArgName("sources").hasArg()
        .withDescription("source nodes").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(BASE) || !cmdline.hasOption(START) ||
        !cmdline.hasOption(END) || !cmdline.hasOption(NUM_NODES) ||
        !cmdline.hasOption(SOURCES)) {
       
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String basePath = cmdline.getOptionValue(BASE);
    int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
    int s = Integer.parseInt(cmdline.getOptionValue(START));
    int e = Integer.parseInt(cmdline.getOptionValue(END));
   // boolean useCombiner = cmdline.hasOption(COMBINER);
   // boolean useInmapCombiner = cmdline.hasOption(INMAPPER_COMBINER);
   // boolean useRange = cmdline.hasOption(RANGE);
    String sources = cmdline.getOptionValue(SOURCES);

    LOG.info("Tool name: RunPageRank");
    LOG.info(" - base path: " + basePath);
    LOG.info(" - num nodes: " + n);
    LOG.info(" - start iteration: " + s);
    LOG.info(" - end iteration: " + e);
    //LOG.info(" - use combiner: " + useCombiner);
    //LOG.info(" - use in-mapper combiner: " + useInmapCombiner);
    //LOG.info(" - user range partitioner: " + useRange);
     
    LOG.info(" - sources: " + sources);
    // Iterate PageRank.
    for (int i = s; i < e; i++) {
      iteratePageRank(i, i + 1, basePath, n, sources);
    }

    return 0;
  }

  // Run each iteration.
  private void iteratePageRank(int i, int j, String basePath, int numNodes, String sources) throws Exception {
    // Each iteration consists of two phases (two MapReduce jobs).

    // Job 1: distribute PageRank mass along outgoing edges.
    //float mass = phase1(i, j, basePath, numNodes, sources);
    ArrayList<Float> mass = phase1(i, j, basePath, numNodes, sources);;
    // Find out how much PageRank mass got lost at the dangling nodes.
    //float missing = 1.0f - (float) StrictMath.exp(mass);
    
    ArrayList<Float> missing = new ArrayList<Float>();

    for (int l = 0; l < mass.size(); l++) {
      float miss = 1.0f - (float) StrictMath.exp(mass.get(l));
      missing.add(miss < 0.0f ? 0.0f : miss);
    }

    // Job 2: distribute missing mass, take care of random jump factor.
    phase2(i, j, missing, basePath, numNodes, sources);
  }

  private ArrayList<Float> phase1(int i, int j, String basePath, int numNodes, String sources)
      throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName("PageRank:Basic:iteration" + j + ":Phase1");
    job.setJarByClass(RunPersonalizedPageRankBasic.class);

    String in = basePath + "/iter" + formatter.format(i);
    String out = basePath + "/iter" + formatter.format(j) + "t";
    String outm = out + "-mass";

    // We need to actually count the number of part files to get the number of partitions (because
    // the directory might contain _log).
    int numPartitions = 0;
    for (FileStatus s : FileSystem.get(getConf()).listStatus(new Path(in))) {
      if (s.getPath().getName().contains("part-"))
        numPartitions++;
    }

    LOG.info("PageRank: iteration " + j + ": Phase1");
    LOG.info(" - input: " + in);
    LOG.info(" - output: " + out);
    LOG.info(" - nodeCnt: " + numNodes);
    //LOG.info(" - useCombiner: " + useCombiner);
    //LOG.info(" - useInmapCombiner: " + useInMapperCombiner);
    LOG.info(" - sources: " + sources);
    LOG.info("computed number of partitions: " + numPartitions);
    
    int numReduceTasks = numPartitions;

    job.getConfiguration().setInt("NodeCount", numNodes);
    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
    //job.getConfiguration().set("mapred.child.java.opts", "-Xmx2048m");
    job.getConfiguration().set("PageRankMassPath", outm);

    job.getConfiguration().setStrings(SRC_NODES, sources);

    job.setNumReduceTasks(numReduceTasks);

    FileInputFormat.setInputPaths(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PageRankNode.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PageRankNode.class);

    job.setMapperClass(MapClass.class);

    //if (useCombiner) {
    job.setCombinerClass(CombinerClass.class);
   // }

    job.setReducerClass(ReduceClass.class);

    FileSystem.get(getConf()).delete(new Path(out), true);
    FileSystem.get(getConf()).delete(new Path(outm), true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    ArrayList<Float> mass = new ArrayList<Float>();
    String[] srcs = sources.split(",");
    for (int l = 0; l < srcs.length; l++) {
      mass.add(Float.NEGATIVE_INFINITY);
    }

    //float mass = Float.NEGATIVE_INFINITY;
    FileSystem fs = FileSystem.get(getConf());
    for (FileStatus f : fs.listStatus(new Path(outm))) {
      FSDataInputStream fin = fs.open(f.getPath());
      for (int l = 0; l < srcs.length; l++) {
        mass.set(l, sumLogProbs(mass.get(l), fin.readFloat()));
      }

      fin.close();
    }

    return mass;
  }

  private void phase2(int i, int j, ArrayList<Float> missing, String basePath, int numNodes, String sources) throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName("PageRank:Basic:iteration" + j + ":Phase2");
    job.setJarByClass(RunPersonalizedPageRankBasic.class);

    //For missing arraylist
    String missingList = "";
    for (int l = 0; l < missing.size(); l++) {
      missingList += String.valueOf(missing.get(l));
      if (l < missing.size() - 1) {
        missingList += ",";
      }
    }

    LOG.info("missing PageRank mass: " + missingList);
    LOG.info("number of nodes: " + numNodes);

    String in = basePath + "/iter" + formatter.format(j) + "t";
    String out = basePath + "/iter" + formatter.format(j);

    LOG.info("PageRank: iteration " + j + ": Phase2");
    LOG.info(" - input: " + in);
    LOG.info(" - output: " + out);
    LOG.info(" - sources: " + sources);

    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
    job.getConfiguration().setStrings("MissingMass",missingList);
    job.getConfiguration().setInt("NodeCount", numNodes);
    job.getConfiguration().setStrings(SRC_NODES, sources);

    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PageRankNode.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PageRankNode.class);

    job.setMapperClass(MapPageRankMassDistributionClass.class);

    FileSystem.get(getConf()).delete(new Path(out), true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
  }

  // Adds two log probs.
  private static float sumLogProbs(float a, float b) {
    if (a == Float.NEGATIVE_INFINITY)
      return b;

    if (b == Float.NEGATIVE_INFINITY)
      return a;

    if (a < b) {
      return (float) (b + StrictMath.log1p(StrictMath.exp(a - b)));
    }

    return (float) (a + StrictMath.log1p(StrictMath.exp(b - a)));
  }
}
