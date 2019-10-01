package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer
import org.apache.spark.Partitioner

import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j._
import org.rogach.scallop._


class BigramRelativeFrequencyConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))

  // To run on DataSci Cluster:Added --num-executors and --executor-cores options
  val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
  val executorCores = opt[Int](descr = "Executors cores", required = false, default = Some(1))
  verify()
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())
  
 // class MyMapper extends Mapper[LongWritable, Text, PairOfStrings, FloatWritable] {
 // override def map(key: LongWritable, value: Text,
 // context: Mapper[LongWritable, Text, PairOfStrings, FloatWritable]#Context) = {

  def main(argv: Array[String]) {
    val args = new BigramRelativeFrequencyConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Relative Frequency Pairs")
    val sc = new SparkContext(conf)
        
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    
   // val marginal = 0.0
    val textFile = sc.textFile(args.input(), args.reducers())
    textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) {
          val bigrams = tokens.sliding(2).map(p1 => (p1.head, p1.tail.mkString)).toList
          val bigmarginal = tokens.init.map(m => (m, "*")).toList
          bigrams ++ bigmarginal
        } else List()
      })
      .map(bigrams => (bigrams, 1))
      .reduceByKey(_ + _)
      .sortByKey()
      .repartitionAndSortWithinPartitions(new BigramPartitioner(args.reducers()))
      .mapPartitions(t1 => {
        var marginal = 0.0
        t1.map(p2 => {
            //val marginal = 0.0
            p2._1 match {
              case (_, "*") => {
                marginal = p2._2
                (p2._1, p2._2)
            }
            case (_, _) => (p2._1, p2._2 / marginal)
            }
            })
      })
      .saveAsTextFile(args.output())

  }
}

class BigramPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions :  Int  = numParts
  override def getPartition(key: Any): Int = key match {
    case _ => 0
    case (k1,k2) => (k1.hashCode() & Integer.MAX_VALUE) % numParts
  }
 // override def equals(other: Any): Boolean = other match {
  override def hashCode: Int = numParts
}
