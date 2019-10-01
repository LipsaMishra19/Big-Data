package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer
//import org.apache.spark.Partitioner

import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j._
import org.rogach.scallop._


class BigramRelFreqStripesConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  // To run on DataSci Cluster:Added --num-executors and --executor-cores options
  val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
  val executorCores = opt[Int](descr = "Executors cores", required = false, default = Some(1))
  verify()
}

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())
  
 // class MyMapper extends Mapper[LongWritable, Text, PairOfStrings, FloatWritable] {
 // override def map(key: LongWritable, value: Text,
 // context: Mapper[LongWritable, Text, PairOfStrings, FloatWritable]#Context) = {

  def main(argv: Array[String]) {
    val args = new BigramRelFreqStripesConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Relative Frequency Stripes")
    val sc = new SparkContext(conf)
        
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    
   // val marginal = 0.0
    val textFile = sc.textFile(args.input(), args.reducers())
    textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) {
          tokens.sliding(2).map(p1 => {(p1.head, Map(p1.last -> 1.0))
         // val bigmarginal = tokens.init.map(m => (m, "*")).toList
         // bigrams ++ bigmarginal
        })
        } else List()
      })
      .reduceByKey((stripes1, stripes2) => {
        stripes1 ++ stripes2.map{ case (key,value) => key -> (value + stripes1.getOrElse(key, 0.0))
        }
        })
        .sortByKey()
        .map(stripes => { 
          val sum = stripes._2.foldLeft(0.0)(_+_._2)
          (stripes._1, stripes._2 map {case (key, value) => key + "=" + (value / sum)})
          })
          .map(p => "(" + p._1 + "," + " {" + (p._2 mkString ", ") + "}")
          // .repartitionAndSortWithinPartitions(new BigramPartitioner(args.reducers()))
          .saveAsTextFile(args.output())

  }
}

