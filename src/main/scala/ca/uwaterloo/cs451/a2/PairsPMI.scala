package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer
//import org.apache.spark.Partitioner

import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j._
import org.rogach.scallop._


class PMIPairsConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(10))
  // To run on DataSci Cluster:Added --num-executors and --executor-cores options
  val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
  val executorCores = opt[Int](descr = "Executors cores", required = false, default = Some(1))
  verify()
}

object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())
  
 // class MyMapper extends Mapper[LongWritable, Text, PairOfStrings, FloatWritable] {
 // override def map(key: LongWritable, value: Text,
 // context: Mapper[LongWritable, Text, PairOfStrings, FloatWritable]#Context) = {

  def main(argv: Array[String]) {
    val args = new PMIPairsConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Relative Frequency Pairs")
    val sc = new SparkContext(conf)
    val threshold = args.threshold() 

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    
   // val marginal = 0.0
    val textFile = sc.textFile(args.input(), args.reducers())
    val count = textFile.count()
    val words = textFile.flatMap(line => {
     // if (tokens.length > 1) {
        val tokens = tokenize(line)
        if (tokens.length > 1) { 
          tokens.take(40).distinct
        } else List()

       // if (tokens.length > 1) {
       // val bigrams = tokens.sliding(2).map(p1 => (p1.head, p1.tail.mkString)).toList
       // val bigmarginal = tokens.init.map(m => (m, "*")).toList
       // bigrams ++ bigmarginal
      })   //else List()
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      //.sortByKey()
      //.repartitionAndSortWithinPartitions(new BigramPartitioner(args.reducers()))
      val broadcastVar = sc.broadcast(words.collectAsMap())
    
      val newWord = textFile.flatMap(line => {
        var wordPair = List[(String,String)]()
        val tokens = tokenize(line)
        if (tokens.length > 1) { 
          val setTokens = tokens.take(40).distinct.toSet
          for (i <- setTokens) {
            for (j<-setTokens) {
              if(i!=j){
                //var pair = List[tokens(i), tokens(j)]
                wordPair = wordPair :+ (i,j)
            }
          }
        }
        wordPair.toList
      } else List()
    })
      .map(pair => (pair,1))
      .reduceByKey(_+_)
      .filter((p) => p._2 >= threshold)
      .map(pmipair => {
        val cnt = pmipair._2
        if (cnt > threshold) {
          val probXY = pmipair._2.toFloat
          val probX = broadcastVar.value(pmipair._1._1)
          val probY = broadcastVar.value(pmipair._1._2)

          val pmiXY = Math.log10((probXY * count.toFloat) / (probX * probY))
          (pmipair._1,(pmiXY, pmipair._2.toInt))
        }
      })
      .saveAsTextFile(args.output())
  }
}

