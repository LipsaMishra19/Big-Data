package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer
//import org.apache.spark.Partitioner
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class PMIStripesConf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(10))
  // To run on DataSci Cluster:Added --num-executors and --executor-cores options
  val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
  val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
  verify()
}

object StripesPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new PMIStripesConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Stripes PMI")
    val sc = new SparkContext(conf)
    val threshold = args.threshold()

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    // val marginal = 0.0
    val textFile = sc.textFile(args.input(), args.reducers())
    val countlines = textFile.count()
    val words = textFile
                      .flatMap(line => {
                        val tokens = tokenize(line)
                         // if (tokens.length > 1) {
                        if (tokens.length > 0) {
                          tokens.take(40).distinct
                        }
                        else List()
                      })
                      // if (tokens.length > 1) { val bigrams = tokens.sliding(2).map(p1 => (p1.head, p1.tail.mkString)).toList
                      // val bigmarginal = tokens.init.map(m => (m, "*")).toList
                      // bigrams ++ bigmarginal
                      .map(word => (word, 1))
                      .reduceByKey(_ + _)
                      
    val broadcastVar = sc.broadcast(words.collectAsMap())

    textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        // var wordPair = scala.collection.mutable.ListBuffer[(String, Map[String, Int])]()
        val word1 = tokens.take(40).distinct
        if (word1.length > 1) {
          var wordstripes = scala.collection.mutable.ListBuffer[(String, Map[String, Int])]()
          var i = 0
          var j = 0
          for (i <- 0 to word1.length - 1) {
            var wordmap = scala.collection.mutable.Map[String, Int]()
            for (j <- 0 to word1.length - 1) {
              if ((i != j)) {
                //var pair = List[tokens(i), tokens(j)]
                var countk = wordmap.getOrElse(word1(j), 0) + 1
                //wordmap.map{p => p._2 = count1}
                wordmap(word1(j)) = countk
                //wordPair = wordPair :+ (i,j)
              }
            }
            var stripe : (String, Map[String, Int]) = (word1(i), wordmap.toMap)
            wordstripes += stripe
          }
          wordstripes.toList
        } else List()
      })
      .reduceByKey((map1, map2) => {
        map1 ++ map2.map{ case (k, v) => k -> (v + map1.getOrElse(k, 0)) }
        // .groupByKey()
      })
        .map(pmistripe => {  
         val x = pmistripe._1
         // var pmi_map = scala.collection.mutable.Map[String,(Double,Double)]()
         // val cnt = pmistripe._2.filter((m) => m._2)
         // if (cnt >= threshold) {
         // val probXY = pmistripe._2.toDouble
         // val probX = broadcastVar.value(pmistripe._1).toDouble
         // pmistripe._2.foreach(x => {
         // pmi_map(x._1) = (pmiXY,x._2)
       // val probXY = v.toFloat
       // val probX = broadcastVar.value(x)
      //val probY = broadcastVar.value(k)

        //val pmiXY = Math.log10((probXY * count.toFloat) / (probX * probY))
       // }}
       // (p._1, (pmi, p._2.toInt))
        ("" + "(" + pmistripe._1, pmistripe._2.filter((n) => n._2 >= threshold).map { case (k, v) => {
          k + "=(" + Math.log10((v.toFloat * countlines.toFloat) / (broadcastVar.value(x) * broadcastVar.value(k))) + "," + v + ")" +")" 
        }})
      })
        .filter((p) => p._2.size > 0)
        // .sortByKey()
        .map(p => p._1 + ","+ " {" + (p._2 mkString ", ") + "}")
      .saveAsTextFile(args.output())
  }
}
