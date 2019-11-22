package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._
import org.apache.spark.SparkConf
import scala.math.exp
import collection.mutable.HashMap
import org.apache.hadoop.fs._


class EnsembleConf(args: Seq[String]) extends ScallopConf(args) {
	mainOptions = Seq(input, model, output)
	val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val output = opt[String](descr = "output", required = false)
  val method = opt[String](descr = "method", required = true)
  
  verify()
}

object ApplyEnsembleSpamClassifier {
   val log = Logger.getLogger(getClass().getName())

   def main(argv: Array[String]) {
     val args = new EnsembleConf(argv)

     log.info("Input: " + args.input())
     log.info("Model:" + args.model())
     log.info("output:" + args.output())
     log.info("Method: " + args.method())

     val conf = new SparkConf().setAppName("Apply Spam Classifier")
     val sc = new SparkContext(conf)

    // val w1 = HashMap[Int, Double]()
    // val w2 = HashMap[Int, Double]()
    // val w3 = HashMap[Int, Double]()

   // Scores a document based on its list of features.
 /*  def spamminess(features: Array[Int]) : Double = {
     var score = 0d
     features.foreach(f => if (w.contains(f)) score += w(f))
     score
}*/


     // This is the main learner:
    // val delta = 0.002
     
     var textFile = sc.textFile(args.input())

     //output
     val outputdir = new Path(args.output())
     FileSystem.get(sc.hadoopConfiguration).delete(outputdir, true)

     // Scores a document based on its list of features.
     def spamminess(features: Array[Int], w: scala.collection.Map[Int, Double]) : Double = {
       var score = 0d
       features.foreach(f => if (w.contains(f)) score += w(f))
       score
      }
     
     //Loading the model 1:
     val bc1 = sc.broadcast(sc.textFile(args.model() + "/part-00000")
      .map(l => {
        val lines = l.substring(1, l.length()-1).split(",")
        (lines(0).toInt, lines(1).toDouble)}).collectAsMap())
        //(lines(0).substring(1,lines(0).length).toInt, lines(1).substring(0,lines(1).length-1).toDouble)}).collectAsMap())
      
    
     //val bc1 = sc.broadcast(w1.collectAsMap())

      //Loading the model 2:
     val bc2 = sc.broadcast(sc.textFile(args.model() + "/part-00001")
      .map(l => {
        val lines = l.substring(1, l.length()-1).split(",")
        (lines(0).toInt, lines(1).toDouble)}).collectAsMap())
        //(lines(0).substring(1,lines(0).length).toInt, lines(1).substring(0,lines(1).length-1).toDouble)}).collectAsMap())

     //val bc2 = sc.broadcast(w2.collectAsMap())

     //Loading the model 3:
     val bc3 = sc.broadcast(sc.textFile(args.model() + "/part-00002")
       .map(l => {
         val lines = l.substring(1, l.length()-1).split(",")
         (lines(0).toInt, lines(1).toDouble)}).collectAsMap())
         //(lines(0).substring(1,lines(0).length).toInt, lines(1).substring(0,lines(1).length-1).toDouble)}).collectAsMap())

     //val bc3 = sc.broadcast(w3.collectAsMap())
     
     val method = args.method()     

     //classify text data
     textFile.map(line => {
       val lines = line.split(" ")
       val docid = lines(0)
       val testclas = lines(1)
			 val features = lines.slice(2,lines.length).map(_.toInt)
       val score1 = spamminess(features, bc1.value)
       val score2 = spamminess(features, bc2.value)
       val score3 = spamminess(features, bc3.value)
       var score = 0d
       if(method == "average") {
         score = (score1 + score2 + score3)/3
       } else {
          //var vote = 0
          val vote1 = if (score1 > 0) 1d else -1d
          val vote2 = if (score2 > 0) 1d else -1d
          val vote3 = if (score3 > 0) 1d else -1d
          score = vote1 + vote2 + vote3
       }
       val isSpam = if (score > 0) "spam" else "ham"
       (docid, testclas, score, isSpam)
       })
       .saveAsTextFile(args.output())
   }


}
