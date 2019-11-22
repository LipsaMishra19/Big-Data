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


class ApplySpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
	mainOptions = Seq(input, model, output)
	val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val output = opt[String](descr = "output", required = false)
  
  verify()
}

object ApplySpamClassifier {
   val log = Logger.getLogger(getClass().getName())

   def main(argv: Array[String]) {
     val args = new ApplySpamClassifierConf(argv)

     log.info("Input: " + args.input())
    // log.info("Model:" + args.model())
     log.info("Output:" + args.output())
     log.info("Model:" + args.model())

     val conf = new SparkConf().setAppName("Apply Spam Classifier")
     val sc = new SparkContext(conf)

    // val w = HashMap[Int, Double]()

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
     
     //Loading the model 
     val bc1 = sc.broadcast(sc.textFile(args.model() + "/part-00000")
      .map(l => {
        val lines = l.substring(1, l.length()-1).split(",")
        (lines(0).toInt, lines(1).toDouble)}).collectAsMap())

        //(lines(0).substring(1,lines(0).length).toInt, lines(1).substring(0,lines(1).length-1).toDouble)}).collectAsMap())
      
    
      //val bc = sc.broadcast(w1.collectAsMap())

      def spamminess(features: Array[Int])  : Double = {
			var score = 0d
			features.foreach(f => if (bc1.value.contains(f)) score += bc1.value(f))
			score
		  } 

     //classify text data
     val test = textFile.map(line =>{
       val lines = line.split(" ")
       val docid = lines(0)
       val testclas = lines(1)
			 val features = lines.slice(2,lines.length).map(_.toInt)
       val score = spamminess(features)
       val isSpam = if (score > 0) "spam" else "ham"
       (docid, testclas, score, isSpam)
       })
       .saveAsTextFile(args.output())
   }


}
