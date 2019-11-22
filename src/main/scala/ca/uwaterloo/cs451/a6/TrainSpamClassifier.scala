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


class TrainConf(args: Seq[String]) extends ScallopConf(args) {
	mainOptions = Seq(input, model, shuffle)
	val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model path", required = true)
  val shuffle = opt[Boolean](descr = "shuffle", required = false)
  
  verify()
}

object TrainSpamClassifier {
   val log = Logger.getLogger(getClass().getName())

   def main(argv: Array[String]) {
     val args = new TrainConf(argv)

     log.info("Input: " + args.input())
     log.info("Model:" + args.model())
     log.info("Shuffle: " + args.shuffle())

     val conf = new SparkConf().setAppName("Train Spam Classifier")
     val sc = new SparkContext(conf)

     val w = HashMap[Int, Double]()

   // Scores a document based on its list of features.
   def spamminess(features: Array[Int]) : Double = {
     var score = 0d
     features.foreach(f => if (w.contains(f)) score += w(f))
     score
}

     val model = new Path(args.model())
     FileSystem.get(sc.hadoopConfiguration).delete(model, true)

     // This is the main learner:
     val delta = 0.002
     
     var textFile = sc.textFile(args.input())

     //Shuffle
     if (args.shuffle() == true) {
       val random = scala.util.Random
       textFile = textFile
         .map(line => {
           (random.nextInt, line)
         })
         .sortByKey()
         .map(k => {
           k._2
         })
     }

     val trained = textFile.map(line =>{
       val lines = line.split(" ")
		   val docid = lines(0)
			 val isSpam = if (lines(1) == "spam") 1d else 0d
			 val features = lines.slice(2,lines.length).map(_.toInt)
       (0, (docid, isSpam, features))
       })
       .groupByKey(1)
       .flatMap(p => {
	  		p._2.foreach(x => {
        // Update the weights as follows:
        val features = x._3
        val isSpam = x._2
        val score = spamminess(features)
        val prob = 1.0 / (1 + exp(-score))
        features.foreach(f => {
          if (w.contains(f)) {
            w(f) += (isSpam - prob) * delta
            } else {
              w(f) = (isSpam - prob) * delta
           }
           })
         })
       w
       })
       trained.saveAsTextFile(args.model())
   }


}
