package ca.uwaterloo.cs451.a5
 
import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._
import org.apache.spark.SparkConf
import org.apache.hadoop.fs._
 
 
class ConfQ2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
	val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean](descr = "text", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "parquet", required = false, default = Some(false))
  verify()
  }
  
object Q2 {
  val log = Logger.getLogger(getClass().getName())
 
  def main(argv: Array[String]): Unit = {
    val args = new ConfQ2(argv)
  
    log.info("Input: " + args.input()) 
    //log.info("Date:" + args.date()) 
  
    val conf = new SparkConf().setAppName("Q2") 
    val sc = new SparkContext(conf) 
  
    val date = args.date() 
    val Order = sc.textFile(args.input() + "/orders.tbl")
    val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
  
    if (args.text()) { 
      val Orderkey = lineitem      
       // .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(10).contains(date)))
        .filter(line => {
          line.split("\\|")(10).contains(date)
        })
          .map(line => {
          (line.split("\\|")(0),line.split("\\|")(10))})

      val clerk = Order
        .map(line => (line.split("\\|")(0), line.split("\\|")(6)))
  
      Orderkey.cogroup(clerk) 
      .filter(p => p._2._1.iterator.hasNext)
      .map(p => (p._2._2.iterator.next(), p._1.toLong))
      .sortBy(_._2)
      .take(20)
      .foreach(println)      
  
       } else if (args.parquet()) { 
         val sparkSession = SparkSession.builder.getOrCreate 
 
         val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem") 
         val lineitemRDD = lineitemDF.rdd 
         
         val orderDF = sparkSession.read.parquet(args.input() + "/orders")
         val orderRDD = orderDF.rdd
         .map(line => (line(0), line(6)))

         lineitemRDD 
         .filter(line => { 
          line.getString(10).contains(date)
         })
          .map(line => (line(0), 1))
          .cogroup(orderRDD)
          .filter(p => p._2._1.iterator.hasNext)
          .map(p => (p._2._2.iterator.next(), p._1.toString.toLong))
          .sortBy(_._2)
          .take(20)
          .foreach(println)
           } 
       } 
     } 
   

 
