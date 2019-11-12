package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._
import org.apache.spark.SparkConf


class ConfQ5(args: Seq[String]) extends ScallopConf(args) {
	mainOptions = Seq(input, date, text, parquet)
	val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = false)
  val text = opt[Boolean](descr = "text", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "parquet", required = false, default = Some(false))
  verify()
}


object Q5 extends {
   val log = Logger.getLogger(getClass().getName())
   
   def main(argv: Array[String]) {
     val args = new ConfQ5(argv)

     log.info("Input: " + args.input())
    // log.info("Date:" + args.date())

     val conf = new SparkConf().setAppName("Q5")
     val sc = new SparkContext(conf)

     //val date = args.date()

     if (args.text()) {
       val nation = sc.textFile(args.input() + "/nation.tbl")
        .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1)))
       val broadcastNation = sc.broadcast(nation.collectAsMap())
       
      // val nationCA = broadcastNation.value("CANADA")
       //val nationUS = broadcastNation.value("UNITED STATES")

       val customer = sc.textFile(args.input() + "/customer.tbl")
       .map(line => (line.split("\\|")(0), line.split("\\|")(3).toInt))
       val broadcastCust = sc.broadcast(customer.collectAsMap())

       /*val orders = sc.textFile(args.input() + "/orders.tbl")
         .map(line => (line.split("\\|")(0), line.split("\\|")(1)))
         .filter(p => {
             val key = broadcastCust.value(p._2)
             key == 3 || key == 24
        })*/

        val orders = sc.textFile(args.input() + "/orders.tbl")
        .filter(line => {
          val lines = line.split("\\|")
          val key = broadcastCust.value(lines(1))
          key == 3 || key == 24
        })
        .map(line => {
          val lines = line.split("\\|")
          (lines(0), broadcastCust.value(lines(1)))
        })



       val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
        //.filter(line => line.split("\\|")(10).contains(date))
        .map(line => (line.split("\\|")(0), line.split("\\|")(10).substring(0, 7)))
      
       lineitem.cogroup(orders)
       .filter(p => p._2._2.iterator.hasNext)
       .flatMap(p => {
         val key = p._2._2
         p._2._1.map(k => ((key, k), 1))})
       .reduceByKey(_ + _)
       .sortByKey()
       .collect()
       .foreach(p => println(p._1._1.mkString, p._1._2, p._2))

     } else if (args.parquet()) {

         val sparkSession = SparkSession.builder.getOrCreate
         
         val nationDF = sparkSession.read.parquet(args.input() + "/nation")
         val nationRDD = nationDF.rdd
         val nation = nationRDD
           .map(line => (line.getInt(0), line.getString(1)))
         val broadcastNation = sc.broadcast(nation.collectAsMap())
         //val nationVal = broadcastNation.value
         //val nationCA = broadcastNation.value("CANADA")
         //val nationUS = broadcastNation.value("UNITED STATES")

        val customerDF = sparkSession.read.parquet(args.input() + "/customer")
        val customerRDD = customerDF.rdd
        val customer = customerRDD
        .map(line => (line.getInt(0), line.getInt(3)))
        val broadcastCust = sc.broadcast(customer.collectAsMap())
        //val supplierVal = broadcastSupplier.value

        val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
        val ordersRDD = ordersDF.rdd
        val orders = ordersRDD
          //.map(line => (line.getInt(0), line.getInt(1)))
          .filter(line => {
          val key = broadcastCust.value(line.getInt(1))
          key == 3 || key == 24
        })
        .map(line => (line.getInt(0), broadcastCust.value(line.getInt(1))))


        val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
        val lineitemRDD = lineitemDF.rdd

        val lineitem = lineitemRDD
        //.filter(line => line.getString(10))
        .map(line => {
          (line.getInt(0), line.getString(10).substring(0,7))
        })

        lineitem.cogroup(orders)
        .filter(p => p._2._2.iterator.hasNext)
        .flatMap(p => {
          p._2._1.map(k => ((p._2._2,k),1))})
        .reduceByKey(_ + _)
        .sortByKey()
        .collect()
        .foreach(p => println(p._1._1.mkString, p._1._2, p._2))
        
        }
       }
     }
   
