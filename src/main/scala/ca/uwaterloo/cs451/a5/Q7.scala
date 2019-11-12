package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._
import org.apache.spark.SparkConf


class ConfQ7(args: Seq[String]) extends ScallopConf(args) {
	mainOptions = Seq(input, date, text, parquet)
	val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean](descr = "text", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "parquet", required = false, default = Some(false))
  verify()
}


object Q7 extends {
   val log = Logger.getLogger(getClass().getName())
   
   def main(argv: Array[String]) {
     val args = new ConfQ7(argv)

     log.info("Input: " + args.input())
     log.info("Date:" + args.date())

     val conf = new SparkConf().setAppName("Q7")
     val sc = new SparkContext(conf)

     val date = args.date()

     if (args.text()) {
     /*  val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
       val part = sc.textFile(args.input() + "/part.tbl")
       val Supplier = sc.textFile(args.input() + "/supplier.tbl")*/
       
      val customer = sc.textFile(args.input() + "/customer.tbl")
        .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1)))
      val broadcastCust = sc.broadcast(customer.collectAsMap())
      val custVal = broadcastCust.value

      val orders = sc.textFile(args.input() + "/orders.tbl")
        .filter(line => {
          (line.split("\\|")(4) < date)
        })
          .map(line => {
            val lines = line.split("\\|")
            val custname = custVal(lines(1).toInt)
            val orderkey = lines(0) 
            val orderdate = lines(4)
            val shippriority = lines(5)
            (orderkey, (custname, orderdate, shippriority))
        })

      /*part
     .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1)))
       val partBroadcast = sc.broadcast(part.collectAsMap())

       Supplier
       .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1)))
       val suppBroadcast = sc.broadcast(Supplier.collectAsMap())
*/
       val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
       .filter(line => 
           line.split("\\|")(10) > date)
       .map(line => {
            val lines = line.split("\\|")
            val price = lines(5).toDouble
            val disc = lines(6).toDouble
            val revenue = price * (1 - disc)
            (lines(0), revenue)
        })
        .reduceByKey(_+_)

        lineitem.cogroup(orders)
        .filter(p => p._2._1.iterator.hasNext && p._2._2.iterator.hasNext)
        .map(p => {
         val item = p._2._2.iterator.next()
         val custname = item._1
         val orderkey = p._1
         val orderdate = item._2
         val shippriority = item._3
         val revenue = p._2._1.iterator.next()
         (revenue, (custname, orderkey, orderdate, shippriority))})
            .sortByKey(false)
            .take(10)
            .map(p => (p._2._1, p._2._2, p._1, p._2._3, p._2._4))
            .foreach(println)
        }

     else if (args.parquet()) {

         val sparkSession = SparkSession.builder.getOrCreate
         
         val custDF = sparkSession.read.parquet(args.input() + "/customer")
         val custRDD = custDF.rdd
         val customer = custRDD
        .map(line => (line.getInt(0), line.getString(1)))
         val broadcastCust = sc.broadcast(customer.collectAsMap())
         val custVal = broadcastCust.value

        val orderDF = sparkSession.read.parquet(args.input() + "/orders")
        val orderRDD = orderDF.rdd
        val orders = orderRDD
        .filter(line => (line.getString(4) < date))
        .map(line => {
          val orderKey = line.getInt(0)
          val custName = broadcastCust.value(line.getInt(1))
          val orderDate = line.getString(4)
          val shipPriority = line.getString(5)
          (orderKey, (custName, orderDate, shipPriority))
        })

        val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
        val lineitemRDD = lineitemDF.rdd

        val lineitem = lineitemRDD
        .filter(line => line.getString(10) > date)
        .map(line => {
          val revenue = line.getDouble(5) * (1 - line.getDouble(6))
          (line.getInt(0), revenue)
        })
        .reduceByKey(_ + _)

        lineitem.cogroup(orders)
        .filter(p => p._2._1.iterator.hasNext && p._2._2.iterator.hasNext)
        .map(p => {
          val item = p._2._2.iterator.next()
          val custName = item._1
          val orderDate = item._2
          val shipPriority = item._3
          val orderKey = p._1
          val revenue = p._2._1.iterator.next()
          (revenue, (custName, orderKey, orderDate, shipPriority))
        })
        .sortByKey(false)
        .take(10)
        .map(p => (p._2._1, p._2._2, p._1, p._2._3, p._2._4))
        .foreach(println)
       }
     }
}
