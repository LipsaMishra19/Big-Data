package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._
import org.apache.spark.SparkConf


class ConfQ6(args: Seq[String]) extends ScallopConf(args) {
	mainOptions = Seq(input, date, text, parquet)
	val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean](descr = "text", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "parquet", required = false, default = Some(false))
  verify()
}


object Q6 extends {
   val log = Logger.getLogger(getClass().getName())
   
   def main(argv: Array[String]) {
     val args = new ConfQ6(argv)

     log.info("Input: " + args.input())
     //log.info("Date:" + args.date())

     val conf = new SparkConf().setAppName("Q6")
     val sc = new SparkContext(conf)

     val date = args.date()

     if (args.text()) {
      /* 
      val part = sc.textFile(args.input() + "/part.tbl")
        .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1)))
      val broadcastPart = sc.broadcast(part.collectAsMap())
      val partVal = broadcastPart.value

      val supplier = sc.textFile(args.input() + "/supplier.tbl")
        .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1)))
      val broadcastSupplier = sc.broadcast(supplier.collectAsMap())
      val supplierVal = broadcastSupplier.value

*/

      /*part
     .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1)))
       val partBroadcast = sc.broadcast(part.collectAsMap())

       Supplier
       .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1)))
       val suppBroadcast = sc.broadcast(Supplier.collectAsMap())
*/
       val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
       .filter(line => 
        line.split("\\|")(10).contains(date))
          .map(line => {
            val lines = line.split("\\|")
            val returnflag = lines(8)
            val linestatus = lines(9)
            val extendedprice = lines(5).toDouble
            val discount = lines(6).toDouble
            val tax = lines(7).toDouble
            val discprice = extendedprice * (1 - discount)
            val charge = discprice * (1 - tax)
            val quantity = lines(4).toInt
            ((returnflag,linestatus),(quantity, extendedprice, discprice, charge, discount, 1))
        })
          .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6))
          .collect()
          .foreach(p => {
            val count = p._2._6
            val avgQuantity = p._2._1/count
            val avgPrice = p._2._2/count
            val avgDisc = p._2._5/count
          println(p._1._1, p._1._2, p._2._1, p._2._2, p._2._3, p._2._4, avgQuantity, avgPrice, avgDisc, count)
            })
        } else if (args.parquet()) {

         val sparkSession = SparkSession.builder.getOrCreate
         /*
         val partDF = sparkSession.read.parquet(args.input() + "/part")
         val partRDD = partDF.rdd
         val part = partRDD
        .map(line => (line.getInt(0), line.getString(1)))
         val broadcastPart = sc.broadcast(part.collectAsMap())
         val partVal = broadcastPart.value

        val supplierDF = sparkSession.read.parquet(args.input() + "/supplier")
        val supplierRDD = supplierDF.rdd
        val supplier = supplierRDD
        .map(line => (line.getInt(0), line.getString(1)))
        val broadcastSupplier = sc.broadcast(supplier.collectAsMap())
        val supplierVal = broadcastSupplier.value

        */

        val lineitemDF = sparkSession.read.parquet(args.input() +"/lineitem")
        val lineitemRDD = lineitemDF.rdd

        val lineitem = lineitemRDD
        .filter(line => line.getString(10).contains(date))
        .map(line => {
          val returnflag = line.getString(8)
          val linestatus = line.getString(9)
          val extendedprice = line.getDouble(5)
          val discount = line.getDouble(6)
          val tax = line.getDouble(7)
          val discprice = extendedprice * (1.0 - discount)
          val charge = discprice * (1.0 - tax)
          val quantity = line.getDouble(4).toInt
          ((returnflag, linestatus), (quantity, extendedprice, discprice, charge, discount, 1))
        })
        .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6))
        .collect()
        .foreach(p => {
          val count = p._2._6
          val avgQuantity = p._2._1/count
          val avgPrice = p._2._2/count
          val avgDisc = p._2._5/count
          println(p._1._1, p._1._2, p._2._1, p._2._2, p._2._3, p._2._4, avgQuantity, avgPrice, avgDisc, count)
        })
       }
     }
}
