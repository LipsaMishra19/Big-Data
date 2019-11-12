package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._
import org.apache.spark.SparkConf


class ConfQ3(args: Seq[String]) extends ScallopConf(args) {
	mainOptions = Seq(input, date, text, parquet)
	val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean](descr = "text", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "parquet", required = false, default = Some(false))
  verify()
}


object Q3 extends {
   val log = Logger.getLogger(getClass().getName())
   
   def main(argv: Array[String]) {
     val args = new ConfQ3(argv)

     log.info("Input: " + args.input())
     log.info("Date:" + args.date())

     val conf = new SparkConf().setAppName("Q3")
     val sc = new SparkContext(conf)

     val date = args.date()

     if (args.text()) {
     /*  val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
       val part = sc.textFile(args.input() + "/part.tbl")
       val Supplier = sc.textFile(args.input() + "/supplier.tbl")*/
       
      val part = sc.textFile(args.input() + "/part.tbl")
        .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1)))
      val broadcastPart = sc.broadcast(part.collectAsMap())
      val partVal = broadcastPart.value

      val supplier = sc.textFile(args.input() + "/supplier.tbl")
        .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1)))
      val broadcastSupplier = sc.broadcast(supplier.collectAsMap())
      val supplierVal = broadcastSupplier.value



      /*part
     .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1)))
       val partBroadcast = sc.broadcast(part.collectAsMap())

       Supplier
       .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(1)))
       val suppBroadcast = sc.broadcast(Supplier.collectAsMap())
*/
       val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
       .filter(line => line.split("\\|")(10).contains(date))
       .map(line => {
         val lines = line.split("\\|")
         //  val partTable = broadcastPart.value
         //  val suppTable = broadcastSupplier.value
         (lines(0).toInt, (partVal(lines(1).toInt), supplierVal(lines(2).toInt)))
        })
        .sortByKey()
        .take(20)
        .foreach(p => println(p._1, p._2._1, p._2._2))
     }
     else if (args.parquet()) {
       
       val sparkSession = SparkSession.builder.getOrCreate
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


       val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
       val lineitemRDD = lineitemDF.rdd

       val lineitem = lineitemRDD
        .filter(line => line.getString(10).contains(date))
        .map(line => {
        (line.getInt(0), (partVal(line.getInt(1)), supplierVal(line.getInt(2))))
        })
        .sortByKey()
        .take(20)
        .foreach(p => println(p._1, p._2._1, p._2._2))
        
        }
       }
     }
   
