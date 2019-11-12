package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.rogach.scallop._
import org.apache.spark.SparkConf


class ConfQ1(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean](descr = "text", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "parquet", required = false, default = Some(false))
  verify()
}

object Q1 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new ConfQ1(argv)

    log.info("Input: " + args.input())
    log.info("Date:" + args.date())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)

    val date = args.date()

    if (args.text()) {
      val textFile = sc.textFile(args.input() + "/lineitem.tbl")

      val count_txt = textFile
      .filter(line => line.split("\\|")(10).contains(date))
      .count

      println("Answer is" + count_txt)
      
     

      } else if (args.parquet()) {
        val sparkSession = SparkSession.builder.getOrCreate

        val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
        val lineitemRDD = lineitemDF.rdd

        val count_parquet = lineitemRDD
        .filter(line => line.getString(10).contains(date))
        .count

        println("ANSWER is" + count_parquet)

      
          }
      }
    }
  





