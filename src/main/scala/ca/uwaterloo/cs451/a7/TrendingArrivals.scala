package ca.uwaterloo.cs451.a7

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, StreamingContext}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.util.LongAccumulator
import org.rogach.scallop._
import scala.collection.mutable.ListBuffer

import scala.collection.mutable
import scala.collection.mutable._

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._

class TrendingArrivalConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, checkpoint, output)
  val input = opt[String](descr = "input path", required = true)
  val checkpoint = opt[String](descr = "checkpoint path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

case class temptuple(current: Int, timestamp: String, previous: Int) extends Serializable

object TrendingArrivals {
  val log = Logger.getLogger(getClass().getName())

  def taxiStateFunc(batchTime: Time, key: String, value: Option[Int], state: State[temptuple]) : Option[(String, temptuple)] = {
      var previous = 0
      if (state.exists()) {
        val curr_state = state.get()
        previous = curr_state.current
      }
      var current = value.getOrElse(0).toInt
      var batchTime_ms = batchTime.milliseconds
      if ((current >= 10) && (current >= 2*previous))
      {
        if (key.equalsIgnoreCase("goldman")) {
          println(s"Number of arrivals to Goldman Sachs has doubled from $previous to $current at $batchTime_ms!") 
        }
        else {
          println(s"Number of arrivals to Citigroup has doubled from $previous to $current at $batchTime_ms!")
        }
      }
      

      var t1 = temptuple(current = current, timestamp = "%08d".format(batchTime_ms), previous = previous)
      state.update(t1)
      Some((key,t1))
  }

  def main(argv: Array[String]): Unit = {
    val args = new TrendingArrivalConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("EventCount")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 144)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)

    val output = StateSpec.function(taxiStateFunc _)

    val goldman = List((-74.0141012, 40.7152191), (-74.013777, 40.7152275), (-74.0141027, 40.71387450), (-74.0144185, 40.7140753))
    val citigroup = List((-74.011869, 40.7217236), (-74.009867, 40.721493), (-74.010140,40.720053), (-74.012083, 40.720267))

    val GREEN = "green"
    val YELLOW = "yellow"
    val wc = stream.map(_.split(","))
      .flatMap(tuple => { 
        var longitude= 0D
        var latitude = 0D
        val taxi_type = tuple(0)
        var company = ""
        if (GREEN.equalsIgnoreCase(taxi_type)){
          longitude = tuple(8).toDouble
          latitude = tuple(9).toDouble
        }
        else if (YELLOW.equalsIgnoreCase(taxi_type)){
          longitude = tuple(10).toDouble
          latitude = tuple(11).toDouble
          }

          //val c1 = "goldman"
          //val c2 = "citigroup"
          
          //select the company
          if ((longitude > -74.012083 && longitude < -74.009867) && (latitude > 40.720053 && latitude < 40.7217236 )) {company = "citigroup"}
                else if ((longitude > -74.0144185 && longitude < -74.013777) && (latitude > 40.7138745 && latitude < 40.7152275 )) {company = "goldman"}

                if (company.equalsIgnoreCase("goldman")) {
                     var list = new ListBuffer[Tuple2[String,Int]]()
                     var tuple:Tuple2[String,Int] = ("goldman",1)
                     list += tuple
                     list
                }
                else if (company.equalsIgnoreCase("citigroup")) {
                     var list = new ListBuffer[Tuple2[String,Int]]()
                     var tuple:Tuple2[String,Int] = ("citigroup",1)
                     list += tuple
                     list
                }
                else { List() }
      })
        //val color = tuple(0)
        //val taxi_type = if(color == "green")
        //(List(tuple(8).toDouble, tuple(9).toDouble),1) else (List(tuple(10).toDouble, tuple(11).toDouble),1)) 
      //Citigroup
         // .filter(l => ((l._1(0).toDouble > -74.012083 && l._1(0).toDouble < -74.009867) && (l._1(1).toDouble > 40.720053 && l._1(1).toDouble < 40.7217236 )) ||
           //            ((l._1(0).toDouble > -74.0144185 && l._1(0).toDouble < -74.013777) && (l._1(1).toDouble > 40.7138745 && l._1(1).toDouble < 40.7152275 )))//Goldman
             //            .map(p => {
               //            if ((l._1(0).toDouble > -74.012083 && l._1(0).toDouble < -74.009867)  && (l._1(1).toDouble > 40.720053 && l._1(1).toDouble < 40.7217236 ))
           //("citigroup",1) else ("goldman",1)
       // })
      //.map(tuple => ("all", 1))
      .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(10), Minutes(10))
      .mapWithState(output)
      //.persist()

    //wc.saveAsTextFiles(args.output())
    var output1 = args.output()

    val snapRDD = wc.stateSnapshots()

    snapRDD.foreachRDD((rdd, time) => {
      var updatedRDD = rdd.map{case(key,value) => (key,(value.current,value.timestamp,value.previous))}
      updatedRDD.saveAsTextFile(output1 + "/part-"+"%08d".format(time.milliseconds))
     })

    snapRDD.foreachRDD(rdd => {
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}
