import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.log4j._
import org.apache.spark.sql._
import java.sql.Timestamp

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import WebPaytmLogRDD._
/**
  * Created by jaideepbajwa on 2017-04-05.
  */
/**
  * Following ENV Variables needs to be set:
  * AWS_ACCESS_KEY_ID (aws access key id needed to access s3 storage)
  * AWS_SECRETACCESS_KEY (aws secret access key needed to access s3 storage)
  */

object WebPaytmLogRDD {
  type IpTimeUrl = (String, (Timestamp, String))
  type IpTimeUrlGroup = (String, Iterable[(Timestamp, String)])
  type IpTimeUrlSessionedSize = (String, Iterable[(Int, Int)], Int)
  type IpTimeSessioned = (String, Iterable[Int])

  type IpTimeUrlSessioned = (String, Iterable[(Int, Iterable[Timestamp], Iterable[String])])

  /**
    * Following method creates an object of class WebLogParser and calls its method parseRecord
    * on a log input line. If the input line is corrupted the result will be None,
    * hence this function is called via flatmap.
    * @param line input log
    * @return tuple of (IP, (Timestamp, URL))
    */
  def convertToIpTimeUrl( line : String) : Option[IpTimeUrl] = {
    val webLog = new WebLogParser
    val parsedLine = webLog.parseRecord(line)
    if (parsedLine.equals(webLog.EmptyLog)) None else Some(parsedLine.client_ip, (parsedLine.timestamp, parsedLine.request))
  }

  /**
    * Sort the input chronologically
    *
    * @param line of type (IP, Iterable[(Timestamp, URL)])
    * @return Sorted Iterable by ascending order of Timestamp
    */
  def sortIpTimeUrlRdd ( line : IpTimeUrlGroup) : IpTimeUrlGroup = {
    (line._1, line._2.toList.sortBy(_._1.getTime))
  }

  /**
    * To determine if 2 timestamp belong to same session or not
    * @param x Timestamp 1
    * @param y Timestamp 2
    * @return If different session return 1 else 0
    */
  def isNewSession(x:Timestamp, y:Timestamp) : Int = {
    // 30 [mins] * 60 [secs/mins] * 1000 [ms/secs] = 1800000ms
    val win30 :Long = 30 * 60 * 1000
    // Increment x's time by 30mins
    val temp : Timestamp = new Timestamp(x.getTime + win30)
    // Compare with Y
    if(y.compareTo(temp)>0) 1 else 0
  }

  /**
    * Split the input in sessions
    * @param line of type (IP, Iterable[(Timestamp, URL)])
    * @return of type (IP, Iterable[(SessionCount, Iterable[Timestamp], Iterable[URL])])
    */
  def splitInSession (line : IpTimeUrlGroup) :IpTimeUrlSessioned = {
    var SessionTimeURL = new ListBuffer[(Int, Timestamp, String)]()
    // List with sliding 2 to group by session
    val listTwoPair = line._2.sliding(2)
    // Add first element regardless and
    // process only if elements greater than 1
    var count = 1
    var t1 :Timestamp = line._2.head._1
    // When there is only 1 time stamp
    SessionTimeURL += ((count, t1 , line._2.head._2))
    if (line._2.size.>(1)) {
      for ( l1 <- listTwoPair) {
        t1 = l1.head._1
        val t2 :Timestamp = l1.last._1
        if (isNewSession(t1, t2) == 1) {
          // Increment Session as time stamps are >30mins apart
          count += 1
        }
        // Only add the 2nd element as 1st would have been
        // added in the previous iteration
        SessionTimeURL += ((count, t2,l1.last._2))
      }
    }
    // Sort the list of timestamps based on the T1 of each timestamp pairs
    //val SessionTimeURLGrouped = SessionTimeURL.groupBy(_._1).map(x => (x._1 , x._2.map(y => (y._2 , y._3)))).toList.sortBy(_._1)
    val SessionTimeURLGrouped = SessionTimeURL.groupBy(_._1).map(x => (x._1 , x._2.map(y => y._2).sortBy(_.getTime) ,  x._2.map(y => y._3))).toList.sortBy(_._1)
    (line._1, SessionTimeURLGrouped)
  }

  /**
    * To calutation difference in 2 timestamps
    * @param x Timestamp 1
    * @param y Timestamp 2
    * @return Return delta in milliseconds if result > 0 , else return -1
    */
  def timeDiff(x:Timestamp, y:Timestamp): Int = {
    val diff = (x.getTime - y.getTime).toInt
    // Return -1 to debug incorrect session time calculation
    if (diff.<(0)) -1 else diff
  }

  /**
    * Apply various transformations
    * @param rdd input log in RDD
    * @return rdd of type (String, Iterable[(Int, Iterable[Timestamp], Iterable[String])])
    */
  def transformRdd(rdd : RDD[String]) : RDD[IpTimeUrlSessioned] = {
    val ipTimeUrlrdd = rdd.flatMap(convertToIpTimeUrl)
    val ipTimeUrlrddGroup = ipTimeUrlrdd.groupByKey()
    val ipTimeUrlrddGroupSorted = ipTimeUrlrddGroup.map(sortIpTimeUrlRdd)
    ipTimeUrlrddGroupSorted.map(splitInSession)
  }

  /**
    * Apply transformations and calculate session duration and URL count for each session
    * @param rdd
    * @return rdd of type (IP, Iterable[(SessionDur, URLcount)], TotalURLCount)
    */
  def sessionize(rdd : RDD[String]) : RDD[IpTimeUrlSessionedSize] = {
    // Sessionize format (IP, (TIMESTAMP, URL))
    val ipTimeUrlrddGroupSessioned = transformRdd(rdd)
    val sessionURLHits = ipTimeUrlrddGroupSessioned.map(x => (x._1 , x._2.map(y => (timeDiff(y._2.last, y._2.head) , y._3.size))))
    sessionURLHits.map(x=> (x._1 , x._2, x._2.map( y => y._2).sum))
  }

  /**
    * Apply transformations and calculate session dur for each session per IP
    * @param rdd
    * @return rdd of type (IP, Iterable[Session Dur])
    */
  def averageSession ( rdd : RDD[String]) : RDD[IpTimeSessioned]= {
    val ipTimeUrlrddGroupSessioned = transformRdd(rdd)
    ipTimeUrlrddGroupSessioned.map(x => (x._1, x._2.map(y => timeDiff(y._2.last, y._2.head))))
  }

  /**
    * Similar to sessionize method, only difference is drop duplicate URLs
    * @param rdd
    * @return rdd of type (IP, Iterable[(SessionDur, URLcount)], TotalURLCount)
    */
  def uniqueURL ( rdd :RDD[String]) : RDD[IpTimeUrlSessionedSize] =  {
    val ipTimeUrlrddGroupSessioned = transformRdd(rdd)
    val sessionURLHits = ipTimeUrlrddGroupSessioned.map(x => (x._1 , x._2.map(y => (timeDiff(y._2.last, y._2.head) , y._3.toList.distinct.size))))
    sessionURLHits.map(x=> (x._1 , x._2, x._2.map( y => y._2).sum))
  }

  def main(args: Array[String]) {

    // Check if input argument is not [1-4], return with ERROR
    if (args.isEmpty || args(0).toInt.<(1) || args(0).toInt.>(4)) {
      println("ERROR: Invalid argument, should be [1-4]")
      sys.exit(1)
    }
    // Set logger level to only print ERROR
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Create spark context using every core of the local machine
    val conf = new SparkConf().setAppName("WebPaytmLogRDD")
    val sc = new SparkContext(conf)
    val hdconf = sc.hadoopConfiguration
    // Needed to access s3 storage
    hdconf.set("fs.s3a.access.key", sys.env("AWS_ACCESS_KEY_ID"))
    hdconf.set("fs.s3a.secret.key", sys.env("AWS_SECRETACCESS_KEY"))
    val logRDD = sc.textFile("s3a://paytmfile/2015_07_22_mktplace_shop_web_log_sample.log.gz")
    args(0).toInt match {
        // Sessionize weblog
      case 1 =>
        val sessioninfo = sessionize(logRDD)
        val sessionResult = sessioninfo.sortBy(_._3, false).take(10)
        sessionResult.foreach(println)
        // Find average session time
      case 2 =>
        val avgRdd = averageSession(logRDD)
        val average = avgRdd.flatMap(x=> x._2).mean()
        println(s"${average}ms")
        // Sesionize weblog with unique URLHits
      case 3 =>
        val sessionUniqueURL = uniqueURL(logRDD)
        val sessionUniqueURLResult = sessionUniqueURL.sortBy(_._3, false).take(10)
        sessionUniqueURLResult.foreach(println)
        // Find most engaged IPs
      case 4 =>
        val IPTimeRdd = averageSession(logRDD)
        val mostEngagedIP = IPTimeRdd.map(x=> (x._1, x._2.max)).sortBy(_._2,false).take(10)
        mostEngagedIP.foreach(println)
    }
  }
}

object WebPaytmLogDS {



  /**
    * Define schema for DS
    * @param IP Client IP
    * @param T1 Initial timestamp
    * @param DUR Duration between the next timestamp in the same session
    * @param URL URL
    */
  case class DFFinal(IP: String, T1 : Timestamp, DUR: Int, URL : String)

  /**
    * Transform RDD to get to the schema define above
    * @param line Input log
    * @return ArrayBuffer of case class DFFinal, which is applied to RDD as flatmap.
    */
  def getFinalDS(line : IpTimeUrlGroup) : ArrayBuffer[DFFinal]= {
    var SessionTimeURL :ArrayBuffer[DFFinal] = ArrayBuffer()
    val ip = line._1
    // List with sliding 2 to group by session
    val listTwoPair = line._2.sliding(2)
    // Add first element regardless and
    // process only if elements greater than 1
    var t1 :Timestamp = line._2.head._1
    var Ttemp : Timestamp = t1
    // When there is only 1 time stamp
    SessionTimeURL += new DFFinal(ip, t1 , 0,  line._2.head._2)
    if (line._2.size.>(1)) {
      for ( l1 <- listTwoPair) {
        t1 = l1.head._1
        val t2 :Timestamp = l1.last._1
        if (isNewSession(t1, t2) == 1) {
          // Increment Session as time stamps are >30mins apart
          Ttemp = t2
          SessionTimeURL += new DFFinal(ip, Ttemp , 0,  l1.head._2)
        }else {
          // Only add the 2nd element as 1st would have been
          // added in the previous iteration
          SessionTimeURL += new DFFinal(ip, Ttemp, timeDiff(t2, t1), l1.head._2)
        }
      }
    }
    return SessionTimeURL
  }

  def main(args: Array[String]) {

    // Check if input argument is not [1-4], return with ERROR
    if (args.isEmpty || args(0).toInt.<(1) || args(0).toInt.>(4)) {
      println("ERROR: Invalid argument, should be [1-4]")
      sys.exit(1)
    }
    // Set logger level to only print ERROR
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Create spark Session
    val spark = SparkSession
    .builder()
    .appName("WebPaytmLogDS")
    .getOrCreate()

    val sc = spark.sparkContext
    val hdconf = sc.hadoopConfiguration
    hdconf.set("fs.s3a.access.key", sys.env("AWS_ACCESS_KEY_ID"))
    hdconf.set("fs.s3a.secret.key", sys.env("AWS_SECRETACCESS_KEY"))
    val logRDD = sc.textFile("s3a://paytmfile/2015_07_22_mktplace_shop_web_log_sample.log.gz")

    // Apply structure
    val logRDDwithStructure = logRDD.flatMap(convertToIpTimeUrl)
    val ipTimeUrlrddGroup = logRDDwithStructure.groupByKey()
    val ipTimeUrlrddGroupSorted = ipTimeUrlrddGroup.map(sortIpTimeUrlRdd)
    val ipTimeUrlrddGroupSortedFinal = ipTimeUrlrddGroupSorted.flatMap(getFinalDS)

    // Infer the schema and register the table as Dataset
    import spark.implicits._
    val DSLog = ipTimeUrlrddGroupSortedFinal.toDS.cache()
    //DSLog.printSchema()

    args(0).toInt match {
      case 1 =>
        // Sessionize weblog
        // Number of URLs per IP per session
        DSLog.select("IP","T1", "DUR", "URL").groupBy("IP", "T1").count().select($"IP", $"count".as("URLHits")).orderBy($"URLHits".desc).show()
      case 2 =>
        // Average Session Duration
        val averageSession = DSLog.select("IP","T1","DUR").groupBy("IP", "T1").sum("DUR").select("sum(DUR)").rdd.map(_(0).asInstanceOf[Long]).mean()
        println(s"${averageSession}ms")
      case 3 =>
        // Number of unique URLs per IP per session
        DSLog.select("IP","T1", "DUR", "URL").dropDuplicates("IP", "T1","URL").groupBy("IP", "T1").count().select($"IP", $"count".as("UniqueURLHits")).orderBy($"UniqueURLHits".desc).show()
      case 4 =>
        // Session Duration, most engaged users
        DSLog.select("IP","T1","DUR").groupBy("IP", "T1").sum("DUR").orderBy($"sum(DUR)".desc).select($"IP", $"sum(DUR)".as("SessionTime[ms]")).show()
    }
    spark.stop()
  }
}
