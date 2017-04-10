import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import java.io.{File, PrintWriter}
import scala.collection.mutable.ListBuffer

/**
  * Created by jaideepbajwa on 2017-04-05.
  */

object webpaytm {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("webpaytm").setMaster("local[4]")
    conf.set("spark.cores.max", "4")
    val sc = new SparkContext(conf)
    val hdconf = sc.hadoopConfiguration
    hdconf.set("fs.s3n.awsAccessKeyId", sys.env("AWS_ACCESS_KEY_ID") )
    hdconf.set("fs.s3n.awsSecretAccessKey", sys.env("AWS_SECRETACCESS_KEY"))
    var distFile = sc.textFile("s3n://paytmfile/2015_07_22_mktplace_shop_web_log_sample.log.gz").cache()
    var weblog = new WebLogParser
    // Apply group by key to key <IP, list of time stamps>
    val ip_time = distFile.map(l => (weblog.parseRecord(l).client_ip, weblog.parseRecord(l).timestamp)).groupByKey().map({case (a,b) => (a,b.toList.sortBy(_.getMillis))})
    // Apply group by key to key <IP, list of (timestamps, URL)>
    val ip_time_url = distFile.map(l => (weblog.parseRecord(l).client_ip, (weblog.parseRecord(l).timestamp,weblog.parseRecord(l).request))).groupByKey().map({case (a,b) => (a, b.toList.sortBy(_._1.getMillis))})
    // Collect data, bring back to driver and call SessionTime to calculate sessions times
    // List of (IP, List(timestamp)) , where iterable(timestamp) is a sorted list of timestamps
    SessionTime(ip_time.collect())
    // Collect data, bring back to driver and call Sessionize to calculate URL info in each session
    // List of (IP, List(timestamp, URL)), where iterable(timestamp, URL) is sorted with timestamps
    Sessionize(ip_time_url.collect())
    sc.stop()
  }
  def isNewSession(x:DateTime, y:DateTime) : Int = {
    // If the y > x+30mins return 1, y and x are in different sessions
    if(y.compareTo(x.plusMinutes(30)).>(0)) 1 else 0
  }

  // Returns the delta between 2 timestamps in milliseconds
  def timeDiff(x:DateTime, y:DateTime): Int = {
    var diff = (x.getMillis - y.getMillis).toInt
    // Return -1 to debug incorrect session time calculation
    if (diff.<(0)) -1 else diff
  }

  // Write to a filename specified by file
  def PrintToFile(file: File) (op: PrintWriter => Unit): Unit = {
    val p = new PrintWriter(file)
    try { op(p)} catch { case e: Exception => println(s"Exception caught: $e");} finally {p.close()}
  }

  def SessionTime(s_time : Array[(String, List[DateTime])]) = {
    // Data will be stored as, (IP1, ( (Session1, (T1, T2,T3)) , (Session2, (T1, T2,T3))), IP2..
    val session = new ListBuffer[(String, List[(Int, ListBuffer[(Int, DateTime)])])]
    for (l <- s_time) {
      var session_time = new ListBuffer[(Int, DateTime)]()
      val ip = l._1
      val time_list = l._2.sliding(2).toList
      var count = 1
      // Add first element regardless and
      // process only if elements greater than 1
      session_time += count -> time_list.head.head
      if (time_list.head.size.>(1)) {
        for (l2 <- time_list) {
          if (isNewSession(l2.head, l2(1)) == 1) {
            // Increment Session count as time stamps are >30mins apart
            count += 1
          }
          // Only add the 2nd element as 1st would have been
          // added in the previous iteration
          session_time += count -> l2(1)
        }
      }
      session += ip -> session_time.groupBy(_._1).toList
    }
    // Print Session info for each IP
    // Sort with number of sessions per IP (getting the size of the map containing the session#->URL)
    val IP_session = new ListBuffer[(String, ListBuffer[Int])]
    for (s <- session.sortWith{(x,y) => x._2.size > y._2.size}) {
      val session_dur = new ListBuffer[Int]
      val ip = s._1
      for (s1 <- s._2.sortWith(_._1 < _._1)) {
        val diff = timeDiff(s1._2.last._2, s1._2.head._2)
        session_dur += diff
      }
      IP_session += ip -> session_dur
    }
    val total_ip = IP_session.size
    // Writing to file
    PrintToFile(new File(s"${sys.env("OUTPUT_DIR")}Max_Session.txt")) { p =>
      p.println(s"Total Number of IP Addresses Analyzed = ${total_ip}")
      p.println(s"${"-".*(50)}")
      p.println(s"      IP${" ".*(8)} | Maximum Time [Millisecond (ms)]")
      p.println(s"${"-".*(50)}")
      for (x <- IP_session.toList.map({ case (a, b) => (a, b.max) }).sortWith(_._2 > _._2)) {
        p.println(s"${x._1} ${" ".*(15 - x._1.length)} |${" ".*(5)}${x._2}")
      }
    }
    // Writing to file
    PrintToFile(new File(s"${sys.env("OUTPUT_DIR")}Avg_Session.txt")) { p =>
      p.println(s"Total Number of IP Addresses Analyzed = ${total_ip}")
      p.println(s"${"-".*(50)}")
      p.println(s"      IP${" ".*(8)} | Average Time [Millisecond (ms)]")
      p.println(s"${"-".*(50)}")
      for (x <- IP_session.toList.map({ case (a, b) => (a, b.sum / b.size) }).sortWith(_._2 > _._2)){
        p.println(s"${x._1} ${" ".*(15 - x._1.length)} |${" ".*(5)}${x._2}")
      }
    }
  }
  def Sessionize(IP_Time_Req: Array[(String, List[(DateTime, String)])]) = {
    // Result would look like (IP1 , ( (Session1, (URL1, URL2, URL3)), (Session2, (URL1, URL2, URL3))), (IP2 ...
    val session = new ListBuffer[(String, List[(Int, ListBuffer[(Int, String)])])]
    for (l <- IP_Time_Req) {
      var session_url = new ListBuffer[(Int,String)]()
      val ip = l._1
      val time_url_list = l._2.sliding(2).toList
      var count = 1
      // Add first element regardless and
      // process only if elements greater than 1
      session_url += count -> time_url_list.head.head._2
      if (time_url_list.head.size.>(1)) {
        for (l2 <- time_url_list) {
          if (isNewSession(l2.head._1, l2(1)._1) == 1) {
            // Increment Session count as time stamps are >30mins apart
            count += 1
          }
          // Only add the 2nd element as 1st would have been
          // added in the previous iteration
          session_url += count -> l2(1)._2
        }
      }
      session += ip -> session_url.groupBy(_._1).toList
    }
    // Writing to file
    // Print Session info for each IP
    // Sort with number of sessions per IP (getting the size of the map containing the session#->URL)
    PrintToFile(new File(s"${sys.env("OUTPUT_DIR")}Sessionize.txt")) { p =>
      for (s <- session.sortWith { (x, y) => x._2.size > y._2.size }) {
        p.println(s"IP:${s._1}, Total sessions:${s._2.size}, Total URLs: ${s._2.map(x => x._2.size).sum}")
        for (s1 <- s._2.sortWith(_._1 < _._1)) {
          p.println(s"  Session: ${s1._1}, URLs: ${s1._2.size}, Unique URLs: ${s1._2.map(x => x._2).distinct.size}")
        }
      }
    }
  }
}