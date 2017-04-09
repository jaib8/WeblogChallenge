/**
  * Created by jaideepbajwa on 2017-04-06.
  */

import java.util.regex.Pattern
import java.util.regex.Matcher

import org.joda.time.DateTime

/** More info on Pattern
  * https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html
 */
class WebLogParser extends Serializable{

  /**
    * Sample log input
    * 2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 \
    * 0.026109 0.00002 200 200 0 699 "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1" \
    * "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36"
    * ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2
    */
  private val timestamp = "(\\S+)"                                            // Non whitespace ISO 8601 format
  private val elb = "(\\S+)"                                                  // load balancer name
  private val client_ip = "(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):"      // Client IP
  private val client_port = "(\\d+)"                                          // Client PORT
  private val backend_port = "(\\S+)"                                         // Backend IP:PORT , could be -
  private val request_processing_time = "(\\S+)"                              // Number
  private val backend_processing_time = "(\\S+)"                              // Number
  private val response_processing_time = "(\\S+)"                             // Number
  private val elb_status_code = "(\\S+)"                                      // The st atus code of the response from the load balancer
  private val backend_status_code = "(\\S+)"                                  // The st atus code from backend
  private val received_bytes = "(\\S+)"                                       // Number  of Bytes received
  private val sent_bytes = "(\\S+)"                                           // Number  of Bytes sent
  private val request = "\"(\\D+ .* HTTP/\\d\\.[01])\""                       // HTTP Method + Protocol://Host header:port + Path + HTTP version.
  private val user_agent = "\"(.*?)\""                                        // User a gent won't have double quotes so we can grab everything b/w concecutive double quotes
  private val ssl_cipher = "(\\S+)"                                           // If SSL  not established, value is -
  private val ssl_protocol = "(\\S+)"                                         // If SSL  not established, value is -
  private val regex = s"$timestamp $elb $client_ip$client_port $backend_port $request_processing_time $backend_processing_time $response_processing_time $elb_status_code $backend_status_code $received_bytes $sent_bytes $request $user_agent $ssl_cipher $ssl_protocol"
  private val p = Pattern.compile(regex)

  // For debug purpose
  val EmptyLog = WebLog(new DateTime(),"","","","","","","","","","","","","","","")
  def parseRecord(record: String): WebLog = {
    val matcher = p.matcher(record)
    if (matcher.find) {
      WebLogConstruct(matcher)
    } else {
      EmptyLog
    }
  }
  private def WebLogConstruct(matcher: Matcher) = {
    WebLog(
      new DateTime(matcher.group(1)),
      matcher.group(2),
      matcher.group(3),
      matcher.group(4),
      matcher.group(5),
      matcher.group(6),
      matcher.group(7),
      matcher.group(8),
      matcher.group(9),
      matcher.group(10),
      matcher.group(11),
      matcher.group(12),
      matcher.group(13),
      matcher.group(14),
      matcher.group(15),
      matcher.group(16)
    )
  }
}