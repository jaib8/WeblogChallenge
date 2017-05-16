/**
  * Created by jaideepbajwa on 2017-05-13.
  */

import java.sql.Timestamp

import org.joda.time.DateTime
import org.scalatest.FlatSpec
class TestParser extends FlatSpec{
  /**
    * Create WebLogParser object and call parseRecord method on input line
    * @param line test input
    * @return WebLog object
    */
  def parselog (line: String) : WebLog = {
    val weblog = new WebLogParser
    weblog.parseRecord(line)
  }
  "Input Web log" should "be parsed correctly" in {
    var parsedlog = parselog("2015-07-22T09:00:28.002726Z marketpalce-shop 223.225.236.110:32279 10.0.4.176:80 0.000025 0.069531 0.000021 200 200 105 532 \"POST https://paytm.com:443/api/v1/expresscart/checkout?wallet=1 HTTP/1.1\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 6_1_2 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10B146 Safari/8536.25\" ECDHE-RSA-AES128-SHA256 TLSv1.2")
    assert(parsedlog.client_ip === "223.225.236.110")
    assert(parsedlog.timestamp.equals(new Timestamp(new DateTime("2015-07-22T09:00:28.002726Z").getMillis)))
    assert(parsedlog.request === "POST https://paytm.com:443/api/v1/expresscart/checkout?wallet=1 HTTP/1.1")

    parsedlog = parselog("2015-07-22T09:01:20.897629Z marketpalce-shop 203.99.205.105:43384 10.0.6.108:80 0.000021 0.000245 0.000021 200 200 0 1150 \"GET https://paytm.com:443/favicon.ico HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2")
    assert(parsedlog.client_ip === "203.99.205.105")
    assert(parsedlog.timestamp.equals(new Timestamp(new DateTime("2015-07-22T09:01:20.897629Z").getMillis)))
    assert(parsedlog.request === "GET https://paytm.com:443/favicon.ico HTTP/1.1")

    parsedlog = parselog("2015-07-22T09:03:01.494385Z marketpalce-shop 117.218.251.84:37435 10.0.6.199:80 0.000021 0.001062 0.000021 200 200 0 30443 \"GET https://paytm.com:443/offer/wp-content/uploads/2015/07/offer-page_20jul.jpg HTTP/1.1\" \"Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.89 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2")
    assert(parsedlog.client_ip === "117.218.251.84")
    assert(parsedlog.timestamp.equals(new Timestamp(new DateTime("2015-07-22T09:03:01.494385Z").getMillis)))
    assert(parsedlog.request === "GET https://paytm.com:443/offer/wp-content/uploads/2015/07/offer-page_20jul.jpg HTTP/1.1")
  }
  it should "not be parsed correctly" in {
    val parsedlog = parselog("2015-07-22T09:02:09.782770Z marketpalce-shop 1.38.23.239:18510 10.0.6.178:80 0.000024 0.034231 0.00002 200 200 0 15134 \"GET https://paytm.com:443/idea-prepaid-mobile-online-recharge.htm HTTP/1.1\" \"Mozilla/5.0 (Windows; U; Windows NT 6.2; en-US) AppleWebKit/534.3 (KHTML, like Gecko) Chrome/6.0.472.25 Safari/534.3\" ECDHE-RSA-AES128-SHA TLSv1")
    assert(parsedlog.client_ip !== "223.225.236.110")
    assert(!parsedlog.timestamp.equals(new Timestamp(new DateTime("2015-07-22T09:00:28.002726Z").getMillis)))
    assert(parsedlog.request !== "POST https://paytm.com:443/api/v1/expresscart/checkout?wallet=1 HTTP/1.1")
  }
}
