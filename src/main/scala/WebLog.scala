import java.sql.Timestamp

/**
  * Created by jaideepbajwa on 2017-04-06.
  */

/**
  * The log file was taken from an AWS Elastic Load Balancer:
  * http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-format
  */
case class WebLog(
  var timestamp: Timestamp,
  //var elb: String,
  var client_ip: String,
  //var client_port: String,
  //var backend_port: String,
  //var request_processing_time: String,
  //var backend_processing_time: String,
  //var response_processing_time: String,
  //var elb_status_code: String,
  //var backend_status_code: String,
  //var received_bytes: String,
  //var sent_bytes: String,
  var request: String
  //var user_agent: String,
  //var ssl_cipher: String,
  //var ssl_protocol: String
)

