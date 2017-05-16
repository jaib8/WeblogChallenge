### Build

```
> sbt package
```
### Test Parser
```
> sbt test
```

### Running spark-submit
```
> ./spark-submit --master <spark master url> --packages org.apache.hadoop:hadoop-aws:2.7.1,com.amazonaws:aws-java-sdk:1.7.4 --class WebPaytmLog[RDD|DS] webpaytm_2.11-1.0.jar [1-4]
```
### Note:
- WebPaytmLogRDD : Solution using RDD
- WebPaytmLogDS : Solution using DS
- Argument: [1-4] number corresponding to question in `Processing & Analytical goals`

## Processing & Analytical goals(results):

1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a fixed time window.
    https://en.wikipedia.org/wiki/Session_(web_analytics)
    ```
    > ./spark-submit --master <spark master url> --packages org.apache.hadoop:hadoop-aws:2.7.1,com.amazonaws:aws-java-sdk:1.7.4 --class WebPaytmLogRDD webpaytm_2.11-1.0.jar 1
    ```
    * Using RDD:
        - Following transformations are applied:
            1. flatmap (flatmap, since the parser can return None if input log has corrupted record) function to convert each row of the input log file into a tuple of type (IP: String, (Time: Timestamp, URL :String))
            2. sort the above RDD by key (IP address), which results in (IP: String, Iterable[(Time: Timestamp, URL: String)])
            3. map function to sort the Iterable (Time: Timestamp, URL: String) for each row chronologically.
            4. map function to split the above form into sessions based on a timewindow of 30mins. The Iterable[(Time: Timestamp, URL: String)] are grouped into tuples of 2 adjacent elements using the sliding(2) method. Each of the 2 elements in each group are compared to see if they belong to the same time window of not. The result of the function is (IP: String, Iterable[(SessionCount: Int, Session_TimeStamps: Iterable[Timestamp], Session_URLs: Iterable[String])])
            5. map to transform into (IP, (Timestamp duration, numer of URLs) ) , Timestamp duration is calculated by finding the timediff of (head, tail) of timestamp iterable, numer of URLs is the size of the iterable.
            6. map to transform into in (IP, Iterable( session_duration, URL_count), Total URL), Total URL is the sum of URL_count of each element of Iterable( session_duration, URL_count) 
            7. Sort the above RDD by the total number of URLs (in Descending order)
        - Following action is performed
            1. take(10) action is perfomed to execute the spark job and bring back 10 output result rows.
        
            ```
            (52.74.219.71,List((299556,1562), (299724,2295), (299732,927), (255004,3090), (2069162,11609), (2060378,9467),(1499486,7852), (299873,3831)),40633)
            (119.81.61.166,List((297011,3429), (298445,1128), (299163,1730), (253304,489), (2068849,1818), (2060068,13431), (1499726,7817), (299612,2987)),32829)
            (106.186.23.95,List((231063,1481), (258626,1133), (266383,435), (254717,600), (2068756,2848), (2060113,1881), (1381709,1073), (299904,5114)),14565)
            (54.169.20.106,List((118948,670), (65542,242), (2003449,2838), (1463678,1653), (52245,135)),5538)
            (54.169.0.163,List((90491,499), (119281,909), (66738,192), (2003309,2560), (168379,1038)),5198)
            (54.169.164.205,List((114302,866), (64978,120), (1982361,2520), (1459628,1492), (51896,176)),5174)
            (112.196.25.164,List((260188,1429), (299778,1946), (299806,1752)),5127)
            (54.169.1.152,List((58056,146), (5243,100), (1923355,2538), (1463931,1955), (52274,178)),4917)
            (106.51.132.54,List((263962,4399)),4399)
            (54.251.151.39,List((296804,141), (298566,229), (299804,217), (252838,378), (2067023,1577), (2058364,707), (1496805,546), (292539,208)),4003)
            ```
    ```
    > ./spark-submit --master <spark master url> --packages org.apache.hadoop:hadoop-aws:2.7.1,com.amazonaws:aws-java-sdk:1.7.4 --class WebPaytmLogDS webpaytm_2.11-1.0.jar 1
    ```
    * Using DS:
        -   Following transformations are applied to RDD to generate a suitable scehema
            1. a. b. c. are the same from above RDD solution. Which results in a RDD of type (IP: String, Iterable[(Time: Timestamp, URL: String)]) which is sorted chronologically.
            2. flatmap (each iterable is expanded) function (similar to d. in RDD solution) to generate a schema with scala case class (IP: String, T1 : Timestamp, DUR: Int, URL : String). Where T1 is the initial 1st timestamp for a session, DUR is time duration from the prevous URL request by same IP in the same session. So all requests in the same session will have the same T1, and DUR is set to 0 for the first input for a session.
            3. The above transformed RDD is converted to DS, with following schema:           
            ```
            |-- IP: string (nullable = true)
            |-- T1: timestamp (nullable = true)
            |-- DUR: integer (nullable = true)
            |-- URL: string (nullable = true)
            ```
            I decided to use this scehma as it provides all the information necessary to answer the 4 questions.
            4. Query the DF as follows
            ```
            select("IP","T1", "DUR", "URL").groupBy("IP", "T1").count().select($"IP", $"count".as("URLHits")).orderBy($"URLHits".desc)
            ```
            select all columns, group by IP first then Timestamp for each IP, count the total logs with same T1 (which is URL count per session per IP), show() action is performed to get result by desc order of URLcount.        
            ```    
            +--------------+-------+                                                        
            |            IP|URLHits|
            +--------------+-------+
            | 119.81.61.166|  13431|
            |  52.74.219.71|  11609|
            |  52.74.219.71|   9467|
            |  52.74.219.71|   7852|
            | 119.81.61.166|   7817|
            | 106.186.23.95|   5114|
            | 106.51.132.54|   4399|
            |  52.74.219.71|   3831|
            | 119.81.61.166|   3429|
            |  52.74.219.71|   3090|
            | 119.81.61.166|   2987|
            | 106.186.23.95|   2848|
            | 54.169.20.106|   2838|
            |  54.169.0.163|   2560|
            |  54.169.1.152|   2538|
            |54.169.164.205|   2520|
            |  52.74.219.71|   2295|
            |  54.169.64.74|   2223|
            |  54.169.1.152|   1955|
            |112.196.25.164|   1946|
            +--------------+-------+
            only showing top 20 rows
            ```

2. Determine the average session time
    ```
    > ./spark-submit --master <spark master url> --packages org.apache.hadoop:hadoop-aws:2.7.1,com.amazonaws:aws-java-sdk:1.7.4 --class WebPaytmLogRDD webpaytm_2.11-1.0.jar 2
    ```
    * Using RDD:
        - Following transformations are applied in addition to a. b. c. d. from 1st answer:
            1. map function to generate RDD of type (IP:String, Iterable[Dur:Int]), where Dur is the timediff between 1st timestamp and last timestamp of timestamp iterable.
            2. flatmap to expand the Iterable[Dur:Int]
        - Following action is performed
            1. mean() action is perfomed to get the average duration.
            ```
            164030.6607880777ms
            ```
    ```
    > ./spark-submit --master <spark master url> --packages org.apache.hadoop:hadoop-aws:2.7.1,com.amazonaws:aws-java-sdk:1.7.4 --class WebPaytmLogDS webpaytm_2.11-1.0.jar 2
    ```            
    * Using DS:
        - Following transformations are applied in addition to a. b. c. from 1st answer:
            1. Query the DF as follows:
            ```
            select("IP","T1","DUR").groupBy("IP", "T1").sum("DUR").select("sum(DUR)").rdd.map(_(0).asInstanceOf[Long]).mean()
            ```
            select IP, initial Timestamp and Duration, group by IP then Timestamp, for each timestamp/ip sum the duration time. Convert to rdd and perform action mean() to retrieve the average session time.
            ```
            164030.66078807763ms
            ```

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
    ```
    > ./spark-submit --master <spark master url> --packages org.apache.hadoop:hadoop-aws:2.7.1,com.amazonaws:aws-java-sdk:1.7.4 --class WebPaytmLogRDD webpaytm_2.11-1.0.jar 3
    ```  
   * Using RDD:
        - Following transformations are applied in addition to a. b. c. d. from 1st answer:
            1. Almost the same as 5. of 1st answer, just the URL count is calculated by first applying distinct method to URL iterable and then getting the size.
            2. Rest same as 6. and 7. from 1st answer.
            ```
            (52.74.219.71,List((299556,1315), (299724,2037), (299732,819), (255004,2572), (2069162,9532), (2060378,7916), (1499486,5424), (299873,2466)),32081)
            (119.81.61.166,List((297011,3334), (298445,1100), (299163,1671), (253304,489), (2068849,1739), (2060068,9012), (1499726,5790), (299612,2841)),25976)
            (106.186.23.95,List((231063,1453), (258626,1120), (266383,421), (254717,578), (2068756,2731), (2060113,1805), (1381709,992), (299904,4656)),13756)
            (54.169.20.106,List((118948,670), (65542,242), (2003449,2648), (1463678,1601), (52245,135)),5296)
            (54.169.0.163,List((90491,499), (119281,909), (66738,192), (2003309,2423), (168379,1038)),5061)
            (54.169.164.205,List((114302,866), (64978,120), (1982361,2413), (1459628,1461), (51896,176)),5036)
            (54.169.1.152,List((58056,146), (5243,100), (1923355,2426), (1463931,1909), (52274,178)),4759)
            (54.169.106.125,List((90348,429), (6232,67), (1376012,1803), (1463692,934)),3233)
            (54.169.136.105,List((67884,379), (6439,95), (1256762,970), (1459208,1664)),3108)
            (52.74.59.227,List((6448,82), (2058867,1808), (118009,765), (52284,152)),2807)
            ```
    ```
    > ./spark-submit --master <spark master url> --packages org.apache.hadoop:hadoop-aws:2.7.1,com.amazonaws:aws-java-sdk:1.7.4 --class WebPaytmLogDS webpaytm_2.11-1.0.jar 3
    ```
    * Using DS:
        - Following transformations are applied in addition to a. b. c. from 1st answer:
            1. Query the DF as almost the same as 1st answer just adding the following:
            ```
            select("IP","T1", "DUR", "URL").dropDuplicates("IP", "T1","URL")...
            ```
            After selecting all colums, drop the duplicate entries with same "IP", "Initial Timestamp (should be same for all logs belonging to the same session", and "URL". Rest is same as 1st answer.
            ```
            +--------------+-------------+                                                  
            |            IP|UniqueURLHits|
            +--------------+-------------+
            |  52.74.219.71|         9532|
            | 119.81.61.166|         9013|
            |  52.74.219.71|         7916|
            | 119.81.61.166|         5791|
            |  52.74.219.71|         5424|
            | 106.186.23.95|         4656|
            | 119.81.61.166|         3333|
            | 119.81.61.166|         2840|
            | 106.186.23.95|         2731|
            | 54.169.20.106|         2648|
            | 106.51.132.54|         2609|
            |  52.74.219.71|         2572|
            |  52.74.219.71|         2467|
            |  54.169.1.152|         2426|
            |  54.169.0.163|         2423|
            |54.169.164.205|         2413|
            |  54.169.64.74|         2129|
            |  52.74.219.71|         2037|
            |  54.169.1.152|         1909|
            |  52.74.59.227|         1808|
            +--------------+-------------+
            only showing top 20 rows
            ```

4. Find the most engaged users, ie the IPs with the longest session times
    ```
    > ./spark-submit --master <spark master url> --packages org.apache.hadoop:hadoop-aws:2.7.1,com.amazonaws:aws-java-sdk:1.7.4 --class WebPaytmLogRDD webpaytm_2.11-1.0.jar 4
    ```
    * Using RDD:
        - Following transformations are applied in addition to a. b. c. d. from 1st answer:
            1. map function to transform RDD to (String: IP, Int: TimeDur.max), 
            2. sort the RDD by TimeDur_max
            
        - Following action:
            1. take(10) is performed to execute spark job and get top 10 most engaged users.
            ```
            (220.226.206.7,3248164)
            (52.74.219.71,2069162)
            (119.81.61.166,2068849)
            (106.186.23.95,2068756)
            (125.19.44.66,2068713)
            (125.20.39.66,2068320)
            (192.8.190.10,2067235)
            (54.251.151.39,2067023)
            (180.211.69.209,2066961)
            (202.167.250.59,2066330)
            ```
    ```
    > ./spark-submit --master <spark master url> --packages org.apache.hadoop:hadoop-aws:2.7.1,com.amazonaws:aws-java-sdk:1.7.4 --class WebPaytmLogDS webpaytm_2.11-1.0.jar 4
    ```
     * Using DS:
        - Following transformations are applied in addition to a. b. c. from 1st answer:
            1. Query the DF as follows:
            ```
            select("IP","T1","DUR").groupBy("IP", "T1").sum("DUR").orderBy($"sum(DUR)".desc).select($"IP", $"sum(DUR)".as("SessionTime[ms]")).show()
            ```
            select IP, Timestamp, DUR. Groupby IP then Timestamp, then get the sum of Duration for each IP (that should be the session time). Order by the sum of duration, and perform show() action to fetch top 20 most engaged users.

            ```
            +---------------+---------------+                                               
            |             IP|SessionTime[ms]|
            +---------------+---------------+
            |  220.226.206.7|        3248164|
            |   52.74.219.71|        2069162|
            |  119.81.61.166|        2068849|
            |  106.186.23.95|        2068756|
            |   125.19.44.66|        2068713|
            |   125.20.39.66|        2068320|
            |   192.8.190.10|        2067235|
            |  54.251.151.39|        2067023|
            | 180.211.69.209|        2066961|
            | 202.167.250.59|        2066330|
            | 180.179.213.70|        2065638|
            | 203.189.176.14|        2065594|
            |213.239.204.204|        2065587|
            |  122.15.156.64|        2065520|
            | 103.29.159.138|        2065413|
            | 203.191.34.178|        2065346|
            | 180.151.80.140|        2065250|
            |    78.46.60.71|        2064862|
            | 103.29.159.186|        2064699|
            | 125.16.218.194|        2064475|
            +---------------+---------------+
            only showing top 20 rows
            ```

### Tools allowed (in no particular order):
- Spark (Scala)

### Observation and design decision:
- The primary algorithm in this assignment is to find sessions. To create sessions I have used the following approach:
    * Group all timestamps together in a Iterable for each IP.
    * Sort the Iterable of timestamps chronologically (ascending order of timestamps).
    * Group the Iteable of timestamps in pairs of 2 using sliding method.
    * Iterate through all the pairs and compare the each element of the pair to decide if they belong to the same session or not ( if timediff > timewindow). If same session, then associate them with the specific session count (session count initialized to 0), else increment the session count and associate with that.
    * Now the result is for each IP you have a tuple like (IP,  (count number, (timestamp iterable associated with the session count number), (URL iterable associated with the session count number))).
- Previously I was using joda's Datetime to store timestamp, when implementing DataSet/DataFrame solution I made a switch to java.sql.TimeStamp (since Datetime is not supported by DF). This resulted in 1.7x performance gain (from 50sec -> 28 secs). (running on macbook pro 2015, 4 cores)
- The DS solution has the same RDD algorithm to find sessions. Just converting the log to DF would not give enough information to sessionize.
- The perfomance of both RDD and DS solution is similar (~30secs). But once schema is created DS is definitely easier to query data.
- Used **sbt** to package jar file, **s3(aws)** to access input log file and **Intellij** for IDE.

### Additional notes:
- IP addresses do not guarantee distinct users, but this is the limitation of the data. As a bonus, consider what additional data would help make better analytical conclusions

    Using IP with port number, Ip:port should signify unique connection
    If (IP,port) pair is same then inspect the user agent to check Brower type/ OS type
- For this dataset, complete the sessionization by time window rather than navigation. Feel free to determine the best session window time on your own, or start with 15 minutes.

    The timewindow can be easily changed, in one function (isNewSession). I've used 30mins following https://support.google.com/analytics/answer/2731565?hl=en
