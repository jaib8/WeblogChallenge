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
    164030.66078807763ms
    ```

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
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

### Additional notes:
- You are allowed to use whatever libraries/parsers/solutions you can find provided you can explain the functions you are implementing in detail.
- IP addresses do not guarantee distinct users, but this is the limitation of the data. As a bonus, consider what additional data would help make better analytical conclusions
- For this dataset, complete the sessionization by time window rather than navigation. Feel free to determine the best session window time on your own, or start with 15 minutes.
- The log file was taken from an AWS Elastic Load Balancer:
http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-format
