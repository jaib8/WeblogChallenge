## Processing & Analytical goals:

1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a fixed time window.
    https://en.wikipedia.org/wiki/Session_(web_analytics)
    
    [Session information per IP](results/Sessionize.txt)
    For each IP, session information contains Session Number, Total URLs accessed in that session, unique URLs accessed in that session.
    This should also answer 3.
    Output example:
    ```
    IP:220.226.206.7, Total sessions:11, Total URLs: 1536
      Session: 1, URLs: 15, Unique URLs: 7
      Session: 2, URLs: 8, Unique URLs: 5
      Session: 3, URLs: 33, Unique URLs: 25
      Session: 4, URLs: 19, Unique URLs: 13
      Session: 5, URLs: 84, Unique URLs: 42
      Session: 6, URLs: 113, Unique URLs: 15
      Session: 7, URLs: 159, Unique URLs: 61
      Session: 8, URLs: 292, Unique URLs: 245
      Session: 9, URLs: 554, Unique URLs: 284
      Session: 10, URLs: 96, Unique URLs: 30
      Session: 11, URLs: 163, Unique URLs: 163
    IP:52.74.219.71, Total sessions:8, Total URLs: 40633
      Session: 1, URLs: 1562, Unique URLs: 1315
      Session: 2, URLs: 2295, Unique URLs: 2037
      Session: 3, URLs: 927, Unique URLs: 819
      Session: 4, URLs: 3090, Unique URLs: 2572
      Session: 5, URLs: 11609, Unique URLs: 9532
      Session: 6, URLs: 9467, Unique URLs: 7916
      Session: 7, URLs: 7852, Unique URLs: 5424
      Session: 8, URLs: 3831, Unique URLs: 2466
    ```

2. Determine the average session time

    [Average session time per IP](results/Avg_Session.txt)
    Sorted in descending order of the session time.
    Output example:
    ```
    Total Number of IP Addresses Analyzed = 90544
    --------------------------------------------------
          IP         | Average Time [Millisecond (ms)]
    --------------------------------------------------
    103.29.159.138   |     2065413
    125.16.218.194   |     2064475
    14.99.226.79     |     2062909
    117.217.94.18    |     2061762
    117.218.61.172   |     2060761
    180.151.32.147   |     2060410
    122.169.141.4    |     2060159
    223.182.246.141  |     2059864
    59.97.160.225    |     2059829
    59.91.252.41     |     2059071
    14.139.220.98    |     2058319
    14.97.148.248    |     2057760
    117.205.158.11   |     2057223

    ```

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

    Please see 1.

4. Find the most engaged users, ie the IPs with the longest session times

    [List of IPs with longest session times](results/Max_Session.txt)
    The list is sorted in descending order of the max session time.
    Output example:
    ```
    Total Number of IP Addresses Analyzed = 90544
    --------------------------------------------------
          IP         | Maximum Time [Millisecond (ms)]
    --------------------------------------------------
    220.226.206.7    |     3248164
    52.74.219.71     |     2069162
    119.81.61.166    |     2068849
    106.186.23.95    |     2068756
    125.19.44.66     |     2068713
    125.20.39.66     |     2068320
    192.8.190.10     |     2067235
    54.251.151.39    |     2067023
    180.211.69.209   |     2066961
    202.167.250.59   |     2066330
    180.179.213.70   |     2065638
    203.189.176.14   |     2065594
    213.239.204.204  |     2065587
    ```

### Tools used:
- Spark
- Scala
- IDE: Intellij

HDP Sandbox:
http://hortonworks.com/hdp/downloads/
(Did set it up but didn't end up using it)

### Additional notes:
- IP addresses do not guarantee distinct users, but this is the limitation of the data. As a bonus, consider what additional data would help make better analytical conclusions
  - Using IP with port number, port number should signify unique connection
  - If IP:port are same then parsing the user agent to check Brower type/ OS type
- For this dataset, complete the sessionization by time window rather than navigation. Feel free to determine the best session window time on your own, or start with 15 minutes.
  - I've used 30mins as per https://support.google.com/analytics/answer/2731565?hl=en
- The log file was taken from an AWS Elastic Load Balancer:
http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-format
