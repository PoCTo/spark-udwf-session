# spark-udwf-session
a spark custom window function example, to generate session IDs

A forked version of the UDWF from rcongiu. Here sessions numbers are non-random successive integers.

Usage
--------------------

Implements a custom window function to create session IDs on user activity.

Sessionization is a common calculation when processing user activity. We want to mark all events belonging to a session
if between them there's no time gap greater than T.

Will continue existing session if there is one in the data, or create one if the next event is 
outside the session duration interval. 


```
// Window specification
val specs = Window.partitionBy(f.col("user")).orderBy(f.col("ts").asc)
// create the session
val res = df.withColumn( "newsession", 
   calculateSession(f.col("ts"), f.col("session"), 
        f.lit(30*60**1000) over specs)  // window duration in ms

```

result similar to this (UUIDs are randomly generated).

```
+-----+-------------+------------------------------------+------------------------------------+
|user |ts           |session                             |newsession                          |
+-----+-------------+------------------------------------+------------------------------------+
|user1|1509036478537|f237e656-1e53-4a24-9ad5-2b4576a4125d|0                                   |
|user1|1509037078537|null                                |0                                   |
|user1|1509037378537|null                                |1                                   |
|user1|1509044878537|null                                |1                                   |
|user1|1509046078537|null                                |2                                   |
|user2|1509036778537|null                                |0                                   |
|user2|1509037378537|null                                |0                                   |
+-----+-------------+------------------------------------+------------------------------------+

```


See http://blog.nuvola-tech.com/2017/10/spark-custom-window-function-for-sessionization/ for 
a detailed explanation.
