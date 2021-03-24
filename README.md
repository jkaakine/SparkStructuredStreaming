# Spark structured streaming

## Overview

This is mock data pipeline which reads streams of data from IoT devices and then aggregates it.

## Architecture

IoT devices are represented as Python scripts ` device.py` script can be found in `data-generator` folder. Device records trips and using Kafka Producer API it sends data to locally running Kafka "cluster" with one topic called ` location`.  Spark is subscribed to this topic and will do basic aggregation to the dataframe and print it out to console in 10 seconds batches.

### Data format

```python
msg= f"{time()},{device},{start_x},{start_y},{end_x},{end_y}"
kafka_producer.send('location', bytes(msg, encoding='utf8'))
```

## Spark

Spark is reads data streams from Kafka location topic. Finally after some data wrangling it will generate following data frames: 

```scala
		val aggDF = distanceDF
			.groupBy("device")
			.agg(
				sum("distance").as("Total distance"),
				avg("distance").as("Average trip distance"),
				min("distance").as("Shortest Trip"),
				max("distance").as("Longest Trip"),
				count(lit(1).as("Number of trips"))
			)
```

Write stream is currently setup to console with 10 second trigger time. 

Output to console should look something in lines of

```bash
Batch: 5
-------------------------------------------
+-------+--------------+---------------------+-------------+------------+-----------------------------+
| device|Total distance|Average trip distance|Shortest Trip|Longest Trip|count(1 AS `Number of trips`)|
+-------+--------------+---------------------+-------------+------------+-----------------------------+
|device3|        1600.0|    29.09090909090909|          3.0|        65.0|                           55|
|device1|        2783.0|    35.67948717948718|          4.0|        85.0|                           78|
|device2|        2071.0|   33.950819672131146|          5.0|        74.0|                           61|
+-------+--------------+---------------------+-------------+------------+-----------------------------+

-------------------------------------------
Batch: 6
-------------------------------------------
+-------+--------------+---------------------+-------------+------------+-----------------------------+
| device|Total distance|Average trip distance|Shortest Trip|Longest Trip|count(1 AS `Number of trips`)|
+-------+--------------+---------------------+-------------+------------+-----------------------------+
|device3|        2198.0|     29.7027027027027|          3.0|        65.0|                           74|
|device1|        3566.0|   35.306930693069305|          4.0|        85.0|                          101|
|device2|        2748.0|   33.925925925925924|          4.0|        74.0|                           81|
+-------+--------------+---------------------+-------------+------------+-----------------------------+
```

## Start guide

1. Start Kafka environment

   ```bash
   #Zookeeper
   $ bin/zookeeper-server-start.sh config/zookeeper.properties
   #Kafka broker
   $ bin/kafka-server-start.sh config/server.properties
   #Create topic called location
   $ bin/kafka-topics.sh --create --topic location --bootstrap-server localhost:9092
   ```

2. Package up the scala file ` Streamhandler.scala`

   ```bash
   #inside /SparkStreamHandler
   sbt package
   ```

   

3. Run spark-submit. I used

   ```bash
   spark-submit --class "StreamHandler" --master "local[*]" --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1" target/scala-2.12/iot-data-stream-handler_2.12-0.1.jar
   ```

   Packages flag will change depending on your Kafka and Spark version. You can search for correct one in [Maven Central Repository](https://search.maven.org).

4. Run data generating device scripts

   ```python
   python device.py device1
   python device.py device2
   python device.py device3
   ```

   