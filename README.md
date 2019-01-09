# KafkaToSparkStreaming
This application demonstrate how to fetch data from Kafka topic, get sentiments of tweets and aggregate count based on sentiment.

## Dependencies:

Scala 2.11.x

Spark 2.3.2

## Building Jar:
Package as a FAT jar file.

 $ sbt clean
 
 $ sbt complie
 
 $ sbt assembly

## To run the application:
$ spark-submit --master local --class KafkaToDashboard.Kafka2Dashboard --conf spark.ui.port=12901 target/scala-2.11/KafkaToDashboard-assembly-0.1.jar prod

## Architecture:

![alt text](https://raw.githubusercontent.com/sdp1992/KafkaToSparkStreaming/master/diagram.png)
