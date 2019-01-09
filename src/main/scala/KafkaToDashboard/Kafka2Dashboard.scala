package KafkaToDashboard

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._


object Kafka2Dashboard {


  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.load
    val envProps: Config = conf.getConfig(args(0))
    val sparkConf = new SparkConf().setMaster("local").setAppName("SiteTraffic")
    val streamingContext = new StreamingContext(sparkConf, Seconds(envProps.getInt("window")))
    streamingContext.sparkContext.setLogLevel("WARN")

    val topicsSet = Set("TWEETS_NAMORAGA")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> envProps.getString("bootstrap.server"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val tweetData: DStream[String] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    ).map(record => record.value)


    val tweetList = tweetData.map(line => {
      val namo_strings = List("narendra", "modi", "namo", "narendra modi")
      val raga_strings = List("rahul", "gandhi", "raga", "rahul gandhi")
      if (namo_strings.exists(line.toLowerCase.contains)) {
        val tweet = "Narendra Modi"
        (tweet, 1)
      } else if (raga_strings.exists(line.toLowerCase.contains)) {
        val tweet = "Rahul Gandhi"
        (tweet, 1)
      } else {
        val tweet = "Undefined"
        (tweet, 1)
      }
    }).reduceByKey(_ + _)

    val tweetSentiment = tweetData.map(line => {

          val namo_strings = List("narendra", "modi", "namo", "narendra modi")
          val raga_strings = List("rahul", "gandhi", "raga", "rahul gandhi")

          if (namo_strings.exists(line.toLowerCase.contains)) {

            val sentiment = GetSentiment.GetSentiment(line)

              if (sentiment==1){
                val temp = "Narendra Modi Positive"
                (temp, 1)
              }else{
                val temp = "Narendra Modi Negative"
                (temp, 1)
              }

          }else if (raga_strings.exists(line.toLowerCase.contains)) {

            val sentiment = GetSentiment.GetSentiment(line)
            if (sentiment==1){
              val sentimentType = "Rahul Gandhi Positive"
              (sentimentType, 1)
            }else{

              val sentimentType = "Rahul Gandhi Negative"
              (sentimentType, 1)

            }
          }else {
              val sentimentType = "Undefined"
            (sentimentType, 1)

          }
    }).reduceByKey(_ + _)


    tweetList.foreachRDD { rdd =>

      val namo_count_1 = rdd.filter( x => x._1=="Narendra Modi" ).map( y => y._2)
      val namo_count_2 = namo_count_1.collect()

      val raga_count_1 = rdd.filter( x => x._1=="Rahul Gandhi" ).map( y => y._2)
      val raga_count_2 = raga_count_1.collect()


      val namo_count_3 = "[" + namo_count_2.mkString("") + "]"
      val raga_count_3 = "[" + raga_count_2.mkString("") + "]"

      val namo_count_4 = if(namo_count_3 == "[]") "[0]" else namo_count_3
      val raga_count_4 = if(raga_count_3 == "[]") "[0]" else raga_count_3

      println("Tweets related to Narendra Modi" + namo_count_4)
      println("Tweets related to Rahul Gandhi" + raga_count_4)

      SendCount.SendCount(namo_count_4, raga_count_4)

    }


    tweetSentiment.foreachRDD { rdd =>

      val npsc_1 = rdd.filter(x => x._1 == "Narendra Modi Positive").map(y => y._2)
      val nnsc_1 = rdd.filter(x => x._1 == "Narendra Modi Negative").map(y => y._2)
      val rpsc_1 = rdd.filter(x => x._1 == "Rahul Gandhi Positive").map(y => y._2)
      val rnsc_1 = rdd.filter(x => x._1 == "Rahul Gandhi Negative").map(y => y._2)


      val npsc_2 = npsc_1.collect()
      val nnsc_2 = nnsc_1.collect()
      val rpsc_2 = rpsc_1.collect()
      val rnsc_2 = rnsc_1.collect()


      val npsc_3 = "[" + npsc_2.mkString("") + "]"
      val nnsc_3 = "[" + nnsc_2.mkString("") + "]"
      val rpsc_3 = "[" + rpsc_2.mkString("") + "]"
      val rnsc_3 = "[" + rnsc_2.mkString("") + "]"

      val npsc_4 = if (npsc_3 == "[]") "[0]" else npsc_3
      val nnsc_4 = if (nnsc_3 == "[]") "[0]" else nnsc_3
      val rpsc_4 = if (rpsc_3 == "[]") "[0]" else rpsc_3
      val rnsc_4 = if (rnsc_3 == "[]") "[0]" else rnsc_3

      println("Positive tweets related to Narendra Modi" + npsc_4)
      println("Negative tweets related to Narendra Narendra Modi" + nnsc_4)
      println("Positive tweets related to Rahul Gandhi" + rpsc_4)
      println("Negative tweets related to Rahul Gandhi" + rnsc_4)

      SendCount.SendSentimentCount(npsc_4, nnsc_4, rpsc_4, rnsc_4)
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
