package com.stratio.streaming

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import _root_.kafka.serializer.StringDecoder
import com.typesafe.config.{Config, ConfigFactory}

import scala.util.parsing.json.JSON



object StreamingReceiver{
  def main(args: Array[String]): Unit = {

    val confFile = new File("/home/cnavarro/workspace/emplea2015/streaming_test/streamingRecommend/src/main/resources/recommender.conf")
    val parsedConf = ConfigFactory.parseFile(confFile)
    val scalaConf : Config = ConfigFactory.load(parsedConf)

    val appName = scalaConf.getString("app_name")
    val master = scalaConf.getString("master")
    val brokerList = scalaConf.getString("kafka_broker_list")
    val host = scalaConf.getString("recommender_host")
    val port = scalaConf.getInt("recommender_port")
    val topics = scalaConf.getString("kafka_topics").split(",").toSet

    /*val appName = "ratings"
    val master = "local[*]"
    val brokerList = "localhost:9092"

    val topics = "ratings".split(",").toSet

    val host = "localhost"
    val port = 5432*/
    val kafkaParams = Map[String, String]("metadata.broker.list"->brokerList)


    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val ssc = new StreamingContext(conf, Seconds(1))

    val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)

    /*// FileWriter
    val file = new File("text.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("text")*/


    directKafkaStream.foreachRDD {
      rdd=>{
        if(!rdd.isEmpty()){
          val valueString = rdd.collect().toSet.head.asInstanceOf[Tuple2[String, String]]._2
          val rating = parseStringRecommendation(valueString)
          RecommenderRequests.sendRating(rating, host, port)
          //bw.write(valueString)
        }

      }
    }

   /* val lines = directKafkaStream.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()*/

    ssc.start()
    ssc.awaitTermination
    //bw.close()

  }

  def parseStringRecommendation(str: String): Rating = {
    val jsonMap : Map[String, Any] = JSON.parseFull(str).getOrElse(Map()).asInstanceOf[Map[String, Any]]
    val rating = Rating(jsonMap("timestamp").toString,jsonMap("user").asInstanceOf[Double].toInt,
      jsonMap("item").asInstanceOf[Double].toInt, jsonMap("rating").asInstanceOf[Double])
    rating


  }

}
