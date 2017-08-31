package com.stratio.clustering

import com.stratio.model.ItemWatched
import com.stratio.streaming.RecommenderRequests
import com.stratio.streaming.StreamingReceiver.parseStringRecommendation
import com.stratio.utils.RedisUtils
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

import scala.util.parsing.json.JSON

object ClusteringService {
  def main(args: Array[String]): Unit = {
    val master = "local[*]"
    val appName = "clustering"
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)
    val ratingsPath = "/home/cnavarro/workspace/emplea2015/streaming_test/streamingRecommend/src/main/resources/ml-latest-small/ratings.csv"

    LogManager.getRootLogger.setLevel(Level.ERROR)

    val redisUtils = new RedisUtils("localhost", 6379)
    val usersInClusters = KMClustering.createUsersInClusters(sc, ratingsPath)

    val users = 1 to 671

    for(user<-users){
      //println("User "+user+" is in cluster "+usersInClusters(user))
      val cluster = usersInClusters(user)
      println(user+","+cluster)
      redisUtils.assignCluster(user, cluster)
    }

    val ssc = new StreamingContext(sc, Seconds(1))
    val brokerList = "localhost:9092"
    val kafkaParams = Map[String, String]("metadata.broker.list"->brokerList)
    val topics = "itemViews".split(",").toSet



    val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)

    /*// FileWriter
    val file = new File("text.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("text")*/


    var watchedMap : scala.collection.mutable.Map[Int, scala.collection.mutable.Map[Int, Int]]= scala.collection.mutable.Map()


    directKafkaStream.foreachRDD {
      rdd=>{
        if(!rdd.isEmpty()){
          val valueString = rdd.collect().toSet.head.asInstanceOf[Tuple2[String, String]]._2
          val itemWatched = parseStringItemWatched(valueString)
          val item = itemWatched.item
          val user = itemWatched.user
          val cluster = usersInClusters(user)
          //RecommenderRequests.sendRating(rating, host, port)
          println("User "+ user +" in cluster: "+cluster)
          if(watchedMap.contains(cluster)){
            if(watchedMap(cluster).contains(item)) {
              val countMap: scala.collection.mutable.Map[Int, Int] = watchedMap(cluster)
              val currentViews: Int = countMap(item) + 1
              watchedMap(cluster)(item) = currentViews
            }else{
              watchedMap(cluster)(item) = 1
            }
          }else{
            watchedMap(cluster) = scala.collection.mutable.Map(item-> 1)
          }
          redisUtils.markView(itemWatched, cluster)
          println("In cluster "+cluster+" there are "+watchedMap(cluster)(itemWatched.item)+" people viewing "+itemWatched.item)
          //bw.write(valueString)
        }

      }
    }

    ssc.start()
    ssc.awaitTermination


  }


  def parseStringItemWatched(str: String): ItemWatched = {
    val jsonMap : Map[String, Any] = JSON.parseFull(str).getOrElse(Map()).asInstanceOf[Map[String, Any]]
    ItemWatched(jsonMap("timestamp").asInstanceOf[Double].toLong,jsonMap("item").asInstanceOf[Double].toInt,
      jsonMap("user").asInstanceOf[Double].toInt)
  }

}
