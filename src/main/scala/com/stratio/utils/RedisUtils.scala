package com.stratio.utils

import com.redis.RedisClient
import com.stratio.model.ItemWatched

class RedisUtils(val redisClient: RedisClient) {

  def this(host: String, port: Int){
    this(new RedisClient(host, port))
  }

  def markView(itemWatched: ItemWatched, cluster: Int): Unit ={
    val key = s"cluster_${cluster}_item_${itemWatched.item}_user_${itemWatched.user}"
    redisClient.setex(key, 3600, 1)
  }

  def assignCluster(user: Int, cluster: Int): Unit ={
    redisClient.hset("clusterAssignment", user, cluster)
  }


}

object RedisUtils{

  def markView(host : String, port : Int, itemWatched: ItemWatched, cluster: Int): Unit ={
    val r = new RedisClient("localhost", 6379)
    val ru = new RedisUtils(r)
    ru.markView(itemWatched, cluster)
  }

  def obtainClusterViewCounts(): Map[String, Map[String,Int]] ={
    val r = new RedisClient("localhost", 6379)
    r.setex("cluster0", 30, 1)
    val keys = r.keys()

    val redis = new RedisClient("localhost", 6379)

    val clusterKeys = redis.keys("cluster_*").get.map(_.get)

    val clusterMovies = clusterKeys.map(k=> {
      val parts = k.split("_")
      (parts(1),parts(3))

    })

    val text = "I have no idea how this idea workss"


    text.split("\\W+").foldLeft(Map.empty[String, Int]){
      (count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
    }


    val mapClusters = clusterMovies.groupBy(x=>x._1)

    val mapCluster = mapClusters("0").groupBy(x=>x._2).mapValues(_.size)
    val mapCluster1 = mapClusters("1").groupBy(x=>x._2).mapValues(_.size)


    val qFinalCounts = for(item<-mapClusters.keys) yield (item, mapClusters(item).groupBy(x=>x._2).mapValues(_.size))

    val clusterMovieCounts = qFinalCounts.groupBy(_._1).mapValues(x=>x.last._2)
    clusterMovieCounts

  }

  def main(args: Array[String]): Unit = {
    val r = new RedisUtils("localhost", 6379)
    println("Assigning")
    r.assignCluster(671,100)
    println("Done")

  }

}
