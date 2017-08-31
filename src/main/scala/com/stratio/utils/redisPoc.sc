import com.redis.RedisClient
import org.apache.spark.SparkConf

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












