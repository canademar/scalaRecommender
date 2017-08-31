package com.stratio.clustering

import com.stratio.recommender.ALSRecommender
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.rdd.RDD



class KMClustering {

}

object KMClustering{

  def createUsersInClusters(sc: SparkContext, ratingsPath: String): Map[Int, Int] ={
    val alsModel = ALSRecommender.createALSModel(sc, ratingsPath)

    val userFactors = alsModel.userFeatures.map {
      case(id, factor) => (id, Vectors.dense(factor))
    }
    val userVectors = userFactors.map(_._2)

    val numClusters = 5
    val numIterations = 40
    val numRuns = 3

    userVectors.cache()
    val userClusterModel = KMeans.train(userVectors, 6, numIterations, numRuns)

    val userClusters : RDD[(Int, Int)] = userFactors.map{
      rdd=> (rdd._1 ->userClusterModel.predict(rdd._2))
    }

    userClusters.collect().toMap
  }

  def main(args: Array[String]): Unit = {

    val master = "local[*]"
    val appName = "clustering"
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)
    val ratingsPath = "/home/cnavarro/workspace/emplea2015/streaming_test/streamingRecommend/src/main/resources/ml-latest-small/ratings.csv"

    LogManager.getRootLogger.setLevel(Level.ERROR)

    val alsModel = ALSRecommender.createALSModel(sc, ratingsPath)

    val movieFactors = alsModel.productFeatures.map{
      case(id,factor)=>(id,Vectors.dense(factor))
    }
    val movieVectors = movieFactors.map(_._2)

    val userFactors = alsModel.userFeatures.map {
      case(id, factor) => (id, Vectors.dense(factor))
    }
    val userVectors = userFactors.map(_._2)

    val numClusters = 5
    val numIterations = 40
    val numRuns = 3


    /*println("Training")
    movieVectors.cache()
    val movieClusterModel = KMeans.train(movieVectors, numClusters, numIterations, numRuns)
    println("Trained")

    val movie1 = movieVectors.first
    val movieCluster = movieClusterModel.predict(movie1)
    println(movieCluster)

    val predictions = movieClusterModel.predict(movieVectors)
    println(predictions.take(10).mkString(","))

    val trainTestSplitUsers = userVectors.randomSplit(Array(0.6, 0.4), 123)
    val trainUsers = trainTestSplitUsers(0)
    val testUsers = trainTestSplitUsers(1)
    val costsUsers = Seq(2, 3, 4, 5, 6, 7, 8, 9, 10, 20).map { k => (k,
      KMeans.train(trainUsers, numIterations, k,
        numRuns).computeCost(testUsers)) }
    println("User clustering cross-validation:")
    costsUsers.foreach { case (k, cost) => println(f"WCSS for K=$k id $cost%2.2f") }

    val bestK = costsUsers.minBy(_._2)._1

    println("BestK: "+ bestK)
    */
    println("Training")
    userVectors.cache()
    val userClusterModel = KMeans.train(userVectors, 6, numIterations, numRuns)
    println("Trained")

    val oneUser = userVectors.first()
    val oneUserPredictions = userClusterModel.predict(oneUser)
    println("One user: "+oneUser+"\n prediction: "+ oneUserPredictions)
    val userPredictions = userClusterModel.predict(userVectors)
    println(userPredictions.take(10).mkString(","))
    //0,1,1,2,1,1,2,4,4,4
    //1,0,0,4,0,0,5,4,5,5
    //3,3,0,3,0,3,1,0,1,3
    println("User and cluster")
    println("Predictions: " + userPredictions.count())
    println("Factors: " + userFactors.count())
    println("One Factor: "+userFactors.take(10).map(_._1).mkString(","))


    println("All predictions"+userPredictions.collect().mkString(","))
    println("User vectors: " + userVectors.count())
    val tenPredictions = userClusterModel.predict(sc.parallelize(userVectors.take(10)))
    println("Ten predictions length "+tenPredictions.count())
    println(tenPredictions.collect().mkString(","))

    /*userFactors.foreach{
      rdd=>println("User: "+rdd._1+" , prediction: "+ userClusterModel.predict(rdd._2))
    }*/

    val userClusters : RDD[(Int, Int)] = userFactors.map{
      rdd=> (rdd._1, userClusterModel.predict(rdd._2))
    }

  }
}
