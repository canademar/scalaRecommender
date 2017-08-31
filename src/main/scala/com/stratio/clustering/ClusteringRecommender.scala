package com.stratio.clustering

import org.apache.spark._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

class ClusteringRecommender {


}

object ClusteringRecommender{
  def main(args: Array[String]): Unit = {
    // Loads data.
    val master = "local[*]"
    val appName = "clustering"
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)
    // Load and parse the data
    val data = sc.textFile("/home/cnavarro/workspace/emplea2015/streaming_test/streamingRecommend/src/main/resources/kmeans.data")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    println(clusters.toPMML())
    val prediction = clusters.predict(Vectors.dense(1,1,1))
    println("Prediction: "+prediction)
    // Save and load model
    //clusters.save(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
    //val sameModel = KMeansModel.load(sc, "target/org/apache/spark/KMeansExample/KMeansModel")
  }

}
