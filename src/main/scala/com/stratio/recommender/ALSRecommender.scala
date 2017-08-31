package com.stratio.recommender
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}

class ALSRecommender {


}

object ALSRecommender{

  def createALSModel(sc: SparkContext, ratingsPath: String): MatrixFactorizationModel ={
    val rawData = sc.textFile(ratingsPath).filter(_ != "userId,movieId,rating,timestamp")
    val rawRatings = rawData.map(_.split(",").take(3))
    val ratings = rawRatings.map{
      case Array(user, movie, rating) => Rating(user.toInt,movie.toInt,rating.toDouble)
    }
    ratings.cache()
    val alsModel = ALS.train(ratings, 50, 10, 0.1)
    alsModel
  }

  def main(args: Array[String]): Unit = {
    val master = "local[*]"
    val appName = "clustering"
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)

    val ratingsPath = "/home/cnavarro/workspace/emplea2015/streaming_test/streamingRecommend/src/main/resources/ml-latest-small/ratings.csv"

    /*val rawData = sc.textFile("/home/cnavarro/workspace/emplea2015/streaming_test/streamingRecommend/src/main/resources/ml-latest-small/ratings.csv")
    val rawRatings = rawData.map(_.split(",").take(3))
    val ratings = rawRatings.map{
      case Array(user, movie, rating) => Rating(user.toInt,movie.toInt,rating.toDouble)
    }
    ratings.cache()
    val alsModel = ALS.train(ratings, 50, 10, 0.1)
    */
    val alsModel = createALSModel(sc, ratingsPath)

    val movieFactors = alsModel.productFeatures.map{
      case(id,factor)=>(id,Vectors.dense(factor))
    }
    val movieVectors = movieFactors.map(_._2)

    val userFactors = alsModel.userFeatures.map {
      case(id, factor) => (id, Vectors.dense(factor))
    }
    println("User Factors")
    println(userFactors.collect().mkString(","))
    println("\n\n\n\n\n")
    val userVectors = userFactors.map(_._2)

    val movieMatrix = new RowMatrix(movieVectors)
    val movieMatrixSummary = movieMatrix.computeColumnSummaryStatistics()
    val userMatrix = new RowMatrix(userVectors)
    val userMatrixSummary = userMatrix.computeColumnSummaryStatistics()

    println("Movie factors mean: " + movieMatrixSummary.mean)
    println("Movie factors variance: " + movieMatrixSummary.variance)
    println("User factors mean: " + userMatrixSummary.mean)
    println("User factors variance: " + userMatrixSummary.variance)

  }
}