package com.stratio.streaming

import scalaj.http.{Http, HttpOptions, HttpRequest, HttpResponse}

class RecommenderRequests {

}

object RecommenderRequests{
  def sendRating(rating: Rating, host: String, port: Int): HttpResponse[String] ={
    val bytesToSend = s"${rating.item},${rating.rating}"
    val url = s"http://${host}:${port}/${rating.user}/ratings"
    val result = Http("http://localhost:5432/0/ratings").postData(bytesToSend).option(HttpOptions.readTimeout(10000)).asString
    result

  }


  def oldMain(args: Array[String]): Unit = {
    val bytesToSend = """260,9
                                                                    1,8
                                                                    16,7
                                                                    25,8
                                                                    32,9
                                                                    335,4
                                                                    379,3
                                                                    296,7
                                                                    858,10
                                                                    50,8""".toCharArray.map(_.toByte)
    val result = Http("http://localhost:5432/0/ratings").postData(bytesToSend)
      //.header("Content-Type", "application/json")
      //.header("Charset", "UTF-8")
      .option(HttpOptions.readTimeout(10000)).asString
    println(result)
  }

  def main(args: Array[String]): Unit = {
    val rating = Rating("adasdsa",0,1,4)
    val result = sendRating(rating, "localhost", 5432)
    println(result.code)
  }
}
