name := "streamingRecommend"

version := "0.2"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.1" //% "provided"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.2.0"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.1.1" //% "provided"

libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.8.2.2"

libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "2.3.0"

libraryDependencies += "com.typesafe" % "config" % "1.3.1"

libraryDependencies +=   "net.debasishg" %% "redisclient" % "3.4"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.4.0"


mainClass in (Compile, run) := Some("com.stratio.streaming.StreamingReceiver")

mainClass in assembly := Some("com.stratio.streaming.StreamingReceiver")

/*assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case "META-INF/MANIFEST.MF"                        => MergeStrategy.discard
  case x => MergeStrategy.first

}
*/
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  //case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  //case PathList("META-INF", ps @ _*) => MergeStrategy.first
  case _ => MergeStrategy.first
}

        