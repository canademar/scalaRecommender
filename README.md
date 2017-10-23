# recommender
# recommender
## Streaming Service for Recommender
java -cp target/scala-2.11/streamingRecommend-assembly-0.2.jar com.stratio.streaming.StreamingReceiver src/main/resources/recommender.conf

##Streaming Service for User Clustering
java -cp target/scala-2.11/streamingRecommend-assembly-0.2.jar com.stratio.clustering.ClusteringService src/main/resources/recommender.conf
