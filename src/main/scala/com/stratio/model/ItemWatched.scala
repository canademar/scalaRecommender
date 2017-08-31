package com.stratio.model

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

case class ItemWatched(timestamp: Timestamp, item: Int, user: Int) {

}
