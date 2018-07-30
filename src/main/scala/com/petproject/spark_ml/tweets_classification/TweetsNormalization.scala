package com.petproject.spark_ml.tweets_classification

import org.apache.spark.sql.Dataset

object TweetsNormalization extends SparkSessionCreator {
  def normalizeTweets()(tweets: Dataset[String]) : Dataset[String] = {
    import spark.implicits._

    val normalizedTweets = tweets
      .map(line => line
        .replaceAll(", true\\)", ", 1")
        .replaceAll(", false\\)", ", 0")
        .replaceAll(", und, ", ", ??, ")
      )

    normalizedTweets
  }
}
