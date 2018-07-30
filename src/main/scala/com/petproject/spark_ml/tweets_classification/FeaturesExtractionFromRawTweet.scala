package com.petproject.spark_ml.tweets_classification

object FeaturesExtractionFromRawTweet {
  case class StructuredTweet(text: String, language: String, label: Int)

  def extractFeaturesFromRawTweet(tweetInRawFormat: String) : StructuredTweet = {
    val tweetLength = tweetInRawFormat.length()

    StructuredTweet(tweetInRawFormat.substring(0, tweetLength - 7),
      tweetInRawFormat.substring(tweetLength - 5, tweetLength - 3),
      tweetInRawFormat.substring(tweetLength - 1).toInt)
  }
}
