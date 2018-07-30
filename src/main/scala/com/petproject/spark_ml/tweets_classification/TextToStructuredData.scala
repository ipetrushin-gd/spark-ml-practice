package com.petproject.spark_ml.tweets_classification

import com.petproject.spark_ml.tweets_classification.FeaturesExtractionFromRawTweet.{extractFeaturesFromRawTweet,
  StructuredTweet}
import com.petproject.spark_ml.tweets_classification.TweetsNormalization.normalizeTweets
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

object TextToStructuredData extends SparkSessionCreator {
  def getStructuredDataFromText(text: Dataset[String]): Dataset[StructuredTweet] = {
    import spark.implicits._

    val aggregatedText = text.agg(concat_ws("\n", collect_list("value"))).as[String]

    val tweets = aggregatedText
      .flatMap(line => line.split("List\\("))

    val nonEmptyTweets = tweets
      .map(_.stripLineEnd)
      .filter(!_.isEmpty)

    val nonEmptyTweetsNormalized = nonEmptyTweets.transform[String](normalizeTweets())

    nonEmptyTweetsNormalized
      .map(extractFeaturesFromRawTweet)
  }
}
