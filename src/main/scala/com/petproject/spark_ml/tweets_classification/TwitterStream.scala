package com.petproject.spark_ml.tweets_classification

import com.petproject.spark_ml.tweets_classification.TwitterSetup.setupTwitter
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.FilterQuery

object TwitterStream extends LazyLogging with ConfigurationCreator {
  def main(args: Array[String]) {

    setupTwitter()
    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))

    val filteredQuery = new FilterQuery().track("blockchain", "btc", "crypto", "cryptocurrency",
      "bitcoin", "ICO", "Ethereum", "altcoin", "dogecoin", "cryptomemes")

    val tweets = TwitterUtils.createFilteredStream(ssc, None, Some(filteredQuery))

    val statuses = tweets
      .map(status => Seq(status.getText(), status.getLang(), status.isRetweet()))

    statuses.foreachRDD {
      rdd => if (!rdd.isEmpty()) rdd.collect().foreach {
        element => logger.debug(element.mkString(", "))
      }
    }
    //TODO: fix output file format to CSV/JSON
    statuses.repartition(1).saveAsTextFiles(config.getString("output.path"), "")

    ssc.start()
    ssc.awaitTermination()
  }

}
