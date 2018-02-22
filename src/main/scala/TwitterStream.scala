import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.Level
import Utilities._
import twitter4j.FilterQuery

object TwitterStream {
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val filteredQuery = new FilterQuery().track("blockchain") //.locations(boundingBoxes : _*)

    //val tweets = TwitterUtils.createStream(ssc, None)
    val tweets = TwitterUtils.createFilteredStream(ssc, None, Some(filteredQuery))

    // Now extract the text of each status update into RDD's using map()
    val statuses = tweets
      .map(status => status.getText())
      //.filter(_.contains("blockchain"))

    statuses.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
