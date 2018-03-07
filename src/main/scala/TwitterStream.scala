import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import TwitterSetup.setupTwitter
import twitter4j.FilterQuery
import com.typesafe.scalalogging.LazyLogging

object TwitterStream extends LazyLogging {
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))

    // Create a DStream from Twitter using our streaming context
    val filteredQuery = new FilterQuery().track("blockchain", "btc", "crypto", "cryptocurrency",
      "bitcoin", "ICO", "Ethereum", "altcoin", "dogecoin", "cryptomemes")

    //val tweets = TwitterUtils.createStream(ssc, None)
    val tweets = TwitterUtils.createFilteredStream(ssc, None, Some(filteredQuery))

    // Now extract the text of each status update into RDD's using map()
    val statuses = tweets
      .map(status => status.getText())

    statuses.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
