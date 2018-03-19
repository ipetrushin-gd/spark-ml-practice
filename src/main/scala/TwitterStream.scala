import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import TwitterSetup.setupTwitter
import twitter4j.FilterQuery
import com.typesafe.scalalogging.LazyLogging

object TwitterStream extends LazyLogging {
  def main(args: Array[String]) {

    setupTwitter()
    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))

    val filteredQuery = new FilterQuery().track("blockchain", "btc", "crypto", "cryptocurrency",
      "bitcoin", "ICO", "Ethereum", "altcoin", "dogecoin", "cryptomemes")

    val tweets = TwitterUtils.createFilteredStream(ssc, None, Some(filteredQuery))

    val statuses = tweets
      .map(status => status.getText())

    statuses.print()
    statuses.repartition(1).saveAsTextFiles("/user/ilos/tweets/tweets_for_timestpamp", "")

    ssc.start()
    ssc.awaitTermination()
  }

}
