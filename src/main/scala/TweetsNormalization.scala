import org.apache.spark.rdd.RDD

object TweetsNormalization {
  def normalizeTweets(tweets: RDD[String]) : RDD[String] = {
    val normalizedTweets = tweets
      .map(line => line
        .replaceAll(", true\\)", ", 1")
        .replaceAll(", false\\)", ", 0")
        .replaceAll(", und, ", ", ??, ")
      )

    normalizedTweets
  }
}
