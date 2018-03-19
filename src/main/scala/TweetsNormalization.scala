import org.apache.spark.sql.{Dataset, SparkSession}

object TweetsNormalization {
  def normalizeTweets(tweets: Dataset[String]) : Dataset[String] = {
    val spark = SparkSession.builder().getOrCreate()
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
