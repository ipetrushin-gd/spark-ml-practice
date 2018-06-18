import org.apache.spark.sql.functions._
import TweetsNormalization.normalizeTweets
import FeaturesExtractionFromRawTweet.extractFeaturesFromRawTweet
import org.apache.spark.sql.{DataFrame, Dataset}

object TextToStructuredData extends SparkSessionCreator {
  def getStructuredDataFromText(text: Dataset[String]): DataFrame = {
    import spark.implicits._

    val aggregatedText = text.agg(concat_ws("\n", collect_list("value"))).as[String]

    val tweets = aggregatedText
      .flatMap(line => line.split("List\\("))

    val nonEmptyTweets = tweets
      .map(line => line.stripLineEnd)
      .filter(line => line.length() > 0)

    val nonEmptyTweetsNormalized = nonEmptyTweets.transform[String](normalizeTweets())

    nonEmptyTweetsNormalized
      .map(extractFeaturesFromRawTweet)
      .withColumn("id", monotonically_increasing_id())
  }
}
