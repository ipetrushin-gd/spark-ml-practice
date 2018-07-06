import org.scalatest.FunSpec
import TweetsNormalization.normalizeTweets
import com.typesafe.scalalogging.LazyLogging
import com.github.mrpowers.spark.fast.tests.DatasetComparer

//TODO: switch to more concise and powerful spark-testing-base
class TweetsNormalizationSpec extends FunSpec with SparkSessionCreator with DatasetComparer {
  import spark.implicits._

  it("Normalize tweets checking") {
    val source = spark.createDataset[String](List(
      "Tweet text, en, true)",
      "Tweet text, en, false)",
      "Tweet text, und, true)",
      "Tweet text, und, false)",
      "Tweet text, ru, true)",
      "Tweet text, ru, false)",
      "false true false und en ru, en, false)"
    ))

    val actual = source.transform[String](normalizeTweets())

    val expected = spark.createDataset[String](List(
      "Tweet text, en, 1",
      "Tweet text, en, 0",
      "Tweet text, ??, 1",
      "Tweet text, ??, 0",
      "Tweet text, ru, 1",
      "Tweet text, ru, 0",
      "false true false und en ru, en, 0"
    ))

    assertSmallDatasetEquality(actual, expected)
  }
}
