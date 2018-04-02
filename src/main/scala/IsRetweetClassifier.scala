import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.functions._
import TweetsNormalization.normalizeTweets
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.col
import SubstringUdfFunctions._
import org.apache.spark.sql.types.IntegerType

object IsRetweetClassifier extends LazyLogging with SparkSessionWrapper {
  def main(args: Array[String]) {
    import spark.implicits._

    val config = ConfigFactory.parseResources("application.conf")

    val text = spark.read.textFile(config.getString("input.path"))
    val aggregatedText = text.agg(concat_ws("\n", collect_list("value"))).as[String]

    val tweets = aggregatedText
      .flatMap(line => line.split("List\\("))

    val nonEmptyTweets = tweets
      .map(line => line.stripLineEnd)
      .filter(line => line.length() > 0)

    val nonEmptyTweetsNormalized = nonEmptyTweets.transform[String](normalizeTweets())

    val structuredData = nonEmptyTweetsNormalized
      .withColumn("text", substringUdfText(col("value")))
      .withColumn("language", substringUdfLanguage(col("value")))
      .withColumn("label", substringUdfIsRT(col("value")).cast(IntegerType))
      .drop(col("value"))
      .withColumn("id", monotonically_increasing_id())

    val trainLength = (structuredData.count() * 0.8).toInt
    val testLength = (structuredData.count() - trainLength).toInt

    val train = structuredData.limit(trainLength)
    val test = structuredData.sort($"id".desc).limit(testLength)

    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")

    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

    val model = pipeline.fit(train)

    val testPrediction = model.transform(test)

    val testResult = testPrediction
      .map(sample => if (sample.getAs[Int]("label") == sample.getAs[Double]("prediction").toInt) 1 else 0)
      .reduce(_+_).toFloat / testPrediction.count()

    val testAccuracyAsIntegerNumberOfPercents = (testResult * 100 + 0.5).toInt

    logger.info(s"Model accuracy on test data: $testAccuracyAsIntegerNumberOfPercents")
  }
}
