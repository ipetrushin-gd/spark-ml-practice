import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.functions._

object IsRetweetClassifier extends LazyLogging {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Is retweet tweets classifier")
      .master("yarn")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val text = spark
      .sparkContext.wholeTextFiles("hdfs:///user/ilos/tweets/tweets.txt")
      .map { case (filename, content) => content}

    val tweets = text
      .flatMap(line => line.split("List\\("))

    val nonEmptyTweets = tweets
      .map(line => line.stripLineEnd)
      .filter(line => line.length() > 0)

    val nonEmptyTweetsNormalized = nonEmptyTweets
      .map(line => line.replaceAll(", true\\)", ", 1"))
      .map(line => line.replaceAll(", false\\)", ", 0"))
      .map(line => line.replaceAll(", und, ", ", ??, "))

    val structuredDataRDD = nonEmptyTweetsNormalized.map(line => Row(
      line.substring(0, line.length() - 7),
      line.substring(line.length() - 5, line.length() - 3),
      line.substring(line.length() - 1).toInt
    ))

    val schema = new StructType()
      .add(StructField("text", StringType, nullable = false))
      .add(StructField("language", StringType, nullable = false))
      .add(StructField("label", IntegerType, nullable = false))

    val structuredData = spark
      .createDataFrame(structuredDataRDD, schema)
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

    logger.info("Model accuracy on test data: " + testAccuracyAsIntegerNumberOfPercents.toString)
  }
}
