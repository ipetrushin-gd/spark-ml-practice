package com.petproject.spark_ml.tweets_classification

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}

object IsRetweetClassifier extends LazyLogging with SparkSessionCreator {
  def main(args: Array[String]) {
    import spark.implicits._

    val text = spark.read.textFile(config.getString("input.path"))
    val structuredData = TextToStructuredData.getStructuredDataFromText(text)

    val trainLength = (structuredData.count() * 0.8).toInt
    val testLength = (structuredData.count() - trainLength).toInt

    val train = structuredData.limit(trainLength)
    val test = structuredData.except(train)

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
