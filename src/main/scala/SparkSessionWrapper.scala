import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("yarn")
      .appName("Is retweet tweets classifier")
      .getOrCreate()
  }
}