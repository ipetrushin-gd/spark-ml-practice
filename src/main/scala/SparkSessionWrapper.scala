import org.apache.spark.sql.SparkSession

//TODO: upgrade to a compile safe API (e.g. pureconfig)
trait SparkSessionWrapper extends ConfigurationWrapper {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master(config.getString("spark-submit.master"))
      .appName("Is retweet tweets classifier")
      .getOrCreate()
  }
}