import org.apache.spark.sql.SparkSession

//TODO: upgrade to a compile safe API (e.g. pureconfig)
trait SparkSessionCreator extends ConfigurationCreator {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master(config.getString("spark-submit.master"))
      .appName("Is retweet tweets classifier")
      .getOrCreate()
  }
}