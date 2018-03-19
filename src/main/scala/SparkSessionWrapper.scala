import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  lazy val spark: SparkSession = {
    val config = ConfigFactory.parseResources("application.conf")

    SparkSession
      .builder()
      .master(config.getString("spark-submit.master"))
      .appName("Is retweet tweets classifier")
      .getOrCreate()
  }
}