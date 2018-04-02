import com.typesafe.config.ConfigFactory

trait ConfigurationWrapper {
  lazy val config = ConfigFactory.parseResources("application.conf")
}
