import com.typesafe.config.ConfigFactory

trait ConfigurationCreator {
  lazy val config = ConfigFactory.parseResources("application.conf")
}
