import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}

object LoggerSetup {
  /** Makes sure all INFO messages get logged. */
  def setupLogging() = {
    val loggerLevel = ConfigFactory.load().getString("loggerLevel")
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.toLevel(loggerLevel))
  }
}
