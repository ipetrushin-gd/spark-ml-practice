import scala.io.Source
import java.io.InputStream

object TwitterSetup {
  def setupTwitter() = {
    val stream : InputStream = getClass.getResourceAsStream("creds.txt")
    for (line <- Source.fromInputStream(stream).getLines) {
      val fields = line.split("=")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }
}