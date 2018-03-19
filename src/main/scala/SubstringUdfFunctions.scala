import org.apache.spark.sql.functions.udf

object SubstringUdfFunctions {
  def substringFunctionText(line: String) : String = {
    line.substring(0, line.length() - 7)
  }
  val substringUdfText = udf(substringFunctionText _)

  def substringFunctionLanguage(line: String) : String = {
    line.substring(line.length() - 5, line.length() - 3)
  }
  val substringUdfLanguage = udf(substringFunctionLanguage _)

  def substringFunctionIsRT(line: String) : String = {
    line.substring(line.length() - 1)
  }
  val substringUdfIsRT = udf(substringFunctionIsRT _)
}
