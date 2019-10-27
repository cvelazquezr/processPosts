package utils

object Utils {
  // Split the camelCase style e.g.(CamelCase => camel case)
  def splitCamelCase(token: String): String = {
    token.replaceAll(String.format("%s|%s|%s",
      "(?<=[A-Z])(?=[A-Z][a-z])",
      "(?<=[^A-Z])(?=[A-Z])",
      "(?<=[A-Za-z])(?=[^A-Za-z])"), " ").toLowerCase.trim
  }
}
