import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, reverse}

object Main {
  def main(args: Array[String]): Unit = {
    val prefix = "src/main/resources/"
    val csvFile = s"${prefix}file.csv"
    val spark = SparkSession.builder.appName("Data mining app")
      .master("local")
      .getOrCreate()
    val df = spark.read
      .options(Map("inferSchema" -> "true", "header" -> "true"))
      .csv(csvFile)
      .cache()
//    df.groupBy(col("Town/City")).count().show()
//    df.groupBy(col("County")).count().show()
    val ukCounties = df.select("County").distinct() // какие области в Англии
    val topMostExpensiveByCounty = df.groupBy(col("County"))
      .max("Price")
      .sort(reverse(col("max(Price)")))
    topMostExpensiveByCounty.write.csv(prefix + "res")
    spark.stop()
  }
}