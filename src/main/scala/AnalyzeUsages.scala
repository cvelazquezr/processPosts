import org.apache.spark.sql.Encoder
import spark_conf.Context
import scala.collection.mutable
import scala.reflect.ClassTag

// Get basic information about the Java posts in StackOverflow

object AnalyzeUsages extends Context {
  implicit def kryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] =
    org.apache.spark.sql.Encoders.kryo[A](ct)

  def main(args: Array[String]): Unit = {
    val categoryInStudy: String = "json_libraries"

    println("Loading the usages computed ...")
    val usages = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"data/usages/$categoryInStudy.csv")
      .toDF()

    val usagesNoNull = usages.na.drop()
    val librariesMapping = mutable.HashMap[String, Array[String]]()

    usagesNoNull.collect().foreach(row =>  {
      val title: String = row.getAs[String]("Title")
      val libraries: Array[String] = row.getAs[String]("Libraries").split("\\|")

      var maxValue: Int = 0
      libraries.foreach(library =>  {
        val librarySplitted: Array[String] = library.split("->")
        val value: Int = librarySplitted(1).toInt
        if (value > maxValue)
          maxValue = value
      })

      if (maxValue > 1) {
        libraries.foreach(library =>  {
          val librarySplitted: Array[String] = library.split("->")
          val libraryName: String = librarySplitted(0)
          val value: Int = librarySplitted(1).toInt

          if (value == maxValue) {
            if (librariesMapping.contains(libraryName)) {
              librariesMapping(libraryName) :+= title
            } else {
              librariesMapping(libraryName) = Array(title)
            }
          }
        })
      }
    })

    librariesMapping.foreach(item => {
      println(s"Library: ${item._1}")
      item._2.foreach(println)
      println()
    })

    sparkSession.stop()
  }
}
