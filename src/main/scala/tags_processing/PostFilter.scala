package tags_processing

import java.io.{File, PrintWriter}

import org.apache.spark.sql.Encoder
import spark_conf.Context
import org.apache.spark.sql.functions._

import scala.collection.immutable.ListMap
import scala.collection.mutable

// Extract the most frequent pairs of tags per category

import scala.reflect.ClassTag

case class Post(question_Id: Int = 0,
                answer_Id: Int = 0,
                title: String = "",
                tags: Array[String] = Array(),
                body: String = "") {
  override def toString: String = s"$question_Id,$answer_Id,$title,${tags.mkString("|")},$body"
}

object PostFilter extends Context {
  implicit def kryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] =
    org.apache.spark.sql.Encoders.kryo[A](ct)

  def main(args: Array[String]): Unit = {
    println("Loading selected posts ...")
    val selectedPosts = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/categories/selectedPosts.csv")

    println("Loading all the StackOverflow posts ...")
    val dataPosts = sparkSession
      .read
      .option("quote", "\"")
      .option("escape", "\"")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/posts/java_posts.csv")
    val dataNoNull = dataPosts.na.drop()

    println("Reading processed file ... ")
    val categoryInStudy: String = "i_o_utilities"
    val categoryInformation = selectedPosts.where(col("Category") === categoryInStudy)

    println("Joining data ...")
    val joinedData = categoryInformation
      .join(dataNoNull, "Question_ID")
      .drop(dataNoNull.col("Question_ID"))
      .dropDuplicates()

    println("Getting tags ...")
    val excludedListTags: Array[String] = Array("java", "android", "c++", "c", "c#", "eclipse", "intellij", "java-8")
    val tags = joinedData.select("Tags").dropDuplicates()

    println("Extracting bigrams from tags ...")
    val tagsBigrams = tags.flatMap(row => {

      val tagsArray: Array[String] = row.getAs[String]("Tags")
        .replaceAll("<", "")
        .replaceAll(">", " ")
        .trim.split(" ")
        .filter(tag => !excludedListTags.contains(tag))

      val combinations: Array[Array[String]] = tagsArray.combinations(2).toArray[Array[String]]
      val combinationsGenerated: Array[String] = combinations.map(arr => {
        val firstValue: String = arr(0)
        val secondValue: String = arr(1)

        val textOrder: String = if (firstValue > secondValue) secondValue + "," + firstValue
        else firstValue + "," + secondValue

        textOrder
      })
      combinationsGenerated
    })

    println("Counting bigrams ...")
    val counterBigrams = new mutable.HashMap[String, Int]()
    tagsBigrams.collect().foreach(bigram => {

      if (counterBigrams.contains(bigram)) {
        counterBigrams(bigram) += 1
      } else {
        counterBigrams(bigram) = 1
      }
    })

    val descendentOrder: ListMap[String, Int] = ListMap(counterBigrams.toSeq.sortWith(_._2 > _._2): _*)
    val frequentTags = descendentOrder.filter(_._2  > 5).flatMap(_._1.split(",")).toArray

    println(frequentTags.toSet.size)

    val pw: PrintWriter = new PrintWriter(new File(s"data/results/tags_selection/${categoryInStudy}_bigrams.txt"))
    descendentOrder.filter(_._2 > 5).foreach(pair => {
      pw.write(s"${pair._1}\n")
    })
    pw.close()

    sparkSession.close()
  }
}
