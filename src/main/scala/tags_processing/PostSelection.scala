package tags_processing

import java.io.{File, PrintWriter}
import org.apache.spark.sql.Encoder

import scala.io.Source
import spark_conf.Context
import tags_processing.PreprocessTags.linesOfCode

// Given a previous selection of the tags, returns the posts that contains those tags

import scala.reflect.ClassTag

case class Post(question_Id: Int = 0,
                answer_Id: Int = 0,
                title: String = "",
                tags: Array[String] = Array(),
                body: String = "") {
  override def toString: String = s"$question_Id,$answer_Id,$title,${tags.mkString("|")},$body"
}

object PostSelection extends Context {
  implicit def kryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] =
    org.apache.spark.sql.Encoders.kryo[A](ct)

  def main(args: Array[String]): Unit = {
    println("Loading usages detected for the categories ...")
    val usagesCategory = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/categories/categories_similarities.csv")

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
    val categoryInStudy: String = "json_libraries"
    val categoryInformation = usagesCategory.where(usagesCategory.col("Library") === categoryInStudy)

    println("Joining data ...")
    val joinedData = categoryInformation
      .join(dataNoNull, "Answer_ID")
      .drop(dataNoNull.col("Answer_ID"))
      .drop(dataNoNull.col("Question_ID"))
      .drop(dataNoNull.col("Title"))
      .dropDuplicates()
      .filter(row => linesOfCode(row.getAs[String]("Body")))

    val source = Source.fromFile(s"data/results/tags_selection/$categoryInStudy.txt")

    val selectedTags = source.getLines().map(line => {
      val splittedLine: Array[String] = line.split(",")

      if (splittedLine(2).equals("2") && splittedLine(3).equals("2"))
        splittedLine(0) + "," + splittedLine(1)
      else ""
    }).filter(_.nonEmpty).toArray

    println("Filtering posts ...")
    val postsSelected = joinedData.map(row => {
      val questionId: Int = row.getAs[Int]("Question_ID")
      val answerId: Int = row.getAs[Int]("Answer_ID")
      val title: String = row.getAs[String]("Title")
      val body: String = row.getAs[String]("Body")

      val tagsArray: Array[String] = row.getAs[String]("Tags")
        .replaceAll("<", "")
        .replaceAll(">", " ")
        .trim.split(" ")

      val combinations: Array[Array[String]] = tagsArray.combinations(2).toArray[Array[String]]
      val combinationsMerged: Array[String] = combinations.map(arr => {
        arr(0) + "," + arr(1)
      })
      val combinationsFiltered = combinationsMerged.toSet.intersect(selectedTags.toSet)

      if (combinationsFiltered.nonEmpty)
        Post(questionId, answerId, title, tagsArray, body)
      else Post()
    }).filter(_.question_Id > 0)

    println("Saving selected posts ...")
    val pw: PrintWriter = new PrintWriter(new File(s"data/categories/$categoryInStudy.csv"))
    postsSelected.collect().foreach(post => pw.write(post.toString + "\n"))
    pw.close()

    sparkSession.close()
  }
}
