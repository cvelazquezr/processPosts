package tags_processing

import java.io.{File, PrintWriter}

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.functions.col
import spark_conf.Context

import scala.io.Source
import scala.reflect.ClassTag

// Selection of the posts that contain any tag selected

case class PostBasic(questionId: Int, title: String, tags: String) {
  override def toString: String = s"$questionId,${title.replaceAll(",", "")},$tags\n"
}

object SelectionPostsTags extends Context {
  implicit def kryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] =
    org.apache.spark.sql.Encoders.kryo[A](ct)

  def main(args: Array[String]): Unit = {

    val categoryInStudy: String = "mocking"
    val pathTags: String = s"data/results/tags_selection/${categoryInStudy}_bigrams.txt"

    val source: Source = Source.fromFile(pathTags)
    val lines: Array[String] = source.getLines().toArray

    var selectedTags: Set[String] = Set()
    lines.foreach(line => {
      val lineSplitted: Array[String] = line.split(",")

      if (lineSplitted(2).toInt  == 2)
        selectedTags += lineSplitted(0)
      if (lineSplitted(3).toInt == 2)
        selectedTags += lineSplitted(1)
    })

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
    val categoryInformation = selectedPosts.where(col("Category") === categoryInStudy)

    println("Joining data ...")
    val joinedData = categoryInformation
      .join(dataNoNull, "Question_ID")
      .drop(dataNoNull.col("Question_ID"))
      .dropDuplicates()

    val commonInformation = joinedData.select("Question_ID","Title", "Tags").dropDuplicates()

    println("Extracting posts from tags ...")
    val filteredPosts = commonInformation.filter(row => {
      val tagsArray: Array[String] = row.getAs[String]("Tags")
        .replaceAll("<", "")
        .replaceAll(">", " ")
        .trim.split(" ")

      tagsArray.toSet.intersect(selectedTags).nonEmpty
    })

    println(s"Number of posts with selected tags: ${filteredPosts.count()}")

    val postsObjects = filteredPosts.map(row => {
      val questionId: Int = row.getAs[Int]("Question_ID")
      val title: String = row.getAs[String]("Title")
      val tags: String = row.getAs[String]("Tags")

      PostBasic(questionId, title, tags)
    })

    val pw: PrintWriter = new PrintWriter(new File(s"data/categories/$categoryInStudy.csv"))
    postsObjects.collect().foreach(post => pw.write(post.toString))
    pw.close()

    source.close()
  }
}
