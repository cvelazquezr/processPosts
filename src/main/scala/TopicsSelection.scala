import java.io.{File, PrintWriter}

import edu.stanford.nlp.tagger.maxent.MaxentTagger
import org.apache.spark.sql.Encoder
import spark_conf.Context

// Posts selection by the similarities in their answers towards the same category

import scala.reflect.ClassTag

object TopicsSelection extends Context {
  implicit def kryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] =
    org.apache.spark.sql.Encoders.kryo[A](ct)

  def main(args: Array[String]): Unit = {
    val categoryInStudy: String = "json_libraries"

    println("Loading selected posts ...")
    val postsSelected = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"data/categories/$categoryInStudy.csv")

    val titles = postsSelected.select("Title")
    val vocabulary = titles.map(row => {
      val title: String = row.getAs[String]("Title")
      title
    })

    val tokenized = vocabulary
      .map(_.toLowerCase.split(" "))
      .map(_.map(word => word.replaceAll("[^A-Za-z]", " "))
      .filter(_.trim.length > 2))

    val processed = tokenized.map(arr => {
      val rawArr = arr.mkString(" ")
      val processedArr = rawArr.split(" ").filter(_.trim.length > 2)

      processedArr.mkString(" ")
    })

    val maxentTagger: MaxentTagger = new MaxentTagger("data/resources/english-left3words-distsim.tagger")
    processed.collect().foreach(line => {
      val tags: String = maxentTagger.tagString(line)

      val eachTag: Array[String] = tags.split("\\s+")

      val listTags: Array[(String, String)] = for (tag <- eachTag if tag.split("_")(1).contains("VB"))
        yield (tag.split("_")(0), tag.split("_")(1))

      val tokens: Array[String] = for (tag <- listTags) yield { tag._1.toLowerCase }
      println(line, "-->",eachTag.mkString(" "))
    })

    // TODO: Get the verbs for each title
    // TODO: Group the titles with common verbs and quantify
    // TODO: For each group make an internal cluster for similar titles by nouns as a second filter
    // TODO: This is basically Topic Modeling, but with weighted terms, check the bibliography

//    println("Saving selected posts ...")
//    val pw: PrintWriter = new PrintWriter(new File(s"data/categories/selectedPosts.csv"))
//    selectedPosts.collect().foreach(line => pw.write(line))
//    pw.close()

    sparkSession.close()
  }
}
