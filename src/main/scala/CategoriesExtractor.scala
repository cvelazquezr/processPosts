import java.io.{File, PrintWriter}

import opennlp.tools.stemmer.PorterStemmer
import org.apache.spark.sql.Encoder
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}
import org.jsoup.select.Elements
import spark_conf.Context
import utils.Utils.splitCamelCase
import word2vec.LibraryVector

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.io.Source
import scala.reflect.ClassTag

// Associate Maven categories with StackOverflow posts

case class Post(title: String, questionId: Int, answerId: Int, codeIdentifiers: String, var closestLibrary: String = "", var distance: Double = 0.0D) {
  override def toString: String = s"${questionId.toString},${answerId.toString},$title,$codeIdentifiers,$closestLibrary,${distance.toString}"
}

object CategoriesExtractor extends Context {
  implicit def kryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] =
    org.apache.spark.sql.Encoders.kryo[A](ct)

  def cleanText(text: String): String = {
    val cleanedText = text.replaceAll("\n", " ")
    val porterStemmer: PorterStemmer = new PorterStemmer

    val javaBuffered = Source.fromFile("data/java_words.txt")
    val javaWords: Seq[String] = javaBuffered.getLines().toSeq

    val cleanedWords: Array[String] = cleanedText.split(" ").map(word => word.map(chr => {
      if (chr.isLetter)
        chr
      else
        ' '
    }))

    val cleanEmpty: Array[String] = cleanedWords.mkString(" ").split(" ")
      .filter(word => word.trim.length > 2)
      .map(word => word.trim)

    val stemmedWords: Array[Array[String]] = cleanEmpty
      .filter(word => !javaWords.contains(word))
      .map(word => splitCamelCase(word).split(" ")
        .map(word2 => porterStemmer.stem(word2)))

    stemmedWords.map(arr => arr.mkString(" ")).mkString(" ")
  }

  def getIdentifiers(body: String): Array[String] = {
    val html: Document = Jsoup.parse(body)
    val codes: Elements = html.select("code")

    codes.toArray().map(code => cleanText(code.asInstanceOf[Element].text())).mkString(" ").split(" ")
  }

  def getMostFrequent(number: Int, sentence: String): Array[String] = {
    val splitted: Array[String] = sentence.split(" ")
    val counter = new mutable.HashMap[String, Int]()

    for (word <- splitted) {
      if (counter.contains(word)) {
        counter(word) += 1
      } else {
        counter(word) = 1
      }
    }

    val listCounter: ListMap[String, Int] = ListMap(counter.toSeq.sortWith(_._2 > _._2):_*)
    listCounter.take(number).keys.toArray
  }


  def main(args: Array[String]): Unit = {
    println("Loading all libraries ...")
    val librariesDetails = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/libraries/libraries_details.csv")

    val librariesSelected = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/libraries/libraries_selected.csv")

    val librariesIds = librariesSelected.collect().map(row => {
      val categoryName: String = row.getAs[String]("categories")
        .replaceAll(" ", "_")
        .replaceAll("/", "_")
        .toLowerCase
      val url: String = row.getAs[String]("urls")

      val metadata: Array[String] = url.split("/").takeRight(2)
      val groupId: String = metadata(0)
      val artifactId: String = metadata(1)

      val version: String = librariesDetails.where((librariesDetails.col("groupId") === groupId) &&
        (librariesDetails.col("artifactId") === artifactId)).first().getAs[String]("version")

      (categoryName, (groupId, artifactId, version))
    })

    println("Making vectors of the libraries ...")
    val vectorsLibraries = new mutable.HashMap[String, String]()
    var identifiersPerLibrary: String = ""

    librariesIds.foreach(library => {
      val libraryInfo: (String, String, String) = library._2
      val libraryVectorObject: LibraryVector = new LibraryVector(libraryInfo._1, libraryInfo._2, libraryInfo._3)

      val modules: String = libraryVectorObject.getModules()
      if (modules.length > 0) {
        val libraryVector: Array[String] = getMostFrequent(20, cleanText(modules))

        if (library._1.equals(identifiersPerLibrary))
          vectorsLibraries(library._1) += libraryVector.mkString(" ")
        else {
          identifiersPerLibrary = library._1
          vectorsLibraries(library._1) = libraryVector.mkString(" ")
        }
      }
    })

    val vectorsProcessed: Map[String, String] = vectorsLibraries.map(library => {
      (library._1, getMostFrequent(20, library._2).mkString(" "))
    }).toMap

    println("Loading SCOR Model ...")
    val model: word2vec.Word2VecLocal = new word2vec.Word2VecLocal()
    model.load("data/scor_model/model_word2vec.txt")

    println("Loading all the StackOverflow posts ...")
    val dataPosts = sparkSession
      .read
      .option("quote", "\"")
      .option("escape", "\"")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/posts/java_posts.csv")
      .toDF()
    val dataNoNull = dataPosts.na.drop()

    println("Extracting all identifiers from all answers ...")
    val identifiersPosts = dataNoNull.map(row => {
      val title: String = row.getAs[String]("Title")
      val questionID: Int = row.getAs[Int]("Question_ID")
      val answerID: Int = row.getAs[Int]("Answer_ID")
      val body: String = row.getAs[String]("Body")

      val identifiers: Array[String] = getIdentifiers(body)
      Post(title.replaceAll(",", " "), questionID, answerID, identifiers.mkString(" "))
    })

    println("Calculating distances between the posts and the categories ...")
    val pw = new PrintWriter(new File("data/libraries/categories_similarities.txt"))

    identifiersPosts.foreach(post => {
      if (post.codeIdentifiers.length > 0) {
        val distances: Array[Double] = vectorsProcessed.map(pair => {
          model.n_similarity(post.codeIdentifiers.split(" "), pair._2.split(" "))
        }).toArray

        val maxDistance: Double = distances.max
        val keys: Array[String] = vectorsProcessed.keys.toArray
        val indexMax: Int = distances.indexOf(maxDistance)

        if (indexMax >= 0) {
          val closestCategory: String = keys(indexMax)

          post.closestLibrary = closestCategory
          post.distance = maxDistance
          pw.write(post.toString + "\n")
        }
      }
    })
    pw.close()

    sparkSession.stop()
  }
}
