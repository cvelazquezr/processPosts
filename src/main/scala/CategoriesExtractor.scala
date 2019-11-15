import java.io.{File, PrintWriter}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoder
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}
import org.jsoup.select.Elements
import spark_conf.Context
import word2vec.LibraryVector

import org.apache.spark.mllib.feature.Word2VecModel

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.io.Source
import scala.reflect.ClassTag

// Associate Maven categories with StackOverflow posts

case class Post(questionId: Int,
                codeIdentifiers: Array[String],
                var title: String = "",
                var answersIds: Array[String] = Array(),
                var closestCategory: String = "",
                var distance: Double = 0.0D) {

  def partialInfo: String = {
    s"${questionId.toString},$closestCategory,${distance.toString}"
  }

  def completeInfo: String = {
    s"${questionId.toString},${answersIds.mkString("|")},$title,${codeIdentifiers.mkString("|")},$closestCategory,${distance.toString}"
  }
}

case class Answer(title: String,
                  questionId: Int,
                  answerId: Int,
                  codeIdentifiers: String,
                  var closestCategory: String = "",
                  var distance: Double = 0.0) {
  override def toString: String = s"${questionId.toString},${answerId.toString},$title,${codeIdentifiers.split(" ").mkString("|")},$closestCategory,${distance.toString}"
}

case class LibraryData(name: String, groupId: String, artifactId: String, version: String)

object CategoriesExtractor extends Context {
  implicit def kryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] =
    org.apache.spark.sql.Encoders.kryo[A](ct)

  def cleanText(text: String): Array[String] = {
    val cleanedText = text.replaceAll("\n", " ")

    val cleanedWords: Array[String] = cleanedText.split(" ").map(word => word.map(chr => {
      if (chr.isLetter)
        chr
      else
        ' '
    }))

    val sourceJava: Source = Source.fromFile("data/resources/java_words.txt")
    val javaWords: Array[String] = sourceJava.getLines().toArray
    sourceJava.close()

    val cleanEmpty: Array[String] = cleanedWords.filter(_.trim.length > 2)
      .map(word => word.trim)
      .filter(word => !javaWords.contains(word))

    cleanEmpty
  }

  def getIdentifiers(body: String): Array[String] = {
    val html: Document = Jsoup.parse(body)
    val codes: Elements = html.select("pre")

    val output = codes
      .toArray()
      .map(code => cleanText(code.asInstanceOf[Element].text()).mkString(" "))
      .mkString(" ")
   output.split(" ").filter(_.length > 2).mkString(" ").split(" ")
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

  def getModules(librariesIds: Array[LibraryData], moduleType: String = ""): mutable.HashMap[String, String] = {
    val vectorCategories = new mutable.HashMap[String, String]()
    var identifiersPerLibrary: String = ""

    librariesIds.foreach(library => {
      val libraryVectorObject: LibraryVector = new LibraryVector(library.groupId, library.artifactId, library.version)

      val modules: String =
        if (moduleType.equals("classes"))
          libraryVectorObject.getOnlyClasses()
        else if (moduleType.equals("methods"))
          libraryVectorObject.getOnlyMethods()
        else
          libraryVectorObject.getClassesMethods()

      if (modules.length > 0) {
        val libraryVector: Array[String] = cleanText(modules)

        if (library.name.equals(identifiersPerLibrary))
          vectorCategories(library.name) += libraryVector.mkString(" ")
        else {
          identifiersPerLibrary = library.name
          vectorCategories(library.name) = libraryVector.mkString(" ")
        }
      }
    })
    vectorCategories
  }

  def main(args: Array[String]): Unit = {
    val sc = sparkSession.sparkContext

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

      LibraryData(categoryName, groupId, artifactId, version)
    })

//    println("Loading SCOR Model ...")
//    val model: word2vec.Word2VecLocal = new word2vec.Word2VecLocal()
//    model.load("data/scor_model/model_word2vec_v2.txt")

    println("Loading New Model ...")
    val model: Word2VecModel = Word2VecModel.load(sc,"data/model/modelLibraries")
    val vectors: Map[String, Array[Float]] = model.getVectors

    val modelLocal: word2vec.Word2VecLocal = new word2vec.Word2VecLocal()
    modelLocal.setVocab(vectors)

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

    println("ML approach to extract similarities in StackOverflow posts ...")
    val vectorsCategories = getModules(librariesIds)

    println("Extracting all identifiers from all answers ...")
    val identifiersPosts = dataNoNull.map(row => {
      val title: String = row.getAs[String]("Title")
      val questionID: Int = row.getAs[Int]("Question_ID")
      val answerID: Int = row.getAs[Int]("Answer_ID")
      val body: String = row.getAs[String]("Body")

      val identifiers: Array[String] = getIdentifiers(body)
      Answer(title.replaceAll(",", " "), questionID, answerID, identifiers.mkString(" "))
    })

    println("Calculating distances between the posts and the categories ...")

    val identifiers = identifiersPosts.map(answer => {
      if (answer.codeIdentifiers.length > 0) {
        val distances: Array[Double] = vectorsCategories.map(pair => {
          modelLocal.n_similarity(answer.codeIdentifiers.split(" "), pair._2.split(" "))
        }).toArray

        val maxDistance: Double = distances.max
        val keys: Array[String] = vectorsCategories.keys.toArray
        val indexMax: Int = distances.indexOf(maxDistance)

        if (indexMax >= 0) {
          val closestCategory: String = keys(indexMax)

          answer.closestCategory = closestCategory
          answer.distance = maxDistance
          answer.toString + "\n"
        } else ""
      } else ""
    }).filter(_.length > 0)

    println("Classical approach to find similarities ...")

    val groupedPosts = dataNoNull.groupBy("Question_ID")
      .agg(collect_list("Body") as "Bodies")

    println("Extracting all identifiers from all the groupedPosts ...")
    val identifiersPostsClassical = groupedPosts.map(row => {
      val questionID: Int = row.getAs[Int]("Question_ID")
      val bodies: Array[String] = row.getAs[mutable.WrappedArray[String]]("Bodies").toArray

      val identifiers: Array[String] = bodies.map(body => getIdentifiers(body)).filter(_.length > 2).map(_.mkString(" "))

      Post(questionID, identifiers)
    })

    println("Making vectors of the libraries ...")
    val vectorsCategoriesClasses = getModules(librariesIds, "classes")
    val vectorsClasses: Map[String, String] = vectorsCategoriesClasses.toMap

    val vectorsCategoriesMethods = getModules(librariesIds, "methods")
    val vectorsMethods: Map[String, String] = vectorsCategoriesMethods.toMap

    // An specific category is evaluated in all classes in the answers of a post
    def similarityClasses(identifiersPost: Array[String], identifiersCategory: Array[String]): Double = {
      val counterAnswers: Array[String] = identifiersPost.filter(answer => {
        val intersect = answer.split(" ").toSet.intersect(identifiersCategory.toSet)
        val cleanedIntersect = intersect.filter(word => word(0).isUpper)

        cleanedIntersect.nonEmpty
      })
      counterAnswers.length.toFloat / identifiersPost.length.toFloat
    }

    // An selected category is evaluated in the answers of a post
    def similarityMethods(identifiersPost: Array[String], identifiersCategory: Array[String]): Double = {
      val counterAnswers: Array[Float] = identifiersPost.map(answer => {
        val lowerWords: Array[String] = answer.split(" ").filter(word => word(0).isLower)
        val intersect = lowerWords.toSet.intersect(identifiersCategory.toSet)

        intersect.size.toFloat / lowerWords.length.toFloat
      })
      counterAnswers.sum / identifiersPost.length.toFloat
    }

    val identifiersClassical = identifiersPostsClassical.map(post => {
      if (post.codeIdentifiers.length > 0) {
        // First filter for classes to the category candidates
        val distancesClasses: Array[Double] = vectorsClasses.map(pair => {
          similarityClasses(post.codeIdentifiers, pair._2.split(" "))
        }).toArray

        val maxDistanceClasses: Double = distancesClasses.max
        val keys: Array[String] = vectorsClasses.keys.toArray

        if (maxDistanceClasses >= 0.5) {
          val candidateCategories = for (
            i <- distancesClasses.indices
            if distancesClasses(i) == maxDistanceClasses
          ) yield keys(i)

          // Second filter for the methods
          val distancesMethods: IndexedSeq[Double] = for (candidateCategory <- candidateCategories)
            yield similarityMethods(post.codeIdentifiers, vectorsMethods(candidateCategory).split(" "))

          val maxDistanceMethods: Double = distancesMethods.max
          val indexMaxMethods: Int = distancesMethods.indexOf(maxDistanceMethods)

          if (maxDistanceMethods > 0.5) {
            val closestCategory: String = candidateCategories(indexMaxMethods)

            println(post.questionId)
            println(closestCategory)

            post.closestCategory = closestCategory
            post.distance = maxDistanceMethods
            post.partialInfo + "\n"
          } else ""
        } else ""
      } else ""
    }).filter(_.length > 0)

    println("Saving data ...")
    val pw = new PrintWriter(new File("data/categories/categories_similarities_ml_new_model.txt"))
    identifiers.collect().foreach(line => pw.write(line))
    pw.close()

    sparkSession.stop()
  }
}
