import java.io.{File, PrintWriter}

import org.apache.spark.sql.Encoder
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}
import org.jsoup.select.Elements
import spark_conf.Context
import word2vec.LibraryVector

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.io.Source

import scala.reflect.ClassTag

case class Library(name: String, metadata: String, var classes: Set[String] = Set())

case class Usage(groupID: String,
                 artifactID: String,
                 classes: String,
                 prevalence: Float,
                 var answerID: Int = 0,
                 var title: String = "",
                 var tags: String = "") {
  override def toString: String = s"$answerID,${title.replaceAll(",", "")},$groupID,$tags," +
    s"$artifactID,$classes,$prevalence"
}

object LibraryExtractorFilter extends Context {
  implicit def kryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] =
    org.apache.spark.sql.Encoders.kryo[A](ct)

  def cleanText(text: String): String = {
    val cleanedText = text.replaceAll("\n", " ")

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

    val filteredClasses: Array[String] = cleanEmpty
      .filter(word => !javaWords.contains(word))
      .filter(word => word(0).isUpper)

    filteredClasses.mkString(" ")
  }

  def getIndexLibrary(libraries: Array[Library], metadataInfo: String): Int = {
    for (i <- libraries.indices)
      if (libraries(i).metadata.equals(metadataInfo))
        return i
    -1
  }

  def getClassesReferences(body: String, libraries: Array[Library]): Option[Usage] = {
    val html: Document = Jsoup.parse(body)
    val codes: Elements = html.select("code")

    val processedCodes: Array[String] = codes
      .toArray()
      .map(code => cleanText(code.asInstanceOf[Element].text()))
      .mkString(" ")
      .split(" ")

    val inLibraries = new mutable.HashMap[String, Int]
    val inLibrariesClasses = new mutable.HashMap[String, String]

    for (code <- processedCodes) {
      for (library <- libraries) {
        if (library.classes.contains(code.trim)) {
          if (inLibraries.contains(library.metadata)) {
            inLibraries(library.metadata) += 1
            inLibrariesClasses(library.metadata) += "|" + code.trim
          } else {
            inLibraries(library.metadata) = 1
            inLibrariesClasses(library.metadata) = code.trim
          }
        }
      }
    }

    if (inLibraries.nonEmpty) {
      val orderedElements: ListMap[String, Int] = ListMap(inLibraries.toSeq.sortWith(_._2 > _._2): _*)

      val metadata: Array[String] = orderedElements.head._1.split(" ")
      val groupElement: String = metadata(0)
      val artifactElement: String = metadata(1)
      val prevalence: Float = orderedElements.head._2.toFloat / processedCodes.length.toFloat

      Some(Usage(groupElement, artifactElement, inLibrariesClasses(orderedElements.head._1), prevalence))
    } else None
  }

  def main(args: Array[String]): Unit = {
    println("Loading information about the libraries ...")
    val informationLibrary = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/libraries/json_libraries.csv")

    val librariesVersions = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/libraries/json_libraries_versions.csv")

    val categorySimilarities = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/libraries/categories_similarities/json_libraries_similarities")

    println("Loading all the StackOverflow posts ...")
    val dataPosts = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/posts/java_posts.csv")
      .toDF()
    val dataNoNull = dataPosts.na.drop()

    val librariesIds = librariesVersions.map(library => {
      val groupId: String = library.getAs[String]("GroupID")
      val artifactId: String = library.getAs[String]("ArtifactID")

      groupId + " " + artifactId
    })

    println("Getting all versions for the libraries in the category ...")
    val allLibrariesVersions = librariesVersions.map(library => {
      val groupId: String = library.getAs[String]("GroupID")
      val artifactId: String = library.getAs[String]("ArtifactID")
      val versions: Array[String] = library.getAs[String]("Versions").split("\\|")

      val combinations: List[String] = versions.map(version => {
        groupId + " "  + artifactId + " " + version
      }).toList
      combinations
    }).collect().toList.flatten

    println("Mapping names with metadata ...")
    val libraries: Array[Library] = librariesIds.collect().map(library => {
      val metadata: String = library.replace(" ", "/")
      val name: String = informationLibrary.where(informationLibrary
        .col("urls").endsWith(metadata))
        .first()
        .getAs[String]("libraries")
      Library(name, library)
    })

    println("Mapping classes with previous information ...")
    for (library <- allLibrariesVersions) {
      val metadataInformation: Array[String] = library.split(" ")
      val considerMetadata: String = metadataInformation(0) + " "  + metadataInformation(1)

      val libraryVector: LibraryVector = new LibraryVector(metadataInformation(0),
        metadataInformation(1), metadataInformation(2))
      val classes: Array[String] = libraryVector
        .getOnlyClasses()
        .split(" ")
        .filter(_.length >  1)

      val index: Int = getIndexLibrary(libraries, considerMetadata)
      libraries(index).classes = libraries(index).classes.union(classes.toSet)
    }

    println("Extracting information about the selected posts ...")
    val answersIds = categorySimilarities.select("Answer_ID")
    val dataToAnalyze = dataNoNull.select("Answer_ID", "Body", "Title", "Tags")

    val fullData = dataToAnalyze.join(answersIds, "Answer_ID").dropDuplicates()

    println("Start processing ...")
    val writeText = fullData.map(row => {
      val answerId = row.getAs[String]("Answer_ID")
      val title = row.getAs[String]("Title")
      val tags = row.getAs[String]("Tags")
      val body = row.getAs[String]("Body")

      val usageOption = getClassesReferences(body, libraries)
      if (usageOption.isEmpty)
        ""
      else {
        val usage = usageOption.get
        usage.answerID = answerId.toInt
        usage.title = title
        usage.tags = tags

        usage.toString + "\n"
      }
    }).filter(_.length > 1)

    println(s"Length textsToWrite: ${writeText.count()}")

    val pw = new PrintWriter(new File("data/libraries/libraries_similarities/json_identifiers.csv"))
    pw.write(writeText.collect().mkString(""))
    pw.close()

    sparkSession.stop()
  }
}
