import java.io.{File, PrintWriter}

import org.apache.spark.sql.Encoder
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}
import org.jsoup.select.Elements
import spark_conf.Context
import word2vec.LibraryVector
import edu.stanford.nlp.tagger.maxent.MaxentTagger
import tags_processing.PreprocessTags.linesOfCode

import scala.io.Source
import scala.reflect.ClassTag

case class Library(name: String,
                   metadata: String,
                   var classesMethods: Set[String] = Set())

case class Usage(metadata: String, matches: Int) {
  override def toString: String = s"$metadata->$matches"
}

case class PostInformation(answer_Id: Int, title: String) {
  override def toString: String = s"$answer_Id,$title"
}

object LibraryExtractorFilter extends Context {
  implicit def kryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] =
    org.apache.spark.sql.Encoders.kryo[A](ct)

  def cleanText(text: String): String = {
    val cleanedText = text.replaceAll("\n", " ")

    val javaBuffered = Source.fromFile("data/resources/java_words.txt")
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

    filteredClasses.mkString(" ")
  }

  def getIdentifiers(body: String): Array[String] = {
    val html: Document = Jsoup.parse(body)
    val codes: Elements = html.select("code")

    codes.toArray().map(code => cleanText(code.asInstanceOf[Element].text())).mkString(" ").split(" ")
  }

  def getIndexLibrary(libraries: Array[Library], metadataInfo: String): Int = {
    for (i <- libraries.indices)
      if (libraries(i).metadata.equals(metadataInfo))
        return i
    -1
  }

  def containsImport(body: String): Boolean = {
    val html: Document = Jsoup.parse(body)
    val codes: Elements = html.select("code")

    val processedCodes: Array[String] = codes
      .toArray()
      .map(_.asInstanceOf[Element].text())
      .mkString(" ")
      .split(" ")

    val importLines = processedCodes.filter(_.contains("import"))

    if (importLines.length > 0)
      return true
    false
  }

  def containsLinks(body: String): Boolean = {
    val html: Document = Jsoup.parse(body)
    val codes: Elements = html.select("a")

    codes.toArray().nonEmpty
  }

  def containsReferenceInText(title: String, body: String, libraries: Array[String]): Boolean = {
    // Check for the title
    val inTitle: Array[String] = libraries.filter(name => title.toLowerCase().contains(name.toLowerCase()))
    if (inTitle.length > 0)
      return true

    // Check for the body
    val html: Document = Jsoup.parse(body)
    html.select("pre").remove()
    html.select("code").remove()

    val htmlString: String = html.toString

    val inBody: Array[String] = libraries.filter(name => htmlString.toLowerCase().contains(name.toLowerCase()))
    if (inBody.length > 0)
      return true
    false
  }


  def processTitle(title: String): String = {
    val cleanedTitle: String = title.replaceAll(",", "")
    val maxentTagger: MaxentTagger = new MaxentTagger("data/resources/english-left3words-distsim.tagger")

    val tags: String = maxentTagger.tagString(cleanedTitle)
    val eachTag: Array[String] = tags.split("\\s+")

    val listTags: Array[(String, String)] = for (tag <- eachTag if tag.split("_")(1).contains("NN") ||
      tag.split("_")(1).contains("VB") || tag.split("_")(1).contains("JJ"))
      yield (tag.split("_")(0), tag.split("_")(1))

    val tokens: Array[String] = for (tag <- listTags) yield { tag._1.toLowerCase }
    tokens.mkString(" ")
  }

  // Regular expressions of the state-of-the-art
  def fullyQualifiedRegex(body: String, typeQualified: String): Int = {
    val expressionRegex = "(?i). ∗\\b"  + typeQualified + "\\b.∗"
    val regex = expressionRegex.r
    regex.findAllIn(body).toArray.length
  }

  def nonQualifiedRegex(body: String, typeName: String): Int = {
    val expressionRegex = ".*(^|[a-z]+ |[\\.!?] |[\\(<])" + typeName + "([>\\)\\.,!?$]| [a-z]+).*"
    val regex = expressionRegex.r
    regex.findAllIn(body).toArray.length
  }

  def linksRegex(body: String, packageName: String, typeName: String): Unit = {
    val expressionRegex = ".∗ <a.∗href.∗" + packageName + "/" + typeName + "\\.html.∗ >.∗ </a>.∗"
    val regex = expressionRegex.r
    regex.findAllIn(body)
  }

  def classMethodsRegex(body: String, className: String, methodName: String): Int = {
    val expressionRegex = ". ∗ "+ className + "\\." + methodName + "[\\(| ]"
    val regex = expressionRegex.r
    regex.findAllIn(body).toArray.length
  }

  def main(args: Array[String]): Unit = {
    println("Loading data ...")

    val categoryInStudy: String = "json_libraries"
    val categorySimilarities = sparkSession
      .read
      .option("quote", "\"")
      .option("escape", "\"")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"data/categories/$categoryInStudy.csv")

    val usagesCategory = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/categories/categories_similarities.csv")

    val dataPosts = sparkSession
      .read
      .option("quote", "\"")
      .option("escape", "\"")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/posts/java_posts.csv")
      .toDF()
    val dataNoNull = dataPosts.na.drop()

    val informationLibrary = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"data/libraries/$categoryInStudy.csv")

    val librariesVersions = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"data/libraries/${categoryInStudy}_versions.csv")

    val categoryInformation = usagesCategory.where(usagesCategory.col("Library") === categoryInStudy)
    val joinedData = categoryInformation
      .join(dataNoNull, "Answer_ID")
      .drop(dataNoNull.col("Answer_ID"))
      .drop(dataNoNull.col("Question_ID"))
      .drop(dataNoNull.col("Title"))
      .dropDuplicates()
      .filter(row => linesOfCode(row.getAs[String]("Body")))

    val postsWithImports = joinedData.filter(row => {
      val body: String = row.getAs[String]("Body")
      containsImport(body)
    })

    val postsWithoutImports = joinedData.filter(row => {
      val body: String = row.getAs[String]("Body")
      !containsImport(body)
    })

    val postsWithoutImportsWithLinks = postsWithoutImports.filter(row => {
      val body: String = row.getAs[String]("Body")
      containsLinks(body)
    })

    val postsWithoutImportsWithoutLinks = postsWithoutImports.filter(row => {
      val body: String = row.getAs[String]("Body")
      !containsLinks(body)
    })

    val librariesNames: Array[String] = Array("Jackson", "Gson", "JSON in Java", "Fastjson", "JSON Simple", "JSON Path")
    val postsTextReference = postsWithoutImportsWithoutLinks.filter(row => {
      val title: String = row.getAs[String]("Title")
      val body: String = row.getAs[String]("Body")
      containsReferenceInText(title, body, librariesNames)
    })

    println(s"With Imports: ${postsWithImports.count()}")
    println(s"Without Imports and with Links: ${postsWithoutImportsWithLinks.count()}")
    println(s"With References in the text: ${postsTextReference.count()}")
    println(s"Total amount of posts: ${joinedData.count()}")

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

      val classesMethods: Array[String] = libraryVector
        .getClassesMethods()
        .split(" ")
        .filter(_.length >  1)

      val index: Int = getIndexLibrary(libraries, considerMetadata)
      libraries(index).classesMethods = libraries(index).classesMethods.union(classesMethods.toSet)
    }

    val total = joinedData.count()
    var k = 0

    println("Selecting most likely library for the answer (Code based): ")
    val usages = joinedData.map(post => {
      k += 1
      println(s"Processing $k/$total")

      val answer_Id: Int = post.getAs[Int]("Answer_ID")
      val title: String = post.getAs[String]("Title")
      val body: String = post.getAs[String]("Body")
      val identifiers: Array[String] = getIdentifiers(body)

      var usagesPost: Array[Usage] = Array()

      libraries.foreach(library => {
        val matchesLibrary: Int = library.classesMethods.intersect(identifiers.toSet).size
        if (matchesLibrary > 0) {
          usagesPost :+= Usage(library.metadata, matchesLibrary)
        }
      })
      val processedTitle: String =  processTitle(title)
      val postInformation: PostInformation = PostInformation(answer_Id, processedTitle)
      (postInformation, usagesPost)
    }).collect().toMap

    val pw: PrintWriter = new PrintWriter(new File(s"data/usages/$categoryInStudy.txt"))
    usages.foreach(usage => {
      val usageArr: Array[Usage] = usage._2
      val usagesStr: String = usageArr.mkString("|")
      pw.write(s"${usage._1},$usagesStr\n")
    })

    pw.close()
    sparkSession.stop()
  }
}
