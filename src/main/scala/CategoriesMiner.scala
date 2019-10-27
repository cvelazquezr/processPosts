import java.io.{File, PrintWriter}

import org.apache.spark.sql.Encoder
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}
import org.jsoup.select.Elements
import spark_conf.Context
import word2vec.LibraryVector

import scala.util.matching.Regex

import scala.io.Source
import scala.reflect.ClassTag


// Extract all the answers that are related with an specific category

object CategoriesMiner extends Context {

  implicit def kryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] =
    org.apache.spark.sql.Encoders.kryo[A](ct)

  def cleanText(body: String): Array[String] = {
    val html: Document = Jsoup.parse(body)
    val codes: Elements = html.select("code")
    val codesStr: Array[String] = codes.toArray().map(_.asInstanceOf[Element].text())

    val processedCodes: Array[String] = codesStr
      .filter(code => {
        code.split("\n").length > 2
      })

    processedCodes.foreach(code => {
      val cleanedText = code.replaceAll("\n", " ")

      val javaBuffered = Source.fromFile("data/java_words.txt")
      val javaWords: Seq[String] = javaBuffered.getLines().toSeq

      val cleanedWords: Array[String] = cleanedText.split(" ").map(word => word.map(chr => {
        if (chr.isLetter)
          chr
        else
          ' '
      })).mkString(" ").split(" ").filter(_.trim.length > 2)

      val classesNames: Array[String] = cleanedWords
        .map(_.trim)
        .filter(word => !javaWords.contains(word.toLowerCase))
        .filter(_(0).isUpper)

      return classesNames
    })
    Array()
  }

  def cleanToMethods(body: String): Array[String] = {
    val html: Document = Jsoup.parse(body)
    val codes: Elements = html.select("code")
    val codesStr: Array[String] = codes.toArray().map(_.asInstanceOf[Element].text())

    val processedCodes: Array[String] = codesStr
      .filter(code => {
        code.split("\n").length > 2
      })

    processedCodes.foreach(code => {
      val cleanedText: String = code.split("\n").filter(_.contains(".")).mkString(" ")

      val cleanedWords: Array[String] = cleanedText.split(" ").map(word => word.map(chr => {
        if (chr.isLetter || chr.equals('.'))
          chr
        else
          ' '
      })).mkString(" ")
        .split(" ")
        .filter(_.trim.length > 2)
        .filter(_.contains("."))

      val calls: Array[String] = cleanedWords.map(call => {
        val splittedCall: Array[String] = call.split("\\.")
        if (splittedCall.length > 0) {
          val rightMostElement = splittedCall.takeRight(1)(0)
          if (rightMostElement.length > 0 && rightMostElement(0).isLower)
            rightMostElement
          else
            ""
        } else ""
      })

      return calls.filter(_.length > 0)
    })
    Array()
  }

  def linkReferenceLibrary(body: String, pattern: String): Boolean = {
    val html: Document = Jsoup.parse(body)
    val codes: Elements = html.select("a")

    var references: Int = 0
    codes.forEach(code => {
      val linkSplitted = code.attr("href").split("/")
      if (linkSplitted.contains(pattern))
        references += 1
    })

    if (references > 0)
      return true
    false
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

  def linesOfCode(body: String): Boolean = {
    val html: Document = Jsoup.parse(body)
    val codes: Elements = html.select("code")

    val processedCodes: Array[String] = codes
      .toArray()
      .map(_.asInstanceOf[Element].text())
      .filter(code => {
        val splittedCode: Array[String] = code.split("\n");
        splittedCode.length > 2
      })
    processedCodes.length > 0
  }

  def containsLinks(body: String): Boolean = {
    val html: Document = Jsoup.parse(body)
    val codes: Elements = html.select("a")

    codes.toArray().nonEmpty
  }

  def main(args: Array[String]): Unit = {
    println("Loading all similarities with categories ...")

    val categoriesSimilarities = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/libraries/categories_similarities.csv")

    println("Loading the libraries for the category in study ...")
    val librariesVersions = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/libraries/i_o_utilities_versions.csv")

    println("Storing all libraries metadata in memory ...")
    val allLibrariesVersions = librariesVersions.map(library => {
      val groupId: String = library.getAs[String]("GroupID")
      val artifactId: String = library.getAs[String]("ArtifactID")
      val versions: Array[String] = library.getAs[String]("Versions").split("\\|")

      val combinations: Array[String] = versions.flatMap(version => {
        val libraryVector: LibraryVector = new LibraryVector(groupId, artifactId, version)
        val modulesLibrary: String = libraryVector.getClassesMethods()
        modulesLibrary.split(" ")
      }).distinct.filter(!_.endsWith("init>")).filter(!_.contains("$")).filter(_.length > 2)

      val mainMetadata: String = groupId + " " + artifactId

      (mainMetadata, combinations)
    })

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

    println("Filtering out answers ...")
    val selectedLibraries = categoriesSimilarities.where(categoriesSimilarities.col("Category") === "i_o_utilities")
    val joinedData = selectedLibraries
      .join(dataNoNull, "Answer_ID")
      .drop(dataNoNull.col("Answer_ID"))
      .drop(dataNoNull.col("Question_ID"))
      .drop(dataNoNull.col("Title"))
      .dropDuplicates()
      .filter(row => linesOfCode(row.getAs[String]("Body")))

    println("Storing the methods in memory  ...")
    val mappingMethods = librariesVersions.map(library => {
      val groupId: String = library.getAs[String]("GroupID")
      val artifactId: String = library.getAs[String]("ArtifactID")
      val versions: Array[String] = library.getAs[String]("Versions").split("\\|")

      (groupId + " " + artifactId, versions)
    }).collect().toMap

    println("Processing the posts ...")
    val allLibrariesMap: Map[String, Array[String]] = allLibrariesVersions.collect().toMap
    val keysLibraries: Array[String] = allLibrariesMap.keys.toArray
    val valuesLibraries: Array[Array[String]] = allLibrariesMap.values.toArray

    joinedData.foreach(row => {
      val answerID: Int = row.getAs[Int]("Answer_ID")

      val body: String = row.getAs[String]("Body")
      val regex: Regex = ".*(^|[a-z]+ |[.!?] |[(<])IOUtils([>).,!?$]| [a-z]+).*".r
      val matches: Array[String] = regex.findAllIn(body).toArray

      if (matches.length > 0)
        println(answerID)
    })

    val usagesLibraries: Array[String] = joinedData.map(row => {
      val answerID: Int = row.getAs[Int]("Answer_ID")
      val questionID: Int = row.getAs[Int]("Question_ID")
      val body: String = row.getAs[String]("Body")

      val classesBody: Array[String] = cleanText(body)
      val methodsBody: Array[String] = cleanToMethods(body)

      // First filter for the classes
      var candidateLibraries: Array[(String, Array[String])] = Array()
      var k = 0
      for (arr <- valuesLibraries) {
        if (arr.count(token => classesBody.contains(token)) > 0) {
          val keyHashMap: String = keysLibraries(k)
          val valuesHashMap: Array[String] = arr.toSet.intersect(classesBody.toSet).toArray
          candidateLibraries :+= (keyHashMap, valuesHashMap)
        }
        k += 1
      }

      if (candidateLibraries.length > 0) {
        // Second filter for the methods
        val finalCandidates: Array[Int] = candidateLibraries.map(library => {
          val versionsLibrary: Array[String] = mappingMethods(library._1)

          val methodsLibrary = versionsLibrary.flatMap(version => {
            val splittedMetadata: Array[String] = library._1.split(" ")
            val libraryVector: LibraryVector = new LibraryVector(splittedMetadata(0), splittedMetadata(1), version)
            val methods = library._2.map(clazz => libraryVector.getMethods(clazz))
            methods
          }).filter(!_.endsWith("init>")).distinct

          methodsLibrary.toSet.intersect(methodsBody.toSet).size
        })

        val remainingCandidates = for {
          i <- finalCandidates.indices
          if finalCandidates(i) > 0
        } yield {
          keysLibraries(i).toString
        }

        s"$questionID,$answerID,${remainingCandidates.mkString("|")}\n"

      } else ""
    }).filter(_.split(",").length > 2).collect()

    val pw = new PrintWriter(new File("data/usages/i_o_utilities.txt"))
    usagesLibraries.foreach(line => pw.write(line))
    pw.close()
    sparkSession.stop()

//    println("Checking for the answers with imports ...")
//    val withImports = joinedData.filter(row => containsImport(row.getAs[String]("Body")))
//    val noImports = joinedData.filter(row => !containsImport(row.getAs[String]("Body")))
//
//    println(s"Total of answers in category: ${joinedData.count()}\nWith import: ${withImports.count()}")
//
//    println("Analyzing posts with imports ...")
//
//    withImports.foreach(row => {
//      val answerID: Int = row.getAs[Int]("Answer_ID")
//      val body: String = row.getAs[String]("Body")
//      val modulesPost: Array[String] = getLibrariesImport(body).split(" ")
//
//      println(modulesPost.mkString(" "))
//      for (modulePost <- modulesPost)  {
//        for (modulesLibraries <- valuesJoined) {
//          if (modulesLibraries.contains(modulePost)) {
//            println(modulePost, modulesLibraries)
//            println(s"$answerID -> $modulePost")
//          }
//        }
//      }
////      val modulesFiltered: Array[String]  = modulesPost.filter(module => {
////        val inValues: Array[String] = valuesJoined.filter(_.contains(module))
////        inValues.length > 0
////      })
////
////      if (modulesFiltered.length > 0)
////        println(s"$answerID -> ${modulesFiltered.mkString(" ")}")
//    })
//
//
//    println("Checking for the answers without imports and with links ...")
//    val withLinks = noImports.filter(row => containsLinks(row.getAs[String]("Body")))
//    val noLinks = noImports.filter(row => !containsLinks(row.getAs[String]("Body")))
//
//    println(s"Total answers without import: ${noImports.count()}\nWith links: ${withLinks.count()}")
//    println(s"Answers without import or links: ${noLinks.count()}")
//
//    println("Writing to a file ...")
//    selectedLibraries
//      .write.mode(SaveMode.Overwrite)
//      .option("header", "true")
//      .csv("data/libraries/categories_similarities/json_libraries_similarities")
  }
}
