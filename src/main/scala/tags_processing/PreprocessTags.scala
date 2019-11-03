package tags_processing

import java.io.{File, PrintWriter}
import java.util.jar.JarFile

import org.apache.bcel.classfile.ClassParser
import org.apache.spark.sql.Encoder
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}
import org.jsoup.select.Elements
import spark_conf.Context

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

// Process the tags and return bigrams of tags sorted by their frequencies

object PreprocessTags extends Context {
  implicit def kryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] =
    org.apache.spark.sql.Encoders.kryo[A](ct)

  def indexClusters(tag: String, clusters: Array[Array[String]]): Int = {
    val tagSplitted: Array[String] = tag.split(",")
    for (i <- clusters.indices) {
      if (clusters(i).contains(tagSplitted(0)) || clusters(i).contains(tagSplitted(1)))
        return i
    }
    -1
  }

  def getSystemClasses(): Array[String] = {
    val jrePath: String = System.getProperty("java.home") + "/lib/rt.jar"

    val jarFile = new JarFile(jrePath)
    val entries = jarFile.entries.asScala

    val classesSystem: Array[String] = entries.filter(x => x.getName.endsWith(".class"))
      .map(entry => {
      val parser = new ClassParser(jrePath, entry.getName)
      val jc = parser.parse()
      var className = jc.getClassName.split("\\.").last

      if (className.contains("$")) {
        val classesSplitted = className.split("\\$")
        var index: Int = classesSplitted.length - 1

        for (i <- classesSplitted.indices) {
          if (classesSplitted(i)(0).isDigit) {
            index = i - 1
          }
        }
        className = classesSplitted(index)
      }
        className
    }).toArray

    classesSystem
  }

  def processLine(line: String, clazz: String): String = {
    var usageLine = ""

    val splittedLine: Array[String] = line.split(" ")

    if (line.contains("new")) {
      val index = splittedLine.indexWhere(chunk => chunk.contains("new"))
      val indexClass = splittedLine.indexWhere(chunk => chunk.contains(clazz))

      if (indexClass > 0 && splittedLine(indexClass -  1).contains("new")) {
        usageLine = splittedLine.slice(index + 1, splittedLine.length + 1).mkString(" ")
      } else {
        val index = splittedLine.indexWhere(chunk => chunk.contains(clazz + "."))

        if (index > -1)
          usageLine = splittedLine.slice(index, splittedLine.length + 1).mkString(" ")
      }
    } else {
      val index = splittedLine.indexWhere(chunk => chunk.contains(clazz + "."))

      if (index > -1)
        usageLine = splittedLine.slice(index, splittedLine.length + 1).mkString(" ")
    }
    usageLine
  }

  def lineUsage(classes: Array[String], body: String): Array[String] = {
    val html: Document = Jsoup.parse(body)
    val codes: Elements = html.select("code")

    val linesCodes = codes
      .toArray().flatMap(code => code.asInstanceOf[Element].text().split("\n"))

    var linesContainingClasses: Array[String] = Array()

    for (clazz <- classes) {
      for (line <- linesCodes) {
        if (line.contains(clazz) && !line.startsWith("import")) {
          linesContainingClasses :+= processLine(line.trim, clazz)
        }
      }
    }
    linesContainingClasses.filter(_.length > 1)
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
      .toDF()
    val dataNoNull = dataPosts.na.drop()

    val excludedListTags: Array[String] = Array("java", "android", "c#", "eclipse", "intellij-idea", "java-8")
    val thresholdsIterate: Array[Double] = Array(.6, .55, .5, .45, .4)

    val categoryInStudy: String = "json_libraries"
    val categoryInformation = usagesCategory.where(usagesCategory.col("Library") === categoryInStudy)

    val pairsCounter = new mutable.HashMap[String, (Int, Int, Array[Double])]
    for (threshold <- thresholdsIterate) {
      println(s"Threshold: $threshold")
      val selectedLibraries = categoryInformation.where(usagesCategory.col("Similarity") >= threshold)

      val joinedData = selectedLibraries
        .join(dataNoNull, "Answer_ID")
        .drop(dataNoNull.col("Answer_ID"))
        .drop(dataNoNull.col("Question_ID"))
        .drop(dataNoNull.col("Title"))
        .dropDuplicates()
        .filter(row => linesOfCode(row.getAs[String]("Body")))

      println("Processing the tags in the category ...")
      val tags = joinedData.select("Tags", "Similarity")

      println("Extracting bigrams from tags ...")
      val tagsBigrams = tags.flatMap(row => {
        val similarity: Double = row.getAs[Double]("Similarity")

        val tagsArray: Array[String] = row.getAs[String]("Tags")
          .replaceAll("<", "")
          .replaceAll(">", " ")
          .trim.split(" ")
          .filter(tag => !excludedListTags.contains(tag))

        val combinations: Array[Array[String]] = tagsArray.combinations(2).toArray[Array[String]]
        val combinationsGenerated: Array[(String, Double)] = combinations.map(arr => {
          val firstValue: String = arr(0)
          val secondValue: String = arr(1)

          val textOrder: String = if (firstValue > secondValue) secondValue + "," + firstValue
          else firstValue + "," + secondValue

          (textOrder, similarity)
        })
        combinationsGenerated
      })

      println("Counting bigrams ...")
      val counterBigrams = new mutable.HashMap[String, (Int, Array[Double])]()
      tagsBigrams.collect().foreach(bigram => {
        val key: String = bigram._1
        val value: Double = bigram._2

        if (counterBigrams.contains(key)) {
          val oldValues = counterBigrams(key)
          val newArray: Array[Double] = oldValues._2 :+ value
          counterBigrams(key) = (oldValues._1 + 1, newArray)
        } else {
          counterBigrams(key) = (1, Array(value))
        }
      })
      val descendentOrder: ListMap[String, (Int, Array[Double])] = ListMap(counterBigrams.toSeq.sortWith(_._2._1 > _._2._1): _*)

      // Storing the information
      descendentOrder.foreach(pair => {
        val name: String = pair._1
        val occurrences: (Int, Array[Double]) = pair._2

        if (pairsCounter.contains(name)) {
          val oldValues = pairsCounter(name)
          pairsCounter(name) = (oldValues._1 + 1,
            oldValues._2 + occurrences._1,
            oldValues._3.toSet.union(occurrences._2.toSet).toArray)
        } else {
          pairsCounter(name) = (1, occurrences._1, occurrences._2)
        }
      })
    }

    println("Calculating average for the similarities")
    val weightedPairs = pairsCounter.map(item => {
      (item._1, (item._2._1, item._2._2, item._2._3.sum / item._2._3.length))
    })

    println("Selecting tags ...")
    val pairsOrdered = ListMap(weightedPairs.toSeq.sortWith(_._2._2 > _._2._2): _*).filter(_._2._2 >= 5)

    println(pairsOrdered.size)

    val pw: PrintWriter = new PrintWriter(new File(s"data/results/tags_selection/$categoryInStudy.txt"))
    pairsOrdered.foreach(item => {
      val textToWrite: String = item._1 + "\n"
     pw.write(textToWrite)
    })
    pw.close()

    sparkSession.stop()
  }
}
