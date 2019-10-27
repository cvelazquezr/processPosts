import java.io.{File, PrintWriter}

import org.apache.spark.sql.Encoder
import spark_conf.Context

import scala.reflect.ClassTag
import scala.collection.JavaConverters._
import java.util.jar.JarFile

import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}
import org.jsoup.select.Elements

import scala.collection.immutable.ListMap
import scala.collection.mutable

object AllDone extends Exception

import org.apache.bcel.classfile.ClassParser

object AnalyseUsages extends Context {
  implicit def kryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] =
    org.apache.spark.sql.Encoders.kryo[A](ct)

  def indexClusters(tag: String, clusters: Array[Array[String]]): Int = {
    for (i <- clusters.indices) {
      if (clusters(i).contains(tag))
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

  def main(args: Array[String]): Unit = {
    println("Loading usages detected for libraries ...")
    val usagesCategory = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/libraries/libraries_similarities/mocking_identifiers.csv")

    println("Loading all the StackOverFlow posts ...")
    val dataPosts = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/posts/java_posts.csv")
      .toDF()
    val dataNoNull = dataPosts.na.drop

    val excludedListTags: Array[String] = Array("java", "android", "c#", "eclipse")
    val excludedListClasses: Array[String] = Array("Test")

    println("Processing the tags in the category ...")
    val tags = usagesCategory.select("Tags").collect().map(row => {
      val tagsProcessed = row.getAs[String]("Tags")
        .replaceAll("><", " ")
        .substring(1)
        .dropRight(1)
        .split(" ")
      tagsProcessed.filter(tag => !excludedListTags.contains(tag))
    }).filter(_.length > 0)

    val counter = new mutable.HashMap[String, Int]
    tags.flatten.foreach(tag => {
      if (counter.contains(tag))
        counter(tag) += 1
      else
        counter(tag) = 1
    })

    println("Checking for the neighbors of the tags ...")
    val descendentOrder: ListMap[String, Int] = ListMap(counter.toSeq.sortWith(_._2 > _._2): _*)

    var clusters: Array[Array[String]] = Array()

    descendentOrder.foreach(pair => {
      val tagToCheck: String = pair._1
      var neighbors: Array[String] = Array()

      tags.foreach(arr => {
        if (arr.contains(tagToCheck))
          arr.foreach(tag => neighbors :+= tag)
      })

      val index: Int = indexClusters(tagToCheck, clusters)
      if (index < 0) {
        clusters :+= neighbors.distinct
      } else {
        val unionSet: Set[String] = clusters(index).toSet.union(neighbors.toSet)
        clusters(index) = unionSet.toArray
      }
    })

    val clustersTags = new mutable.HashMap[Int, Int]
    var k = 0
    clusters.foreach(cluster => {
      clustersTags(k) = cluster.length
      k += 1
    })

    val meanFrequencyValue: Int = descendentOrder.values.sum / descendentOrder.size
    val unFrequentWords = descendentOrder.filter(_._2 < meanFrequencyValue).keys.toArray

//    val frequentPairs = descendentOrder.filter(_._2 >= meanFrequencyValue).take(50)
//    println("Getting the information about the tags in the category ...")
//    val pw = new PrintWriter(new File("path.csv"))
//    frequentPairs.foreach(pair => pw.write("cat," + pair._1 + "," + pair._2 + "\n"))
//    pw.close()

    println("Filtering out answers with infrequent words ...")
    val answersFrequent = usagesCategory.filter(row => {
      val tagsProcessed = row.getAs[String]("Tags")
        .replaceAll("><", " ")
        .substring(1)
        .dropRight(1)
        .split(" ")
      val remainingTags = tagsProcessed.filter(tag => !unFrequentWords.contains(tag))
      remainingTags.length > 1 && remainingTags.length == tagsProcessed.length
    })

    println("Filtering out answers with classes in the JDK of the system  ...")
    val classesSystem: Array[String] = getSystemClasses()

    val selectedAnswers = answersFrequent.filter(row => {
      val classesProcessed: Array[String] = row.getAs[String]("Classes").split("\\|")
      val remainingClasses = classesProcessed.filter(clazz => !classesSystem.contains(clazz))
      val remainingClasses2 = remainingClasses.filter(clazz => !excludedListClasses.contains(clazz))

      remainingClasses2.length == classesProcessed.length
    })

//    // TODO: Get the barplot of this information
//    println("Saving the filtered information ...")
//    selectedAnswers
//        .coalesce(1)
//        .write
//        .csv("data/libraries/libraries_similarities/similarities_filtered/i_o_identifiers")

    println("Joining data ...")
    val dataJoined = selectedAnswers.join(dataNoNull,
      selectedAnswers("Answer_ID") === dataNoNull("Answer_ID"))
      .drop(dataNoNull.col("Answer_ID"))
      .drop(dataNoNull.col("Title"))

    println("Extracting the actual usages from the filtered data ...")
    val pw = new PrintWriter(new File("data/usages/mocking.csv"))

    dataJoined.collect().foreach(row => {
      val postID = row.getAs[Int]("Question_ID")
      val answerID = row.getAs[Int]("Answer_ID")
      val classes = row.getAs[String]("Classes").split("\\|").distinct

      val groupId = row.getAs[String]("Group_ID")
      val artifactId = row.getAs[String]("Artifact_ID")

      val title = row.getAs[String]("Title")
      val tags = row.getAs[String]("Tags")
      val prevalence = row.getAs[Double]("Prevalence")
      val body = row.getAs[String]("Body")

      val lineUsagesLibrary: Array[String] = lineUsage(classes, body)

      if (lineUsagesLibrary.length > 0) {
        lineUsagesLibrary.foreach(_ => {
          pw.write(s"$postID,$answerID,$groupId,$artifactId,$title,$tags,${classes.mkString("|")},$prevalence\n")
        })
      }
    })

    pw.close()
    sparkSession.stop()
  }
}
