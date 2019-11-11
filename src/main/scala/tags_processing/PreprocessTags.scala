package tags_processing

import java.util.jar.JarFile

import org.apache.bcel.classfile.ClassParser
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}
import org.jsoup.select.Elements
import spark_conf.Context

import scala.collection.JavaConverters._

// Process the tags and return bigrams of tags sorted by their frequencies (NEEDS CLEANING)

case class TagsPost(tags: Array[String], var covered: Boolean=false)

object PreprocessTags extends Context {
  import sparkSession.sqlContext.implicits._
//  implicit def kryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] =
//    org.apache.spark.sql.Encoders.kryo[A](ct)

  def indexClusters(tag: String, clusters: Array[Array[String]]): Int = {
    val tagSplitted: Array[String] = tag.split(",")
    for (i <- clusters.indices) {
      if (clusters(i).contains(tagSplitted(0)) || clusters(i).contains(tagSplitted(1)))
        return i
    }
    -1
  }

  def spreadCovered(tagsPost: Array[TagsPost], tag: String): Array[TagsPost] = {
    var wasCovered: Int = 0

    val makeCover = tagsPost.map(tagPost => {
      if (tagPost.tags.contains(tag)) {
        tagPost.covered = true
        wasCovered += 1
        tagPost
      } else {
        tagPost
      }
    })
    println(tag, wasCovered)
    makeCover
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

  def containsSelectedTags(tagsInAnalysis: Array[String], tagsSelected: Array[String]): Boolean = {
    tagsSelected.toSet.intersect(tagsSelected.toSet).nonEmpty
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

    val categoryInStudy: String = "mocking"
    val categoryInformation = usagesCategory.where(usagesCategory.col("Library") === categoryInStudy)

    println("Joining data ...")
    val joinedData = categoryInformation
      .join(dataNoNull, "Answer_ID")
      .drop(dataNoNull.col("Answer_ID"))
      .drop(dataNoNull.col("Question_ID"))
      .drop(dataNoNull.col("Title"))
      .dropDuplicates()
      .filter(row => linesOfCode(row.getAs[String]("Body")))

    val dataCounter: Long = joinedData.count()
    val confidenceInterval: Int = 383

    println(s"Selecting $confidenceInterval random posts ...")
    val sampleData = joinedData.sample(true, 1D * confidenceInterval/dataCounter).limit(confidenceInterval)

    sampleData
      .select("Question_ID", "Answer_ID", "Similarity")
      .coalesce(1)
      .write
      .option("header", "true")
      .csv(s"data/samples/$categoryInStudy")

//    val thresholds: Array[Double] = Array(0.1, 0.2, 0.3, 0.4, 0.5)

//    val tagsAnswers = joinedData.select("Tags", "Answer_ID")
//    val infoAnswers = tagsAnswers.map(row => {
//      val tagsProcessed: Array[String] = row.getAs[String]("Tags")
//        .replaceAll("<", "")
//        .replaceAll(">", " ")
//        .trim.split(" ")
//        .filter(tag => !excludedListTags.contains(tag))
//
//      val answer_Id: Int = row.getAs[Int]("Answer_ID")
//      (answer_Id, tagsProcessed)
//    }).collect().toMap
//
//    var postsVisited: Array[Int] = Array()
//    val frequenciesTags = new mutable.HashMap[String, Int]()
//    val total: Int = descendentOrderTags.size
//    var k: Int = 0
//
//    descendentOrderTags.foreach(pair => {
//      println(s"Processing $k/$total")
//      k += 1
//      val tagInAnalysis: String = pair._1
//
//      infoAnswers.foreach(answer => {
//        if (answer._2.contains(tagInAnalysis) && !postsVisited.contains(answer._1)) {
//          postsVisited :+= answer._1
//          if (frequenciesTags.contains(tagInAnalysis)) {
//            frequenciesTags(tagInAnalysis) += 1
//          } else {
//            frequenciesTags(tagInAnalysis) = 1
//          }
//        }
//      })
//    })
//
//    frequenciesTags.foreach(println)


//    println("Selecting a sample of the most ranked answers ...")
//    val firstBatchTags = joinedData.orderBy(desc("Similarity")).limit(100).flatMap(row => {
//      val tagsProcessed: Array[String] = row.getAs[String]("Tags")
//        .replaceAll("<", "")
//        .replaceAll(">", " ")
//        .trim.split(" ")
//        .filter(tag => !excludedListTags.contains(tag))
//      tagsProcessed
//    }).collect().toSeq.groupBy(identity).mapValues(_.size)
//
//    val descendentOrderTags: ListMap[String, Int] = ListMap(firstBatchTags.toSeq.sortWith(_._2 > _._2): _*)
//
//    var selectedTags: Array[String] = Array()
//    descendentOrderTags.filter(_._2 > 1).foreach(pair => {
//      println(pair._1)
//      val evaluation = scala.io.StdIn.readInt()
//
//      if (evaluation == 2)
//        selectedTags :+= pair._1
//    })
//
//    val tagsWithSelected = new mutable.HashMap[String, Int]()
//    val tagsBatch = joinedData.orderBy(desc("Similarity")).limit(10000).flatMap(row => {
//      val tagsProcessed: Array[String] = row.getAs[String]("Tags")
//        .replaceAll("<", "")
//        .replaceAll(">", " ")
//        .trim.split(" ")
//        .filter(tag => !excludedListTags.contains(tag))
//      val processedTags = tagsProcessed.map(tag => {
//        if (containsSelectedTags(tagsProcessed, selectedTags)) {
//          if (!selectedTags.contains(tag)) {
//            return (tag, 1)
////            if (tagsWithSelected.contains(tag)) {
////              tagsWithSelected(tag) += 1
////            } else {
////              tagsWithSelected(tag) = 1
////            }
//          } else ("",0)
//        } else ("",0)
//      })
//      processedTags
//    }).filter(_._2 > 0).collect().toMap
//
//    println(tagsBatch.size)
//
//    val batchOrdered: ListMap[String, Int] = ListMap(tagsBatch.toSeq.sortWith(_._2 > _._2): _*)
//    batchOrdered.foreach(println)


//    var threshold: Double = topThreshold - 0.1

//    val pairsCounter = new mutable.HashMap[String, (Int, Int, Array[Double])]

//    while(threshold >= 0.1) {
//      println(s"Threshold: $topThreshold")
//      val selectedLibraries = categoryInformation.where(usagesCategory.col("Similarity") < topThreshold &&
//        usagesCategory.col("Similarity") >= threshold)
//
//      val joinedData = selectedLibraries
//        .join(dataNoNull, "Answer_ID")
//        .drop(dataNoNull.col("Answer_ID"))
//        .drop(dataNoNull.col("Question_ID"))
//        .drop(dataNoNull.col("Title"))
//        .dropDuplicates()
//        .filter(row => linesOfCode(row.getAs[String]("Body")))
//
//      println("Processing the tags in the category ...")
//      val tags = joinedData.select("Tags", "Similarity")
//

//    println("Extracting bigrams from tags ...")
//    val tagsBigrams = tags.flatMap(row => {
////        val similarity: Double = row.getAs[Double]("Similarity")
//
//      val tagsArray: Array[String] = row.getAs[String]("Tags")
//        .replaceAll("<", "")
//        .replaceAll(">", " ")
//        .trim.split(" ")
//        .filter(tag => !excludedListTags.contains(tag))
//
//      val combinations: Array[Array[String]] = tagsArray.combinations(2).toArray[Array[String]]
//      val combinationsGenerated: Array[String] = combinations.map(arr => {
//        val firstValue: String = arr(0)
//        val secondValue: String = arr(1)
//
//        val textOrder: String = if (firstValue > secondValue) secondValue + "," + firstValue
//        else firstValue + "," + secondValue
//
//        textOrder
//      })
//      combinationsGenerated
//    })
//
//    println("Counting bigrams ...")
//    val counterBigrams = new mutable.HashMap[String, Int]()
//    tagsBigrams.collect().foreach(bigram => {
//
//      if (counterBigrams.contains(bigram)) {
//        counterBigrams(bigram) += 1
//      } else {
//        counterBigrams(bigram) = 1
//      }
//    })
//
//    val descendentOrder: ListMap[String, Int] = ListMap(counterBigrams.toSeq.sortWith(_._2 > _._2): _*)
//
//    val pw: PrintWriter = new PrintWriter(new File("data/results/json_bigrams.txt"))
//    descendentOrder.filter(_._2 > 5).foreach(pair => {
//      pw.write(s"${pair._1}\n")
//    })
//    pw.close()

//      // Storing the information
//      descendentOrder.foreach(pair => {
//        val name: String = pair._1
//        val occurrences: (Int, Array[Double]) = pair._2
//
//        if (pairsCounter.contains(name)) {
//          val oldValues = pairsCounter(name)
//          pairsCounter(name) = (oldValues._1 + 1,
//            oldValues._2 + occurrences._1,
//            oldValues._3.toSet.union(occurrences._2.toSet).toArray)
//        } else {
//          pairsCounter(name) = (1, occurrences._1, occurrences._2)
//        }
//      })
//      topThreshold = threshold
//      threshold -= 0.1
//    }
//
//    println("Calculating average for the similarities")
//    val weightedPairs = pairsCounter.map(item => {
//      (item._1, (item._2._1, item._2._2, item._2._3.sum / item._2._3.length))
//    })
//
//    println("Selecting tags ...")
//    val pairsOrdered = ListMap(weightedPairs.toSeq.sortWith(_._2._2 > _._2._2): _*).filter(_._2._2 >= 5)
//
//    println(pairsOrdered.size)
//
//    val pw: PrintWriter = new PrintWriter(new File(s"data/results/tags_selection/$categoryInStudy.txt"))
//    pairsOrdered.foreach(item => {
//      val textToWrite: String = item._1 + "\n"
//     pw.write(textToWrite)
//    })
//    pw.close()

    sparkSession.stop()
  }
}
