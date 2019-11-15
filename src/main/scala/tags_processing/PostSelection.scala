package tags_processing

import java.io.{File, PrintWriter}

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import spark_conf.Context
import org.apache.spark.sql.functions._

import scala.collection.immutable.ListMap
import scala.collection.mutable

// Posts selection by the similarities in their answers towards the same category

import scala.reflect.ClassTag

object PostSelection extends Context {
  implicit def kryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] =
    org.apache.spark.sql.Encoders.kryo[A](ct)

  def main(args: Array[String]): Unit = {
    println("Loading usages detected for the categories ...")
    val usagesCategory = sparkSession
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/categories/categories_similarities.csv")

    val zipper = udf[Seq[(String, Double)], Seq[String], Seq[Double]](_.zip(_))

    val groupedCategories = usagesCategory.groupBy("Question_ID")
          .agg(collect_list("Library") as "Categories",
            collect_list("Similarity") as "Similarities")
          .withColumn("Categories", zipper(col("Categories"), col("Similarities")))
          .drop("Similarities")


    val selectedPosts = groupedCategories.map(row => {
      val questionID: Int = row.getAs[Int]("Question_ID")
      val categories = row
        .getAs[mutable.WrappedArray[GenericRowWithSchema]]("Categories").toArray
        .map(genericRow => {
          val category: String = genericRow.getAs[String](0)
          val similarity: Double = genericRow.getAs[Double](1)

          (category, similarity)
        })

      if (categories.length >= 2) {
        val values: Seq[String] = categories.map(_._1)
        val repetitions: Map[String, Int] = values.groupBy(identity).mapValues(_.size)

        val descendentOrder: ListMap[String, Int] = ListMap(repetitions.toSeq.sortWith(_._2 > _._2): _*)
        val orderedRepetitions: Array[Int] = descendentOrder.values.toArray
        val orderedCategories: Array[String] = descendentOrder.keys.toArray

        if (orderedRepetitions(0) > values.size / 2) {
          val chosenCategory: String = orderedCategories(0)
          val filteredPerCategory = categories.filter(_._1.equals(chosenCategory))
          val similarities: Array[Double] = filteredPerCategory.map(_._2)
          val meanSimilarities: Double = similarities.sum / similarities.length

          if (meanSimilarities >= 0.4) {
            s"$questionID,$chosenCategory,$meanSimilarities\n"
          } else ""
        } else ""
      } else ""
    }).filter(_.length > 0)

    println("Total posts selected in the 5 categories ...")
    println(selectedPosts.count())

    println("Saving selected posts ...")
    val pw: PrintWriter = new PrintWriter(new File(s"data/categories/selectedPosts.csv"))
    selectedPosts.collect().foreach(line => pw.write(line))
    pw.close()

    sparkSession.close()
  }
}
