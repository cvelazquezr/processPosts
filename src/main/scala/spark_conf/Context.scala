package spark_conf

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Context {

  lazy val sparkConf: SparkConf = new SparkConf()
    .setAppName("Process Posts")
    .setMaster("local[*]")
    .set("spark.cores.max", "8")
    .set("spark.executor.memory", "70g")
    .set("spark.driver.memory", "50g")
    .set("spark.memory.offHeap.enabled", "true")
    .set("spark.memory.offHeap.size", "50g")
    .set("spark.driver.maxResultSize", "50g")

  lazy val sparkSession: SparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")
}
