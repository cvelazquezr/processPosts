package word2vec

import spark_conf.Context
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}

object Model extends Context {

  def main(args: Array[String]): Unit = {
    val sc = sparkSession.sparkContext
    println("Loading training data ...")
    val input = sc.textFile("data/model/training_data/librariesProjects.txt").map(line => line.split(" ").toSeq)

    val word2vec = new Word2Vec()

    println("Fitting the model ...")
    val model = word2vec
      .setMinCount(3)
      .setVectorSize(100)
      .setNumIterations(20)
      .setWindowSize(8)
      .setNumPartitions(20)
      .fit(input)

    println("Saving the model on disk ...")
    model.save(sc, "data/model/modelLibraries")

    println("Done!!!")
    sparkSession.close()
  }
}
