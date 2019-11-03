package tags_processing

import java.io.{File, PrintWriter}
import scala.io.Source

// Manual inspection of the bigrams of tags

case class Word(identifier: String, var covered: Int = 0)
case class Pair(firstWord: Word, secondWord: Word)

object SelectionTags {

  def amountCovered(arr: Array[Pair]): Unit = {
    val amount: Int = arr.count(pair => pair.firstWord.covered == 0 || pair.secondWord.covered == 0)
    println(s"Tags covered: ${amount}")
  }

  def spreadAssignment(currentCovered: Array[Pair], word: String, assignment: Int): Array[Pair] = {
    currentCovered.map(pair => {
      if (pair.firstWord.identifier.equals(word)) {
        pair.firstWord.covered = assignment
        pair
      } else if (pair.secondWord.identifier.equals(word)) {
        pair.secondWord.covered = assignment
        pair
      } else {
        pair
      }
    })
  }

  def main(args: Array[String]): Unit = {
    val categoryInStudy: String = "json_libraries"
    val pathTags: String = s"data/results/tags_selection/$categoryInStudy.txt"

    val source: Source = Source.fromFile(pathTags)
    val lines: Array[String] = source.getLines().toArray

    val pairs = lines.map(line => {
      val lineSplitted: Array[String] = line.split(",")
      Pair(Word(lineSplitted(0)), Word(lineSplitted(1)))
    })

    // Temp code to read saved information
    val source2: Source = Source.fromFile("data/results/tags_selection/temp_output.txt")
    val lines2: Array[String] = source2.getLines().toArray
    val words: Array[String] = lines2.indices.filter(_ %  2 == 0).map(index => {lines2(index)}).toArray
    val numbers: Array[Int] = lines2.indices.filter(_ %  2 != 0).map(index => {lines2(index).toInt}).toArray

    val mappingIndexed: IndexedSeq[(String, Int)] = for (i <- words.indices) yield words(i) -> numbers(i)
    val mapping: Map[String, Int] = mappingIndexed.toMap

    mapping.foreach(item => {
      spreadAssignment(pairs, item._1, item._2)
    })

    println("You will have to put scores 1 for (Not Valid) and 2 for (Valid) to tags:")
    for (i <- pairs.indices) {
      var evaluation: Int = -2

      if (pairs(i).firstWord.covered == 0) {
        val firstWord: String = lines(i).split(",")(0)

        while (evaluation <= 0) {
          println(firstWord)

          evaluation = scala.io.StdIn.readInt()
          if (evaluation == -1)
            amountCovered(pairs)
        }
        pairs(i).firstWord.covered = evaluation

        // Spread the information
        spreadAssignment(pairs, firstWord, evaluation)
      }

      if (pairs(i).secondWord.covered == 0) {
        val secondWord: String = lines(i).split(",")(1)
        evaluation = -2

        while (evaluation <= 0) {
          println(secondWord)

          evaluation = scala.io.StdIn.readInt()
          if (evaluation == -1)
            amountCovered(pairs)
        }
        pairs(i).secondWord.covered = evaluation

        //Spread the information
        spreadAssignment(pairs, secondWord, evaluation)
      }
    }
    val pw: PrintWriter = new PrintWriter(new File(pathTags))

    pairs.foreach(pair =>  {
      val textToWrite: String = s"${pair.firstWord.identifier},${pair.secondWord.identifier}," +
        s"${pair.firstWord.covered},${pair.secondWord.covered}\n"
      pw.write(textToWrite)
    })
    pw.close()

    source.close()
  }
}
