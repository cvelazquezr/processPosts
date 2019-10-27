package word2vec

import java.io.{File, FileNotFoundException}

import scala.collection.mutable
import scala.io.Source

class Word2VecLocal extends Serializable {
  /** Map of words and their associated vector representations */
  private val vocab = new mutable.HashMap[String, Array[Float]]()

  /** Number of words */
  private var numWords = 0

  /** Number of floating-point values associated with each word (i.e., length of the vectors) */
  private var vecSize = 0

  /** Load data from a binary file.
   * @param filename Path to file containing word projections.
   */
  def load(filename: String): Unit = {
    // Check edge case
    val file = new File(filename)
    if (!file.exists()) {
      throw new FileNotFoundException("Vector file not found <" + file.toString + ">")
    }

    println("Loading Word2vec model ...")
    val source: Source = Source.fromFile(filename)
    var k: Int = 0

    for (line  <- source.getLines()) {
      val splittedLine: Array[String] = line.trim.split(" ")
      if (k == 0) {
        numWords = splittedLine(0).toInt
        vecSize = splittedLine(1).toInt

        println(s"Number of words: $numWords\nNumber of dimensions: $vecSize")
      } else {
        val word: String = splittedLine.head
        val vector: Array[Float] = splittedLine.tail.map(numberStr => numberStr.toFloat)

        vocab.put(word, vector)
      }
      k  += 1
    }

    source.close()
    println("Loaded Word2Vec model")
  }

  /** Return the number of words in the vocab.
   * @return Number of words in the vocab.
   */
  def wordsCount: Int = numWords

  /** Size of the vectors.
   * @return Size of the vectors.
   */
  def vectorSize: Int = vecSize

  /** Clear internal data. */
  def clear() {
    vocab.clear()
    numWords = 0
    vecSize = 0
  }

  /** Check if the word is present in the vocab map.
   * @param word Word to be checked.
   * @return True if the word is in the vocab map.
   */
  def contains(word: String): Boolean = {
    vocab.get(word).isDefined
  }

  /** Get the vector representation for the word.
   * @param word Word to retrieve vector for.
   * @return The vector representation of the word.
   */
  def vector(word: String): Array[Float] = {
    vocab.getOrElse(word, Array[Float]())
  }

  /** Compute the Euclidean distance between two vectors.
   * @param vec1 The first vector.
   * @param vec2 The other vector.
   * @return The Euclidean distance between the two vectors.
   */
  def euclidean(vec1: Array[Float], vec2: Array[Float]): Double = {
    assert(vec1.length == vec2.length, "Uneven vectors!")
    var sum = 0.0
    for (i <- vec1.indices) sum += math.pow(vec1(i) - vec2(i), 2)
    math.sqrt(sum)
  }

  /** Compute the Euclidean distance between the vector representations of the words.
   * @param word1 The first word.
   * @param word2 The other word.
   * @return The Euclidean distance between the vector representations of the words.
   */
  def euclidean(word1: String, word2: String): Double = {
    assert(contains(word1) && contains(word2), "Out of dictionary word! " + word1 + " or " + word2)
    euclidean(vocab(word1), vocab(word2))
  }

  /** Compute the cosine similarity score between two vectors.
   * @param vec1 The first vector.
   * @param vec2 The other vector.
   * @return The cosine similarity score of the two vectors.
   */
  def cosine(vec1: Array[Float], vec2: Array[Float]): Double = {
    assert(vec1.length == vec2.length, "Uneven vectors!")
    var dot, sum1, sum2 = 0.0
    for (i <- vec1.indices) {
      dot += (vec1(i) * vec2(i))
      sum1 += (vec1(i) * vec1(i))
      sum2 += (vec2(i) * vec2(i))
    }
    dot / (math.sqrt(sum1) * math.sqrt(sum2))
  }

  /** Compute the cosine similarity score between the vector representations of the words.
   * @param word1 The first word.
   * @param word2 The other word.
   * @return The cosine similarity score between the vector representations of the words.
   */
  def cosine(word1: String, word2: String): Double = {
    assert(contains(word1) && contains(word2), "Out of dictionary word! " + word1 + " or " + word2)
    cosine(vocab(word1), vocab(word2))
  }

  /** Get the mean vector given a previous vector of words.
   * @param sentence The vector of words.
   * @return A vector that represent the mean of the other vectors.
   */
  def getSentenceVectors(sentence: Array[String]): Array[Float] = {
    val wordsPresence: Array[String] = sentence.filter(word => contains(word))
    val vectorsSum: Array[Float] = sumVector(wordsPresence)
    vectorsSum.map(n => n / wordsPresence.length.toFloat)

    val len: Float = math.sqrt(vectorsSum.map(x => x * x).sum).toFloat
    vectorsSum.map(x => x / len)
  }

  /** Compute the similarity between vectors of words
   * @param wordsFirst The first vector of words.
   * @param wordsSecond The second vector of words.
   * @return The similarity value between the two vectors.
   */
  def n_similarity(wordsFirst: Array[String], wordsSecond: Array[String]): Double = {
    val vectorsFirst: Array[Float] = getSentenceVectors(wordsFirst)
    val vectorsSecond: Array[Float] = getSentenceVectors(wordsSecond)

    var dot = 0.0

    for (i <-  vectorsFirst.indices) {
      dot += (vectorsFirst(i) * vectorsSecond(i))
    }
    dot
  }


  /** Compute the magnitude of the vector.
   * @param vec The vector.
   * @return The magnitude of the vector.
   */
  def magnitude(vec: Array[Float]): Double = {
    math.sqrt(vec.foldLeft(0.0){(sum, x) => sum + (x * x)})
  }

  /** Normalize the vector.
   * @param vec The vector.
   * @return A normalized vector.
   */
  def normalize(vec: Array[Float]): Array[Float] = {
    val mag = magnitude(vec).toFloat
    vec.map(_ / mag)
  }

  /** Find the vector representation for the given list of word(s) by aggregating (summing) the
   * vector for each word.
   * @param input The input word(s).
   * @return The sum vector (aggregated from the input vectors).
   */
  def sumVector(input: Array[String]): Array[Float] = {
    // Find the vector representation for the input. If multiple words, then aggregate (sum) their vectors.
    input.foreach(w => assert(contains(w), "Out of dictionary word! " + w))
    val vector = new Array[Float](vecSize)

    input.foreach(word => {
      val vocabVector: Array[Float] = vocab(word)
      for (j <- vector.indices) vector(j) += vocabVector(j)
    })
    vector
  }

  /** Find N closest terms in the vocab to the given vector, using only words from the in-set (if defined)
   * and excluding all words from the out-set (if non-empty).  Although you can, it doesn't make much
   * sense to define both in and out sets.
   * @param vector The vector.
   * @param inSet Set of words to consider. Specify None to use all words in the vocab (default behavior).
   * @param outSet Set of words to exclude (default to empty).
   * @param N The maximum number of terms to return (default to 40).
   * @return The N closest terms in the vocab to the given vector and their associated cosine similarity scores.
   */
  def nearestNeighbors(vector: Array[Float], inSet: Option[Set[String]] = None,
                       outSet: Set[String] = Set[String](), N: Integer = 40)
  : List[(String, Float)] = {
    // For performance efficiency, we maintain the top/closest terms using a priority queue.
    // Note: We invert the distance here because a priority queue will dequeue the highest priority element,
    //       but we would like it to dequeue the lowest scoring element instead.
    val top = new mutable.PriorityQueue[(String, Float)]()(Ordering.by(-_._2))

    // Iterate over each token in the vocab and compute its cosine score to the input.
    var dist = 0f
    val iterator = if (inSet.isDefined) vocab.filterKeys(k => inSet.get.contains(k)).iterator else vocab.iterator
    iterator.foreach(entry => {
      // Skip tokens in the out set
      if (!outSet.contains(entry._1)) {
        dist = cosine(vector, entry._2).toFloat
        if (top.size < N || top.head._2 < dist) {
          top.enqueue((entry._1, dist))
          if (top.length > N) {
            // If the queue contains over N elements, then dequeue the highest priority element
            // (which will be the element with the lowest cosine score).
            top.dequeue()
          }
        }
      }
    })

    // Return the top N results as a sorted list.
    assert(top.length <= N)
    top.toList.sortWith(_._2 > _._2)
  }

  /** Find the N closest terms in the vocab to the input word(s).
   * @param input The input word(s).
   * @param N The maximum number of terms to return (default to 40).
   * @return The N closest terms in the vocab to the input word(s) and their associated cosine similarity scores.
   */
  def distance(input: Array[String], N: Integer = 40): List[(String, Float)] = {
    // Check for edge cases
    if (input.isEmpty) return List[(String, Float)]()
    input.foreach(w => {
      if (!contains(w)) {
        println("Out of dictionary word! " + w)
        return List[(String, Float)]()
      }
    })

    // Find the vector representation for the input. If multiple words, then aggregate (sum) their vectors.
    val vector = sumVector(input)

    nearestNeighbors(normalize(vector), outSet = input.toSet, N = N)
  }

  /** Find the N closest terms in the vocab to the analogy:
   * - [word1] is to [word2] as [word3] is to ???
   *
   * The algorithm operates as follow:
   * - Find a vector approximation of the missing word = vec([word2]) - vec([word1]) + vec([word3]).
   * - Return words closest to the approximated vector.
   *
   * @param word1 First word in the analogy [word1] is to [word2] as [word3] is to ???.
   * @param word2 Second word in the analogy [word1] is to [word2] as [word3] is to ???
   * @param word3 Third word in the analogy [word1] is to [word2] as [word3] is to ???.
   * @param N The maximum number of terms to return (default to 40).
   *
   * @return The N closest terms in the vocab to the analogy and their associated cosine similarity scores.
   */
  def analogy(word1: String, word2: String, word3: String, N: Integer = 40): List[(String, Float)] = {
    // Check for edge cases
    if (!contains(word1) || !contains(word2) || !contains(word3)) {
      println("Out of dictionary word! " + Array(word1, word2, word3).mkString(" or "))
      return List[(String, Float)]()
    }

    // Find the vector approximation for the missing analogy.
    val vector = new Array[Float](vecSize)
    for (j <- vector.indices)
      vector(j) = vocab(word2)(j) - vocab(word1)(j) + vocab(word3)(j)

    nearestNeighbors(normalize(vector), outSet = Set(word1, word2, word3), N = N)
  }

  /** Rank a set of words by their respective distance to some central term.
   * @param word The central word.
   * @param set Set of words to rank.
   * @return Ordered list of words and their associated scores.
   */
  def rank(word: String, set: Set[String]): List[(String, Float)] = {
    // Check for edge cases
    if (set.isEmpty) return List[(String, Float)]()
    (set + word).foreach(w => {
      if (!contains(w)) {
        println("Out of dictionary word! " + w)
        return List[(String, Float)]()
      }
    })

    nearestNeighbors(vocab(word), inSet = Option(set), N = set.size)
  }

  /** Pretty print the list of words and their associated scores.
   * @param words List of (word, score) pairs to be printed.
   */
  def pprint(words: List[(String, Float)]): Unit = {
    println("\n%50s".format("Word") + (" " * 7) + "Cosine distance\n" + ("-" * 72))
    println(words.map(s => "%50s".format(s._1) + (" " * 7) + "%15f".format(s._2)).mkString("\n"))
  }
}
