package fh.big.abschlussaufgabe

import org.apache.spark.rdd.RDD

/**
  * 
  * 
  * @param docType Represented Category
  * @param allUniqueWords All unique words in the dataset
  * @param numUniqueWords Amount of unique words in the dataset
  * @param trainingData RDD consisting of training data as followed: [(category, words in the document, name of the document)]
  * @param numTrainingData Amount of elements in the training data
  */
@SerialVersionUID(100L)
class DocumentCategory(val docType: DocumentType.Value,
					   val allUniqueWords: RDD[String],
					   val numUniqueWords: Long,
					   val trainingData: RDD[(DocumentType.Value, Array[String], String)],
					   val numTrainingData: Long) extends Serializable {
  val categoryData: RDD[(DocumentType.Value, Array[String], String)] = trainingData.filter(document => document._1 == docType) // Filter training data by category
  val pCategory: Double = categoryData.count() / numTrainingData.toDouble // Calculate probability of document being in this category
  val allWordsCategory: RDD[String] = categoryData.flatMap(_._2).cache() // Flatten all Arrays of Strings to one Array
  val numWordsCategory: Long = allWordsCategory.count() // Number of words in all documents of this category

  val allWordsCategoryFrequency: RDD[(String, Double)] = allUniqueWords.map((_, 0)) // Map string to tuple like (word, 0) so we can join
		  											// Join allUniqueWordsRDD with uniqueWordsCategory so all words are contained in the resulting RDD
                                                  .leftOuterJoin(allWordsCategory.map((_, 1)))
		  											// Map (word, (0, Option)) to (word, (0,0 or 1))
                                                  .map(pairWordOccurence => (pairWordOccurence._1, (pairWordOccurence._2._1, pairWordOccurence._2._2.getOrElse(0))))
		  											// Aggregates word-occurences of the same keys
                                                  .reduceByKey((tuple1, tuple2) => (tuple1._1+tuple2._1, tuple1._2+tuple2._2))
		  											// Calculate the probability of each word to occur in this category
                                                  .map(tuple => (tuple._1, (tuple._2._1 + tuple._2._2 + 1) / (numWordsCategory + numUniqueWords).toDouble))
		  										  .cache()

	/**
	  * Calculate probability of one external document matching this category
	  *
	  * @param otherDocWords Words of the other document
	  * @return probability of a match
	  */
	def getPForOtherDocument(otherDocWords: Array[String]): Double = {
    val pOfAllWords: Double = allWordsCategoryFrequency
                        .filter(tuple => otherDocWords.contains(tuple._1)) // Only use the words of the external document
                        .map(tuple => Math.log(tuple._2)) // Calculate logarithm of probability
                        .reduce((p1, p2) => p1 + p2) // Add logarithms
    return Math.log(pCategory) + pOfAllWords
  }
                                                  

}