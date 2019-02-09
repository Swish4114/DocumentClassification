package fh.big.abschlussaufgabe

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.BytesWritable
import org.apache.log4j.Logger
import org.apache.log4j.Level


/**
  * Classification of pdf-documents into one of four classes: {Bewerbung, Empfehlung, Lebenslauf, Rechnung}
  * - Invoking from main-method with different splits of training and test-data
  * - Training the dataset using the bayes-classification-algorithm
  * - Categorizing the testdata based on the results of the training-data
  * - Printing the statistics of the results compared to expected results
 */
object Classification {
  
  val conf = new SparkConf().setAppName("test").setMaster("local[2]")
  val sc = new SparkContext(conf)
  
  def main(args: Array[String]): Unit = {
    analyse(0.9, 0.1)
    analyse(0.8, 0.2)
    analyse(0.7, 0.3)
    analyse(0.6, 0.4)
    analyse(0.5, 0.5)
    analyse(0.4, 0.6)
    analyse(0.3, 0.7)
    analyse(0.2, 0.8)
    analyse(0.1, 0.9)
    //while (true) {}
  }


	/**
	  * Sequencing the PDFs and converting into text files.
	  * Splitting into training and test data.
	  * TrainingPhase: Computing probabilities for words occuring in each category,
	  * which is done in the DocumentCategory class.
	  * After that Categories know how frequent each unique word appears in it.
	  * TestingPhase: Categorizing documents of the test dataset into one of the classes.
	  *
	  * @param percentTraining Percentage of dataset used as training data
	  * @param percentTest Percentage of dataset used as test data
	  */
  def analyse(percentTraining: Double, percentTest: Double) {
    
    Logger.getLogger("org").setLevel(Level.OFF)
    
    val read = sc.sequenceFile("seqfiledocs", classOf[Text], classOf[BytesWritable])
    val parsedData = read.map(pdf => PreparePDF.processPDF(pdf))
			.filter(tuple => !tuple._3.equals("bewerbung-bewerbungsanschrieben_8.pdf")).cache() // Cache to prevent Recomputation of RDD
    
    val splits = parsedData.randomSplit(Array(percentTraining, percentTest), seed = 11L) // Split into training and test data
    val trainingData = splits(0)
    val testData = splits(1).collect() // Execute tests on the driver program
    
    
    /**** Training Phase ****/
    // Word counts
    val numTrainingDocs: Long = trainingData.count()
    val allUniqueWords = parsedData.flatMap(_._2).distinct().cache() // Every unique word of every document
    val numUniqueWords: Long = allUniqueWords.count()

	// Create four categories, train data in each
    val bewerbung = new DocumentCategory(DocumentType.BEWERBUNG, allUniqueWords, numUniqueWords, trainingData, numTrainingDocs)
    val lebenslauf = new DocumentCategory(DocumentType.LEBENSLAUF, allUniqueWords, numUniqueWords, trainingData, numTrainingDocs)
    val rechnung = new DocumentCategory(DocumentType.RECHNUNG, allUniqueWords, numUniqueWords, trainingData, numTrainingDocs)
    val empfehlung = new DocumentCategory(DocumentType.EMPFEHLUNG, allUniqueWords, numUniqueWords, trainingData, numTrainingDocs)
                
   
    /**** Testing Phase ****/

	  /**
		* Categorize one document and compare with expected document-type.
		*
 		* @param pdfContent Content of the analyzed document
		* @param pdfDocType Expected Category
		* @param pdfName PDF-name
		* @param printDebug if true, debug-messages are printed
		* @return true, if category was matched correctly, false if not
		*/
    def doTestForPDF(pdfContent: Array[String], pdfDocType: DocumentType.Value, pdfName: String, printDebug: Boolean = false): Boolean = {
      val categories = List(DocumentType.BEWERBUNG, DocumentType.RECHNUNG, DocumentType.LEBENSLAUF, DocumentType.EMPFEHLUNG)
      val probabilities = List(bewerbung.getPForOtherDocument(pdfContent), rechnung.getPForOtherDocument(pdfContent),
		  					   lebenslauf.getPForOtherDocument(pdfContent), empfehlung.getPForOtherDocument(pdfContent))
      val analysedType = categories(probabilities.indexOf(probabilities.max))
      var isMatch = analysedType.equals(pdfDocType)
      if (printDebug) if (isMatch) println(pdfDocType + " - yes - " + pdfName + " - " + probabilities) else println(pdfDocType + " - no - " + pdfName + " - " + probabilities)
      return isMatch
    }
  
    
    var numTests = 0
    var numMatches = 0
	// Check for every document in the test data if it matches the correct category
    for ((cat, content, pdfName) <- testData) {
      if (doTestForPDF(content, cat, pdfName, false)) {
        numMatches += 1
      }
      numTests += 1
    }

	// Print out statistics
    println("Result " + "(" + numTrainingDocs + ", " + percentTraining + ", " + percentTest + ") = " + numMatches + "/" + numTests + " = " + numMatches/numTests.toDouble)

  }
  
  class Classification {}
}

