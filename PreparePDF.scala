package fh.big.abschlussaufgabe

import java.io.{BufferedReader, FileReader}
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.BytesWritable
import org.apache.pdfbox.pdmodel.PDDocument
import java.util.Arrays
import org.apache.pdfbox.text.PDFTextStripper
import java.io.StringWriter
import scala.collection.mutable.HashSet

/**
  * Parse and convert PDF to text and smooth/filter the existing words
  * so they are easily usable for the classification algorithm
  */
object PreparePDF {
	val stopwords = initStopwordsList()

	/**
	  * Parse stopwords from File
	  * @return stopwords as HashSet
	  */
	def initStopwordsList(): HashSet[String] = {
		val br = new BufferedReader(new FileReader("german_stopwords.txt"))
		var stopwordsSet = new HashSet[String]()
		while(br.ready()) {
			stopwordsSet.add(br.readLine())
		}
		return stopwordsSet
	}

	/**
	  * Parse PDF, smooth/filter and categorize
	  *
	  * @param pair
	  * @return (type of PDF, smoothed/filtered words, name of the PDF)
	  */
   def processPDF(pair: (Text, BytesWritable)): (DocumentType.Value, Array[String], String) = {
    val copy = Arrays.copyOf(pair._2.getBytes(), pair._2.getLength());
    val doc = PDDocument.load(copy)
    val ts = new PDFTextStripper
    val output = new StringWriter
    ts.writeText(doc, output)

    val content = output.toString()
    val pdfName = pair._1.toString().toLowerCase()
    var docType: DocumentType.Value = null
    
    
    if (pdfName.startsWith("lebenslauf")) {
      docType = DocumentType.LEBENSLAUF
    } else if (pdfName.startsWith("empfehlung")) {
      docType = DocumentType.EMPFEHLUNG
    } else if (pdfName.startsWith("rechnung")) {
      docType = DocumentType.RECHNUNG
    } else if (pdfName.startsWith("bewerbung")) {
      docType = DocumentType.BEWERBUNG
    } else {
      throw new Exception(pdfName + " could not be categorized")
    }
    doc.close();
    //val words = content.split("\\s+") // when no filtering/smoothing is wanted
    val words = cleanDocument(content) // start smoothing/filtering content

	return (docType, words, pdfName)
  }

	/**
	  * Clean PDF from content not helpful for classification algorithm
	  *
	  * @param content Content of a document
	  * @return Cleaned document
	  */
	def cleanDocument(content: String): Array[String] = {
		val cleanedPunctDigits = removePunctuationDigits(content)
		
		val cleanedStopwords = cleanedPunctDigits.filter(word => !stopwords.contains(word))
		//val cleanedStopwords = cleanedPunctDigits // when no stopwords removing is wanted
		
		return cleanedStopwords.filter(word => !word.equals(""))
	}

	/**
	  * Replace urls, nonword-characters (digits)
	  *
	  * @param word Word contained in a document
	  * @return Word without nonword-characters and urls
	  */
	private def replaceFct(word: String): String = {
		val replacedUmlauts = word match {
			case _ if word contains 'ä' => word.replaceAll("ä", "ae")
			case _ if word contains 'ü' => word.replaceAll("ü", "ue")
			case _ if word contains 'ö' => word.replaceAll("ö", "oe")
			case _ if word contains 'ß' => word.replaceAll("ß", "ss")
			case _ => word
		}
		val replacedURLs = replacedUmlauts.replaceAll("\\w*(.com|.de)\\w*", "")
		val replacedPunctuationDigits = replacedURLs.replaceAll("[\\s\\d\\W]", "")
		
		return replacedPunctuationDigits
	}

	/**
	  * Cleans content String from any punctuation, urls, and digits
	  *
	  * @param word word from the document
	  * @return processed word
	  */
	private def removePunctuationDigits(word: String): Array[String] = {
		word.split("\\s").map(word => replaceFct(word.toLowerCase))
	}
}