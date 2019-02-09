package fh.big.abschlussaufgabe

/**
 * Zu klassifizierende Typen.
 * Trainingsdaten werden anhand der PDF-Namen gelabelt,
 * Testdaten anhand des Algorithmus.
 */
object DocumentType extends Enumeration{
  type DocumentType = Value
  val BEWERBUNG, LEBENSLAUF, RECHNUNG, EMPFEHLUNG = Value
}

class DocumentType {}