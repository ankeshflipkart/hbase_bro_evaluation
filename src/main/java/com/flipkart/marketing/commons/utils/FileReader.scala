package com.flipkart.marketing.commons.utils

import java.io.{FileNotFoundException, BufferedReader, File}

/**
 * Created by shivam.rai on 11/03/16.
 */
class FileReader {
  private var fileName: String = null
  private var file: File = null

  def this(fileName: String) {
    this()
    this.fileName = fileName
    this.file = new File(fileName)
  }

  def getFileReader: BufferedReader = {
    try {
      val fileReader = new java.io.FileReader(file)
      val br: BufferedReader = new BufferedReader(fileReader)
      br
    }
    catch {
      case e: FileNotFoundException => {
        null
      }
    }
  }
}
