package com.msb.utils

import java.io.{BufferedReader, FileReader, IOException, InputStream}
import java.nio.file.{Files, Paths}
import java.util.Properties

object PropertiesFileReader {

  def readConfig(path: String): Properties = {
    val prop = new Properties
    try {
      val input = Files.newInputStream(Paths.get(path))
      try {
        prop.load(input)
        return prop
      } catch {
        case ex: IOException => ex.printStackTrace()
      } finally {
        if (input != null) input.close()
      }
    }
    prop
  }

  @throws[IOException]
  def fileToString(filePath: String): String = {
    val br = new BufferedReader(new FileReader(filePath))
    val sb = new StringBuilder
    try {
      var line = br.readLine
      while (line != null) {
        sb.append(line)
        sb.append(System.lineSeparator)
        line = br.readLine
      }
    } finally br.close()
    sb.toString
  }

}
