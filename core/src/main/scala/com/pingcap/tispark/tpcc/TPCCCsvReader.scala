package com.pingcap.tispark.tpcc

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

case class TPCCCsvReader(path: String,
                         targetPath: String,
                         split: Int,
                         csvPrefix: String,
                         spark: SparkSession) {
  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      throw new IllegalArgumentException(s"$dir does not contain any files")
    }
  }

  def extractCsvFiles(files: List[File]): List[File] = {
    files.filter(f => f.getName.endsWith(".csv")).filter(f => !f.getName.startsWith("config"))
  }

  val weights: Array[Double] = Array.fill(split) {
    1.0
  }

  def normalizeCsvFiles(file: File): Unit = {
    val path = file.getPath
    val name = file.getName.substring(0, file.getName.indexOf(".csv"))
    val csvDf: DataFrame = spark.read
      .format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .option("mode", "DROPMALFORMED")
      .option("treatEmptyValuesAsNulls", "true")
      .option("nullValue", "NULL")
      .option("inferSchema", value = true)
      .load(path)

    val stringTemplate = s"$targetPath/${name}_%d"
    csvDf.randomSplit(weights).zipWithIndex.foreach { dfWithIdx =>
      val savedPath = stringTemplate.format(dfWithIdx._2)
      dfWithIdx._1
        .repartition(1)
        .write
        .format("csv")
        .option("delimiter", ",")
        .option("nullValue", "NULL")
        .save(savedPath)

      val savedCsvFiles = extractCsvFiles(getListOfFiles(savedPath))
      if (savedCsvFiles.size > 1) {
        throw new UnsupportedOperationException("should only saved one csv files")
      }

      val newSavedCsvPath = s"$targetPath/${csvPrefix.format(name, dfWithIdx._2)}"
      val renamed = savedCsvFiles.head.renameTo(new File(newSavedCsvPath))
      if (!renamed) {
        throw new IllegalArgumentException("failed to renamed files")
      }
      FileUtils.deleteDirectory(new File(savedPath))
    }

  }

  val csvFiles = extractCsvFiles(getListOfFiles(path))

  csvFiles.foreach(
    f => normalizeCsvFiles(f)
  )
}
