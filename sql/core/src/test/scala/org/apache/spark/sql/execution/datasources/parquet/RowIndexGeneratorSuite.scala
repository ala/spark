/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.datasources.parquet

import java.io.File

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties._
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetOutputFormat}
import org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetDataSourceV2
import org.apache.spark.sql.functions.{col, max, min}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

class RowIndexGeneratorSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  val dataSourceName = "parquet"

  test("row index generation test") {
    withTempPaths(2) { paths =>
      paths.foreach(_.delete())
      val df = (0 to 10000).toDF("id")
      val rowIndexColName = RowIndexGenerator.ROW_INDEX_COLUMN_NAME

      val pathWithNoRowIdx = paths.head.getAbsolutePath
      val pathWithRowIdx = paths(1).getAbsolutePath

      // no row index in schema.
      df.repartition(1).write.format(dataSourceName).save(pathWithNoRowIdx)
      val noRowIdxDF = spark.read.format(dataSourceName).load(pathWithNoRowIdx)
      assert(!noRowIdxDF.columns.contains(rowIndexColName))

      // With row index in schema.
      val schemaWithRowIdx = df.schema.add(rowIndexColName, LongType, nullable = true)

      df.repartition(1)
        .write
        .format(dataSourceName)
        .option("parquet.block.size", 128) // Force the data to be split into multiple row groups.
        .save(pathWithRowIdx)

      val dfWithOnePartition = spark.read
        .format(dataSourceName)
        .schema(schemaWithRowIdx)
        .load(pathWithRowIdx)
      assert(dfWithOnePartition.filter(s"$rowIndexColName != id").count() == 0)
    }
  }

  private def readRowGroupRowCounts(path: String): Seq[Long] = {
    ParquetFileReader.readFooter(spark.sessionState.newHadoopConf(), new Path(path))
      .getBlocks.asScala.map(_.getRowCount)
  }

  private def readRowGroupRowCounts(dir: File): Seq[Seq[Long]] = {
    assert(dir.isDirectory)
    dir.listFiles()
      .filter { f => f.isFile && f.getName.endsWith("parquet") }
      .map { f => readRowGroupRowCounts(f.getAbsolutePath) }
  }

  /**
   * Do the files contain exactly one row group?
   */
  private def assertOneRowGroup(dir: File): Unit = {
    readRowGroupRowCounts(dir).foreach { rcs =>
      assert(rcs.length == 1, "expected one row group per file")
    }
  }

  /**
   * Do the files have a good layout to test row group skipping (both range metadata filter, and
   * by using min/max).
   */
  private def assertTinyRowGroups(dir: File): Unit = {
    readRowGroupRowCounts(dir).foreach { rcs =>
      assert(rcs.length > 1, "expected multiple row groups per file")
      assert(rcs.last <= DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK)
      assert(rcs.reverse.tail.distinct == Seq(DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK),
        "expected row groups with minimal row count")
    }
  }

  /**
   * Do the files have a good layout to test a combination of page skipping and row group skipping?
   */
  private def assertIntermediateRowGroups(dir: File): Unit = {
    readRowGroupRowCounts(dir).foreach { rcs =>
      assert(rcs.length >= 3, "expected at least 3 row groups per file")
      rcs.reverse.tail.foreach { rc =>
        assert(rc > DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK,
          "expected row groups larger than minimal row count")
      }
    }
  }

  case class RowIndexTestConf(
      numRows: Long = 10000L,
      useMultipleFiles: Boolean = false,
      useVectorizedReader: Boolean = true,
      // Small pages allow us to test row indexes when skipping individual pages using column
      // indexes.
      useSmallPages: Boolean = false,
      // If small row groups are used, each file will contain multiple row groups.
      // Otherwise, each file will contain only one row group.
      useSmallRowGroups: Boolean = false,
      useSmallSplits: Boolean = true,
      useFilter: Boolean = false,
      useDataSourceV2: Boolean = false) {

    val NUM_MULTIPLE_FILES = 4
    // The test doesn't work correctly if the number of records per file is uneven.
    assert(!useMultipleFiles || (numRows % NUM_MULTIPLE_FILES == 0))

    def numFiles: Int = if (useMultipleFiles) { NUM_MULTIPLE_FILES } else { 1 }

    def rowGroupSize: Long = if (useSmallRowGroups) {
      // Each file will contain multiple row groups. All of them (except for the last one)
      // will contain exactly DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK records.
      64L
    } else {
      // Each file will contain a single row group.
      DEFAULT_BLOCK_SIZE
    }

    def pageSize: Long = if (useSmallPages) {
      // Each page (except for the last one for each column) will contain exactly
      // DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK records.
      64L
    } else {
      DEFAULT_PAGE_SIZE
    }

    def writeFormat: String = "parquet"
    def readFormat: String = if (useDataSourceV2) {
      classOf[ParquetDataSourceV2].getCanonicalName
    } else {
      "parquet"
    }

    // assert(useSmallRowGroups || !useSmallSplits)
    def filesMaxPartitionBytes: Long = if (useSmallSplits) {
      256L
    } else {
      SQLConf.FILES_MAX_PARTITION_BYTES.defaultValue.get
    }

    def desc: String = {
      { if (useVectorizedReader) Seq("vectorized reader") else Seq("parquet-mr reader") } ++
      { if (useMultipleFiles) Seq("many files") else Seq.empty[String] } ++
      { if (useFilter) Seq("filtered") else Seq.empty[String] } ++
      { if (useSmallPages) Seq("small pages") else Seq.empty[String] } ++
      { if (useSmallRowGroups) Seq("small row groups") else Seq.empty[String] } ++
      { if (useSmallSplits) Seq("small splits") else Seq.empty[String] } ++
      { if (useDataSourceV2) Seq("datasource v2") else Seq.empty[String] }
    }.mkString(", ")

    def sqlConfs: Seq[(String, String)] = Seq(
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> useVectorizedReader.toString,
      SQLConf.FILES_MAX_PARTITION_BYTES.key -> filesMaxPartitionBytes.toString
    ) ++ { if (useDataSourceV2) Seq(SQLConf.USE_V1_SOURCE_LIST.key -> "") else Seq.empty }
  }

  def genRowIdxTestConfs: Seq[RowIndexTestConf] = {
    def genBooleanArrays(length: Int): Seq[Array[Boolean]] = {
      if (length == 1) {
        Seq(Seq(true).toArray, Seq(false).toArray)
      } else {
        val shorter = genBooleanArrays(length - 1)
        shorter.map(false +: _) ++ shorter.map(true +: _)
      }
    }
    val combinations = genBooleanArrays(6)
    combinations.map { c =>
      RowIndexTestConf(
        useVectorizedReader = c(0),
        useDataSourceV2 = c(1),
        useMultipleFiles = c(2),
        useSmallRowGroups = c(3),
        useFilter = c(4),
        useSmallSplits = c(5)
      )
    }
  }

  for (conf <- genRowIdxTestConfs)
  test (s"row index generation - ${conf.desc}") {
    withSQLConf(conf.sqlConfs: _*) {
      withTempPath { path =>
        val rowIndexColName = RowIndexGenerator.ROW_INDEX_COLUMN_NAME
        val numRecordsPerFile = conf.numRows / conf.numFiles
        val (skipCentileFirst, skipCentileMidLeft, skipCentileMidRight, skipCentileLast) =
          (0.2, 0.4, 0.6, 0.8)
        val expectedRowIdxCol = "expected_rowIdx_col"
        val df = spark.range(0, conf.numRows, 1, conf.numFiles).toDF("id")
          .withColumn("dummy_col", ($"id" / 55).cast("int"))
          .withColumn(expectedRowIdxCol, ($"id" % numRecordsPerFile).cast("int"))

        // With row index in schema.
        val schemaWithRowIdx = df.schema.add(rowIndexColName, LongType, nullable = true)

        df.write
          .format(conf.writeFormat)
          .option(ParquetOutputFormat.BLOCK_SIZE, conf.rowGroupSize)
          .option(ParquetOutputFormat.PAGE_SIZE, conf.pageSize)
          .option(ParquetOutputFormat.DICTIONARY_PAGE_SIZE, conf.pageSize)
          .save(path.getAbsolutePath)
        val dfRead = spark.read
          .format(conf.readFormat)
          .schema(schemaWithRowIdx)
          .load(path.getAbsolutePath)

        val dfToAssert = if (conf.useFilter) {
          // Add a filter such that we skip 60% of the records:
          // [0%, 20%], [40%, 60%], [80%, 100%]
          dfRead.filter((
            $"id" >= (skipCentileFirst * conf.numRows) &&
              $"id" < (skipCentileMidLeft * conf.numRows)) || (
            $"id" >= (skipCentileMidRight * conf.numRows) &&
              $"id" < (skipCentileLast * conf.numRows)))
        } else {
          dfRead
        }

        var numPartitions: Long = 0
        var numOutputRows: Long = 0
        dfToAssert.collect()
        dfToAssert.queryExecution.executedPlan.foreach {
          case f: FileSourceScanExec =>
            numPartitions += f.inputRDD.partitions.length
            numOutputRows += f.metrics("numOutputRows").value
          case _ =>
        }

        if (!conf.useDataSourceV2 && conf.useSmallSplits) {
          assert(numPartitions >= 2 * conf.numFiles)
        }

        // Assert that every rowIdx value matches the value in `expectedRowIdx`.
        assert(dfToAssert.filter(s"$rowIndexColName != $expectedRowIdxCol")
          .count() == 0)

        if (conf.useFilter) {
          if (conf.useSmallRowGroups) {
            assert(numOutputRows < conf.numRows)
          }

          val minMaxRowIndexes = dfToAssert.select(
            max(col(rowIndexColName)),
            min(col(rowIndexColName))).collect()
          val (expectedMaxRowIdx, expectedMinRowIdx) = if (conf.numFiles == 1) {
            // When there is a single file, we still have row group skipping,
            // but that should not affect the produced rowIdx.
            (conf.numRows * skipCentileLast - 1, conf.numRows * skipCentileFirst)
          } else {
            // For simplicity, the chosen filter skips the whole files.
            // Thus all unskipped files will have the same max and min rowIdx values.
            (numRecordsPerFile - 1, 0)
          }
          assert(minMaxRowIndexes(0).get(0) == expectedMaxRowIdx)
          assert(minMaxRowIndexes(0).get(1) == expectedMinRowIdx)
          if (!conf.useMultipleFiles) {
            val skippedValues = List.range(0, (skipCentileFirst * conf.numRows).toInt) ++
              List.range((skipCentileMidLeft * conf.numRows).toInt,
                (skipCentileMidRight * conf.numRows).toInt) ++
              List.range((skipCentileLast * conf.numRows).toInt, conf.numRows)
            // rowIdx column should not have any of the `skippedValues`.
            assert(dfToAssert
              .filter(col(rowIndexColName).isin(skippedValues: _*)).count() == 0)
          }
        } else {
          // When there is no filter, the rowIdx values should be in range [0-`numRecordsPerFile`].
          val expectedRowIdxValues = List.range(0, numRecordsPerFile)
          assert(dfToAssert.filter(col(rowIndexColName).isin(expectedRowIdxValues: _*))
            .count() == conf.numRows)
        }
      }
    }
  }

  // TODO: Wrong type.
  // TODO: Wrong source.
}
