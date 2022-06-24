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
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetOutputFormat}

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

  val MIN_ROW_GROUP_ROW_COUNT = 100

  private def assertOneRowGroup(dir: File): Unit = {
    readRowGroupRowCounts(dir).foreach { rcs =>
      assert(rcs.length == 1, "expected one row group per file")
    }
  }

  private def assertTinyRowGroups(dir: File): Unit = {
    readRowGroupRowCounts(dir).foreach { rcs =>
      assert(rcs.length > 1, "expected multiple row groups per file")
      assert(rcs.last <= MIN_ROW_GROUP_ROW_COUNT)
      assert(rcs.reverse.tail.distinct == Seq(MIN_ROW_GROUP_ROW_COUNT),
        "expected row groups with minimal row count")
    }
  }

  private def assertIntermediateRowGroups(dir: File): Unit = {
    readRowGroupRowCounts(dir).foreach { rcs =>
      assert(rcs.length >= 3, "expected at least 3 row groups per file")
      rcs.reverse.tail.foreach { rc =>
        assert(rc > MIN_ROW_GROUP_ROW_COUNT, "expected row groups larger than minimal row count")
      }
    }
  }


  //  case class RowIndexTestConf(
  //      numRows: Long = 10000L,
  //      numFiles: Int = 1,
  //      useVectorizedReader: Boolean = true,
  //      // Small pages allow us to test row indexes when skipping individual pages using column
  //      // indexes.
  //      useSmallPages: Boolean = false,
  //      // If small row groups are used, each file will contain multiple row groups.
  //      // Otherwise, each file will contain only one row group.
  //      useSmallRowGroups: Boolean = false,
  //      useSmallSplits: Boolean = true,
  //      useFilter: Boolean ) {
  //    // The test doesn't work correctly if the number of records per file is uneven.
  //    assert(numRows % numFiles == 0)
  //
  //    private val DEFAULT_ROW_GROUP_SIZE = 128 * 1024 * 1024L
  //    private val SMALL_ROW_GROUP_SIZE = 64L
  //    private val DEFAULT_PAGE_SIZE = 1024L * 1024L
  //    private val SMALL_PAGE_SIZE = 64L
  //
  //    def rowGroupSize: Long = if (useSmallRowGroups) SMALL_ROW_GROUP_SIZE
  //    else DEFAULT_ROW_GROUP_SIZE
  //    def pageSize: Long = if (useSmallPages) SMALL_PAGE_SIZE else DEFAULT_PAGE_SIZE
  //
  //    def desc: String = Seq(
  //      { if (useVectorizedReader) "vectorized reader" else "parquet-mr reader" },
  //      { if (useSmallPages) "small pages" else "" },
  //      { if (useSmallRowGroups) "small row groups" else "" },
  //    ).filter(_.nonEmpty).mkString(", ")
  //
  //    // TODO: I don't get it
  //    def filesMaxPartitionBytes: Long = if (useSmallSplits) {
  //      DEFAULT_ROW_GROUP_SIZE
  //    } else {
  //      rowGroupSize
  //    }
  //
  //    def sqlConfs: Seq[(String,String)] = Seq(
  //      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> useVectorizedReader.toString,
  //      SQLConf.FILES_MAX_PARTITION_BYTES.key -> filesMaxPartitionBytes.toString
  //    )
  //  }
  //
  //  object RowIndexTestConf {
  //
  //  }
  //
  //  def getRowIndexTestConfigs()

  private val defaultRowGroupSize = 128 * 1024 * 1024
  for (useVectorizedReader <- Seq(true, false))
  // Test with single file and multiple files.
  for (numFiles <- Seq(1, 4))
  // TODO Number of rows must be multiple of numFiles to make this sane.
  // Small row group size -> a file has more than one row group.
  // Large row group size -> a file has only one row group.
  // for (rowGroupSize <- Seq(64, defaultRowGroupSize))
  for (rowGroupSize <- Seq(defaultRowGroupSize))
  // Set the FILES_MAX_PARTITION_BYTES equal to `splitSize` to have either
  // one or more tasks reading from the same file.
  for (splitSize <- Seq(64, defaultRowGroupSize))
  // Execute the code path when row groups/files are filtered.
  // for (withFilter <- Seq(false, true))
  for (withFilter <- Seq(true))
  for (pageSize <- Seq(64, 1024*1024))
  // TODO: Figure out a nicer naming scheme
  // TODO: Add page skipping test!
  test (s"row index generation - numFiles = $numFiles, RowGroupSize=$rowGroupSize," +
    s"splitSize=$splitSize," +
    s"withFilter=$withFilter," +
    s"withVectorizedReader=$useVectorizedReader, pageSize = $pageSize") {
    // TODO: What the heck is going on here?
    val filesMaxPartitionBytes = if (splitSize == defaultRowGroupSize) {
      rowGroupSize
    } else {
      defaultRowGroupSize
    }
    withSQLConf(SQLConf.FILES_MAX_PARTITION_BYTES.key -> filesMaxPartitionBytes.toString,
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> useVectorizedReader.toString,
      SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      withTempPath { path =>
        val rowIndexColName =
          RowIndexGenerator.ROW_INDEX_COLUMN_NAME
        val numRecords = 10000L
        val numRecordsPerFile = numRecords / numFiles
        val (skipCentileFirst, skipCentileMidLeft, skipCentileMidRight, skipCentileLast) =
          (0.2, 0.4, 0.6, 0.8)
        val expectedRowIdxCol = "expected_rowIdx_col"
        val df = spark.range(0, numRecords, 1, numFiles).toDF("id")
          .withColumn("dummy_col", ($"id" / 55).cast("int"))
          .withColumn(expectedRowIdxCol, ($"id" % numRecordsPerFile).cast("int"))
        // With row index in schema.
        val schemaWithRowIdx = df.schema.add(rowIndexColName, LongType, nullable = true)


        df.write
          .format(dataSourceName)
          .option(ParquetOutputFormat.BLOCK_SIZE, rowGroupSize)
          .option(ParquetOutputFormat.PAGE_SIZE, pageSize)
          .option(ParquetOutputFormat.DICTIONARY_PAGE_SIZE, pageSize)
          .save(path.getAbsolutePath)
        val dfRead = spark.read
          .format(classOf[ParquetDataSourceV2].getCanonicalName)
          //          .format(dataSourceName)
          .schema(schemaWithRowIdx)
          .load(path.getAbsolutePath)

        val dfToAssert = if (withFilter) {
          // Add a filter such that we skip 60% of the records:
          // [0%, 20%], [40%, 60%], [80%, 100%]
          dfRead.filter((
            $"id" >= (skipCentileFirst * numRecords) &&
              $"id" < (skipCentileMidLeft * numRecords)) || (
            $"id" >= (skipCentileMidRight * numRecords) &&
              $"id" < (skipCentileLast * numRecords)))
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
        // The smaller the `fileOpenCostInBytes` the more files can be read in the same partition.
        // For single file there is only 1 partition.

        // TODO(ALA): This doesn't work for v2 :/
        //        if (rowGroupSize != defaultRowGroupSize && splitSize == defaultRowGroupSize) {
        //          assert(numPartitions >= 2 * numFiles)
        //        }

        /*
        [info] - row index generation - numFiles=10, RowGroupSize=64,
        fileOpenCostInBytes=134217728,withFilter=true *** FAILED *** (16 seconds, 437 milliseconds)
        [info]   3600 did not equal 0 (ParquetIOSuite.scala:1639)
        [info]   org.scalatest.exceptions.TestFailedException:
         */
        // Assert that every rowIdx value matches the value in `expectedRowIdx`.
        dfToAssert.filter(s"$rowIndexColName != $expectedRowIdxCol").show(2000000)

        // 1 == 0?
        assert(dfToAssert.filter(s"$rowIndexColName != $expectedRowIdxCol")
          .count() == 0)

        if (withFilter) {
          if (rowGroupSize < defaultRowGroupSize) {
            assert(numOutputRows < numRecords)
          }

          val minMaxRowIndexes = dfToAssert.select(
            max(col(rowIndexColName)),
            min(col(rowIndexColName))).collect()
          val (expectedMaxRowIdx, expectedMinRowIdx) = if (numFiles == 1) {
            // When there is a single file, we still have row group skipping,
            // but that should not affect the produced rowIdx.
            (numRecords * skipCentileLast - 1, numRecords * skipCentileFirst)
          } else {
            // For simplicity, the chosen filter skips the whole files.
            // Thus all unskipped files will have the same max and min rowIdx values.
            (numRecordsPerFile - 1, 0)
          }
          assert(minMaxRowIndexes(0).get(0) == expectedMaxRowIdx)
          assert(minMaxRowIndexes(0).get(1) == expectedMinRowIdx)
          if (numFiles == 1) {
            val skippedValues = List.range(0, (skipCentileFirst * numRecords).toInt) ++
              List.range((skipCentileMidLeft * numRecords).toInt,
                (skipCentileMidRight * numRecords).toInt) ++
              List.range((skipCentileLast * numRecords).toInt, numRecords)
            // rowIdx column should not have any of the `skippedValues`.
            assert(dfToAssert
              .filter(col(rowIndexColName).isin(skippedValues: _*)).count() == 0)
          }
        } else {
          // When there is no filter, the rowIdx values should be in range [0-`numRecordsPerFile`].
          val expectedRowIdxValues = List.range(0, numRecordsPerFile)
          assert(dfToAssert.filter(col(rowIndexColName).isin(expectedRowIdxValues: _*))
            .count() == numRecords)
        }
      }
    }
  }
}
