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

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetDataSourceV2
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class RowIndexGeneratorSuite extends QueryTest with SharedSparkSession {

  // TODO:
  // - check row wise
  // - check unsafe projection
  // - check streaming
  // - check DSV2
  // - check with multiple cols, and partition-by stuffs

  // true/true
  // true/false
  // [info]   org.apache.spark.sql.AnalysisException: [UNRESOLVED_COLUMN] A column or function
  // parameter with name `_metadata`.`row_index` cannot be resolved. Did you mean one of the
  // following? [`id`];
  //
  // false/false
  // [info]   org.apache.spark.SparkException: Job aborted due to stage failure:
  // Task 0 in stage 7.0 failed 1 times, most recent failure: Lost task 0.0 in stage 7.0 (TID 6)
  // (192.168.1.34 executor driver): java.lang.ClassCastException: java.lang.Integer cannot be cast
  // to java.lang.Long
  // [info]  at org.apache.spark.sql.execution.datasources.FileScanRDD$$anon$1
  // .addMetadataColumnsIfNeeded(FileScanRDD.scala:197)

  for (useDataSourceV2 <- Seq(true, false))
  for (useVectorizedReader <- Seq(true, false)) {
    val label = s"parquet $useDataSourceV2 $useVectorizedReader"
    val conf = Seq(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> useVectorizedReader.toString) ++
      { if (useDataSourceV2) Seq(SQLConf.USE_V1_SOURCE_LIST.key -> "") else Seq.empty }
    val format = if (useDataSourceV2) {
      classOf[ParquetDataSourceV2].getCanonicalName
    } else {
      "parquet"
    }

    test(label) {
      withSQLConf(conf: _*) {
        withReadDataFrame(format) { df =>
          val res = df.select("*", s"${FileFormat.METADATA_NAME}.${FileFormat.ROW_INDEX}")
          assert(res.where(s"id != ${FileFormat.ROW_INDEX}").count == 0)
        }
      }
    }
  }

//  test("piggy back in columnar") {
//    withTempPath { path =>
//      val df = spark.range(0, 10, 1, 1).toDF("id")
//      val schemaWithRowIdx = df.schema.add(RowIndexGenerator.ROW_INDEX_COLUMN_NAME,
//        LongType)
//
//      df.write
//        .format("parquet")
//        .save(path.getAbsolutePath)
//
//      val dfRead = spark.read
//        .format("parquet")
//        .schema(schemaWithRowIdx)
//        .load(path.getAbsolutePath)
//        .select("*", METADATA_FILE_PATH, METADATA_ROW_INDEX)
//
//      assert(dfRead.where("id != row_index").count() == 0)
//      dfRead.show(200)
//    }
//  }
//
//  test("no piggy no problem") {
//    withTempPath { path =>
//      val df = spark.range(0, 10, 1, 1).toDF("id")
//
//      df.write
//        .format("parquet")
//        .save(path.getAbsolutePath)
//
//      val dfRead = spark.read
//        .format("parquet")
//        .load(path.getAbsolutePath)
//        .select("*", METADATA_ROW_INDEX)
//
//      assert(dfRead.where("id != row_index").count() == 0)
//      dfRead.show(200)
//    }
//  }

  val supportedFileFormats = Seq("parquet") // Add: V2

  def withReadDataFrame(format: String)(f: DataFrame => Unit): Unit =
    withReadDataFrame(format, format)(f)

  def withReadDataFrame(writeFormat: String,
                        readFormat: String)(f: DataFrame => Unit): Unit = {
    withTempPath { path =>
      val writeDf = spark.range(0, 10, 1, 1).toDF("id")
      writeDf.write.format(writeFormat).save(path.getAbsolutePath)
      val readDf = spark.read.format(readFormat).schema(writeDf.schema).load(path.getAbsolutePath)
      f(readDf)
    }
  }

  private val allMetadataCols = Seq(
    FileFormat.FILE_PATH,
    FileFormat.FILE_SIZE,
    FileFormat.FILE_MODIFICATION_TIME,
    FileFormat.ROW_INDEX
  )

  /** Identifies the names of all the metadata columns present in the schema. */
  private def collectMetadataCols(struct: StructType): Seq[String] = {
    struct.fields.flatMap { field => field.dataType match {
      case s: StructType => collectMetadataCols(s)
      case _ if allMetadataCols.contains(field.name) => Some(field.name)
      case _ => None
    }}
  }

  test("supported file format - read _metadata struct") {
    withReadDataFrame("parquet") { df =>
      val withMetadataStruct = df.select("*", FileFormat.METADATA_NAME)

      // `_metadata.row_index` column is present when selecting `_metadata` as a whole.
      val metadataCols = collectMetadataCols(withMetadataStruct.schema)
      assert(metadataCols.contains(FileFormat.ROW_INDEX))
    }
  }

  test("unsupported file format - read _metadata struct") {
    withReadDataFrame("orc") { df =>
      val withMetadataStruct = df.select("*", FileFormat.METADATA_NAME)

      // Metadata struct can be read without an error.
      withMetadataStruct.collect()

      // Schema does not contain row index column, but contains all the remaining metadata columns.
      val metadataCols = collectMetadataCols(withMetadataStruct.schema)
      assert(!metadataCols.contains(FileFormat.ROW_INDEX))
      assert(allMetadataCols.intersect(metadataCols).size == allMetadataCols.size - 1)
    }
  }

  test("unsupported file format - read _metadata.row_index") {
    withReadDataFrame("orc") { df =>
      val ex = intercept[AnalysisException] {
        df.select("*", s"${FileFormat.METADATA_NAME}.${FileFormat.ROW_INDEX}")
      }
      assert(ex.getMessage.contains("No such struct field row_index"))
    }
  }
}
