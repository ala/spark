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
package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class FileMetadataStructRowIndexSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  val expected_row_idx_col = "expected_row_idx"

  def withReadDataFrame
      (format: String, partitioned: Boolean = false)
      (f: (DataFrame, DataFrame) => Unit): Unit = {
    withTempPath { path =>
      val writeDf = if (partitioned) {
        val df = spark.range(0, 100, 1, 1).toDF("id")
          .select(
            ($"id" % 10) as expected_row_idx_col,
            lit("a text").as("text"),
            ($"id" / 10).cast("int").cast("string").as("pb"))
        df.write.format(format).partitionBy("pb").save(path.getAbsolutePath)
        df
      } else {
        val df = spark.range(0, 10, 1, 1).toDF(expected_row_idx_col)
        df.write.format(format).save(path.getAbsolutePath)
        df
      }
      val schema = writeDf.schema
      val readDf = spark.read.format(format).schema(schema).load(path.getAbsolutePath)
      f(writeDf, readDf)
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

  for (useVectorizedReader <- Seq(false, true))
  for (useOffHeapMemory <- Seq(useVectorizedReader, false).distinct)
  for (partitioned <- Seq(false, true)) {
    val label = Seq(
        { if (useVectorizedReader) "vectorized" else "parquet-mr"},
        { if (useOffHeapMemory) "off-heap" else "" },
        { if (partitioned) "partitioned" else "" }
      ).filter(_.nonEmpty).mkString(", ")
    test(s"parquet ($label) - read _metadata.row_index") {
      withSQLConf(
          SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> useVectorizedReader.toString,
          SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED.key -> useOffHeapMemory.toString,
          "spark.sql.codegen.wholeStage" -> "false") {
        withReadDataFrame("parquet", partitioned) { case (writeDf: DataFrame, df: DataFrame) =>
          println("--------------------- READ --------------------")
          val res = df.select("*", s"${FileFormat.METADATA_NAME}.${FileFormat.ROW_INDEX}")
//          println(res.where(s"$expected_row_id_col != ${FileFormat.ROW_INDEX}")
//            .queryExecution.executedPlan)
          val res2 = res.where(s"$expected_row_idx_col != ${FileFormat.ROW_INDEX}")
          println(s"PLAN === ${res2.queryExecution.executedPlan}")

          checkAnswer(df.select("*", s"${FileFormat.METADATA_NAME}.${FileFormat.ROW_INDEX}"),
            writeDf.withColumn(FileFormat.ROW_INDEX, $"$expected_row_idx_col"))

          // assert(res2.collect().length == 0)


//          assert(res.where(s"$expected_row_id_col == ${FileFormat.ROW_INDEX}").count
//            == df.count())
        }
      }
    }
  }

  test("supported file format - read _metadata struct") {
    withReadDataFrame("parquet") { case (_: DataFrame, df: DataFrame) =>
      val withMetadataStruct = df.select("*", FileFormat.METADATA_NAME)

      // `_metadata.row_index` column is present when selecting `_metadata` as a whole.
      val metadataCols = collectMetadataCols(withMetadataStruct.schema)
      assert(metadataCols.contains(FileFormat.ROW_INDEX))
    }
  }

  test("unsupported file format - read _metadata struct") {
    withReadDataFrame("orc") { case (_: DataFrame, df: DataFrame) =>
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
    withReadDataFrame("orc") { case (_: DataFrame, df: DataFrame) =>
      val ex = intercept[AnalysisException] {
        df.select("*", s"${FileFormat.METADATA_NAME}.${FileFormat.ROW_INDEX}")
      }
      assert(ex.getMessage.contains("No such struct field row_index"))
    }
  }
}
