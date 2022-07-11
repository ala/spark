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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

class RowIndexGeneratorSuite extends QueryTest with SharedSparkSession {
//  import testImplicits._

  private val METADATA_FILE_PATH = "_metadata.file_path"

//  private val METADATA_FILE_NAME = "_metadata.file_name"
//
//  private val METADATA_FILE_SIZE = "_metadata.file_size"
//
//  private val METADATA_FILE_MODIFICATION_TIME = "_metadata.file_modification_time"

  private val METADATA_ROW_INDEX = "_metadata.row_index"

  test("piggy back in columnar") {
    withTempPath { path =>
      val df = spark.range(0, 10, 1, 1).toDF("id")
      val schemaWithRowIdx = df.schema.add(RowIndexGenerator.ROW_INDEX_COLUMN_NAME,
        LongType)

      df.write
        .format("parquet")
        .save(path.getAbsolutePath)

      val dfRead = spark.read
        .format("parquet")
        .schema(schemaWithRowIdx)
        .load(path.getAbsolutePath)
        .select("*", METADATA_FILE_PATH, METADATA_ROW_INDEX)

      assert(dfRead.where("id != row_index").count() == 0)
      dfRead.show(200)
    }
  }

  test("no piggy no problem") {
    withTempPath { path =>
      val df = spark.range(0, 10, 1, 1).toDF("id")

      df.write
        .format("parquet")
        .save(path.getAbsolutePath)

      val dfRead = spark.read
        .format("parquet")
        .load(path.getAbsolutePath)
        .select("*", METADATA_ROW_INDEX)

      assert(dfRead.where("id != row_index").count() == 0)
      dfRead.show(200)
    }
  }

  test("no support == nulls") {
    withTempPath { path =>
      val df = spark.range(0, 10, 1, 1).toDF("id")

      df.write
        .format("parquet")
        .save(path.getAbsolutePath)

      val dfRead = spark.read
        .format("parquet")
        .load(path.getAbsolutePath)
        .select("*", METADATA_ROW_INDEX)

      assert(dfRead.where("id != row_index").count() == 0)
      dfRead.show(200)
    }
  }
}
