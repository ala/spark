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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.{LongType, StructField, StructType}


object RowIndexUtil {
  def findColumnIndexInSchema(sparkSchema: StructType): Int = {
    sparkSchema.fields.zipWithIndex.find { case (field: StructField, _: Int) =>
      field.name == FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME
    } match {
      case Some((field: StructField, idx: Int)) =>
        if (field.dataType != LongType) {
          throw new RuntimeException(s"${FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME} must be of " +
            "LongType")
        }
        idx
      case _ => -1
    }
  }

  def isNeededForSchema(sparkSchema: StructType): Boolean = {
    findColumnIndexInSchema(sparkSchema) >= 0
  }

  /**
   * Helper class for updating row index value in the metadata row, that otherwise remains
   * constant for all the records read from the same file.
   */
  class MetadataRowUpdater(readRowColIdx: Int, metadataRowColIdx: Int) {
    def update(readRow: InternalRow, metadataRow: InternalRow): Unit = {
      metadataRow.update(metadataRowColIdx, readRow.getLong(readRowColIdx))
    }
  }

  def getMetadataRowUpdater(
      readRowSchema: StructType,
      metadataColumns: Seq[AttributeReference]): Option[MetadataRowUpdater] = {
    metadataColumns.zipWithIndex.find(_._1.name == FileFormat.ROW_INDEX)
      .map { case (_, metadataRowColIdx) =>
        val readRowColIdx = findColumnIndexInSchema(readRowSchema)
        assert(readRowColIdx >= 0)
        new MetadataRowUpdater(readRowColIdx, metadataRowColIdx)
      }
  }
}
