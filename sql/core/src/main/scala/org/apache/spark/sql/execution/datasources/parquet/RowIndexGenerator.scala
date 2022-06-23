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

import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.parquet.hadoop.ParquetRecordReader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

/**
 * Generate row index across batches for a file.
 */
class RowIndexGenerator(rowIndexColumnIdx: Int) {
  var currentBatchStartIndex: Long = 0L

  def setCurrentBatchStartIndex(startIndex: Long): Unit = {
    currentBatchStartIndex = startIndex
  }

  def populateRowIndex(columnVectors: Array[ParquetColumnVector], numRows: Int): Unit = {
    populateRowIndex(columnVectors(rowIndexColumnIdx).getValueVector, numRows)
  }

  def populateRowIndex(columnVector: WritableColumnVector, numRows: Int): Unit = {
    assert(!(columnVector.isAllNull))
    for (i <- (0 to numRows)) {
      columnVector.putLong(i, currentBatchStartIndex + i)
    }
    currentBatchStartIndex += numRows
  }
}

object RowIndexGenerator {
  val ROW_INDEX_COLUMN_NAME = "_computed_column_row_index"

  class RecordReaderWithRowIndexes(parent: ParquetRecordReader[InternalRow], rowIndexColumnIdx: Int)
    extends RecordReader[Void, InternalRow] {

    override def initialize(
        inputSplit: InputSplit,
        taskAttemptContext: TaskAttemptContext): Unit = {
      parent.initialize(inputSplit, taskAttemptContext)
    }

    override def nextKeyValue(): Boolean = parent.nextKeyValue()

    override def getCurrentKey: Void = parent.getCurrentKey

    override def getCurrentValue: InternalRow = {
      val row = parent.getCurrentValue
      row.setLong(rowIndexColumnIdx, parent.getCurrentRowIndex)
      row
    }

    override def getProgress: Float = parent.getProgress

    override def close(): Unit = parent.close()
  }

  def addRowIndexToRecordReader(
      reader: ParquetRecordReader[InternalRow],
      sparkSchema: StructType): RecordReader[Void, InternalRow] = {
    val rowIndexColumnIdx = findColumnIndexInSchema(sparkSchema)
    if (rowIndexColumnIdx >= 0) {
      new RecordReaderWithRowIndexes(reader, rowIndexColumnIdx)
    } else {
      reader
    }
  }

  def findColumnIndexInSchema(sparkSchema: StructType): Int = {
    sparkSchema.fields.zipWithIndex.find { case (field: StructField, _: Int) =>
      field.name == ROW_INDEX_COLUMN_NAME
    }.map(_._2).getOrElse(-1)
  }

  def isRowIndexColumn(column: ParquetColumn): Boolean = {
    column.path.length == 1 && column.path.last == ROW_INDEX_COLUMN_NAME
  }

  def isNeededForSchema(sparkSchema: StructType): Boolean = {
    findColumnIndexInSchema(sparkSchema) >= 0
  }

  // TODO: Check data type
  // TODO: Check there's not more than one there
  // TODO: Maybe different name?
  def createIfNeededForSchema(sparkSchema: StructType): RowIndexGenerator = {
    val columnIdx = findColumnIndexInSchema(sparkSchema)
    if (columnIdx >= 0) new RowIndexGenerator(columnIdx)
    else null
  }

}
