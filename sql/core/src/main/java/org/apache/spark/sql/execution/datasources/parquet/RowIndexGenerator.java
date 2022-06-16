package org.apache.spark.sql.execution.datasources.parquet;

import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Generate row index across batches for a file.
 */
public class RowIndexGenerator {
    private int rowIndexColumnIdx;
    private long currentBatchStartIndex;
    public static String ROW_INDEX_COLUMN_NAME = "_computed_column_row_index";

    RowIndexGenerator(int rowIndexColumnIdx) {
        this.rowIndexColumnIdx = rowIndexColumnIdx;
        this.currentBatchStartIndex = 0;
    }

    public void setCurrentBatchStartIndex(long currentBatchStartIndex) {
        this.currentBatchStartIndex = currentBatchStartIndex;
    }

    public void populateRowIndex(ParquetColumnVector[] columnVectors, int numRows) {
        populateRowIndex(columnVectors[this.rowIndexColumnIdx].getValueVector(), numRows);
    }

    public void populateRowIndex(WritableColumnVector columnVector, int numRows) {
        assert(!columnVector.isAllNull());
        for (int i = 0; i < numRows; i++) {
            columnVector.putLong(i, this.currentBatchStartIndex + i);
        }
        this.currentBatchStartIndex += numRows;
    }

    public static boolean isRowIndexColumn(ParquetColumn column) {
        return column.path().length() == 1 && column.path().last().equals(ROW_INDEX_COLUMN_NAME);
    }

    // TODO: Needs to account for duplicates or not?
    private static int findColumnIndexInSchema(StructType sparkSchema) {
        for (int i = 0; i < sparkSchema.fields().length; i++) {
            StructField f = sparkSchema.fields()[i];
            if (sparkSchema.fields()[i].name().equals(ROW_INDEX_COLUMN_NAME)) {
                return i;
            }
        }
        return -1;
    }

    public static boolean isNeededForSchema(StructType sparkSchema) {
        return findColumnIndexInSchema(sparkSchema) >= 0;
    }

    // TODO: Check data type
    // TODO: Check there's not more than one there
    // TODO: Maybe different name?
    public static RowIndexGenerator createIfNeededForSchema(StructType sparkSchema) {
        int columnIdx = findColumnIndexInSchema(sparkSchema);
        if (columnIdx >= 0) return new RowIndexGenerator(columnIdx);
        else return null;
    }
}
