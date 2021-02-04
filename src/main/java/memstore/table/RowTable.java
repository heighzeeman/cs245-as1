package memstore.table;

import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * RowTable, which stores data in row-major format.
 * That is, data is laid out like
 *   row 1 | row 2 | ... | row n.
 */
public class RowTable implements Table {
    protected int numCols;
    protected int numRows;
    protected ByteBuffer rows;

    public RowTable() { }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    @Override
    public void load(DataLoader loader) throws IOException {
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();
        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
                this.rows.putInt(offset, curRow.getInt(ByteFormat.FIELD_LEN * colId));
            }
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        return rows.getInt(ByteFormat.FIELD_LEN * ((rowId * numCols) + colId));
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
		rows.putInt(ByteFormat.FIELD_LEN * ((rowId * numCols) + colId), field);
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table;
     *
     *  Returns the sum of all elements in the first column of the table.
     */
    @Override
    public long columnSum() {
        long result = 0;
		int mult = ByteFormat.FIELD_LEN * numCols;
        for (int rowId = 0; rowId < numRows; rowId++) {
			result += rows.getInt(mult * rowId);
		}
        return result;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
     *
     *  Returns the sum of all elements in the first column of the table,
     *  subject to the passed-in predicates.
     */
    @Override
    public long predicatedColumnSum(int threshold1, int threshold2) {
        long result = 0;
		int mult = ByteFormat.FIELD_LEN * numCols;
        for (int rowId = 0; rowId < numRows; rowId++) {
			int offset = mult * rowId;
			if (rows.getInt(offset + ByteFormat.FIELD_LEN) > threshold1 && rows.getInt(offset + ByteFormat.FIELD_LEN*2) < threshold2 ) {
				result += rows.getInt(mult * rowId);
			}
		}
        return result;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
     *
     *  Returns the sum of all elements in the rows which pass the predicate.
     */
    @Override
    public long predicatedAllColumnsSum(int threshold) {
        long result = 0;
		
		for (int rowId = 0; rowId < numRows; rowId++) {
			int offset = ByteFormat.FIELD_LEN * numCols * rowId;
			int col0Val = rows.getInt(offset);
			
			if (col0Val > threshold) {
				result += col0Val;
				for (int colId = 1; colId < numCols; colId++) {		
					result += rows.getInt(offset + ByteFormat.FIELD_LEN * colId);
				}
			}
		}
        return result;
    }

    /**
     * Implements the query
     *   UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
     *
     *   Returns the number of rows updated.
     */
    @Override
    public int predicatedUpdate(int threshold) {
        int result = 0;
		
		for (int rowId = 0; rowId < numRows; rowId++) {
			int offset = ByteFormat.FIELD_LEN * rowId * numCols;
			int col0Val = rows.getInt(offset);
			
			if (col0Val < threshold) {
				int col2Val = rows.getInt(offset + ByteFormat.FIELD_LEN*2);
				int col3Val = rows.getInt(offset + ByteFormat.FIELD_LEN*3);
				rows.putInt(offset + ByteFormat.FIELD_LEN*3, col3Val + col2Val);
				
				result++;
			}
		}
        return result;
    }
}
