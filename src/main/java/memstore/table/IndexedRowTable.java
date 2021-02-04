package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.List;

/**
 * IndexedRowTable, which stores data in row-major format.
 * That is, data is laid out like
 *   row 1 | row 2 | ... | row n.
 *
 * Also has a tree index on column `indexColumn`, which points
 * to all row indices with the given value.
 */
public class IndexedRowTable implements Table {

    int numCols;
    int numRows;
    private TreeMap<Integer, IntArrayList> index;
    private ByteBuffer rows;
    private int indexColumn;

    public IndexedRowTable(int indexColumn) {
        this.indexColumn = indexColumn;
    }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    @Override
    public void load(DataLoader loader) throws IOException {
		this.index = new TreeMap<Integer, IntArrayList>();
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        this.numRows = rows.size();
        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
				int val = curRow.getInt(ByteFormat.FIELD_LEN * colId);
                this.rows.putInt(offset, val);
				
				if (colId == indexColumn) {
					if (!index.containsKey(val)) {
						IntArrayList empty = new IntArrayList();
						index.put(val, empty);
					}
					
					index.get(val).add(rowId);
				}
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
		int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
		if (colId == indexColumn) {
			int currVal = rows.getInt(offset);
			IntArrayList rowIndices = index.get(currVal);
			rowIndices.rem(rowId);
			if (rowIndices.size() == 0) {
				index.remove(currVal);
			}
		}
        
		rows.putInt(offset, field);
		
		if (!index.containsKey(field)) {
			IntArrayList empty = new IntArrayList();
			index.put(field, empty);
		}
		
		index.get(field).add(rowId);
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
			if (rows.getInt(offset+4) > threshold1 && rows.getInt(offset+8) < threshold2) {
				result += rows.getInt(offset);
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
		/*
		if (indexColumn == 0) {
			Collection<IntArrayList> relIndices = index.tailMap(threshold+1).values();
			
			for (IntArrayList rowIndices : relIndices) {
				for (int rowId : rowIndices) {
					int offset = ByteFormat.FIELD_LEN * numCols * rowId;
					for (int colId = 0; colId < numCols; colId++) {
						result += rows.getInt(offset + ByteFormat.FIELD_LEN * colId);
					}
				}
			}
			
		} else {*/
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
		//}
		
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
		
		if (indexColumn == 0) {
			Collection<IntArrayList> relIndices = index.headMap(threshold).values();
			
			for (IntArrayList rowIndices : relIndices) {
				for (int rowId : rowIndices) {
					int offset = ByteFormat.FIELD_LEN * rowId * numCols;
					int col2Val = rows.getInt(offset + ByteFormat.FIELD_LEN*2);
					int col3Val = rows.getInt(offset + ByteFormat.FIELD_LEN*3);
					rows.putInt(offset + ByteFormat.FIELD_LEN*3, col3Val + col2Val);
						
					result++;
				}
			}
			
		} else {
			for (int rowId = 0; rowId < numRows; rowId++) {
				int offset = ByteFormat.FIELD_LEN * rowId * numCols;
				int col0Val = rows.getInt(offset);
				
				if (col0Val < threshold) {
					int col2Val = rows.getInt(offset + ByteFormat.FIELD_LEN*2);
					int col3Val = rows.getInt(offset + ByteFormat.FIELD_LEN*3);
					
					int newVal = col2Val + col3Val;
					rows.putInt(offset + ByteFormat.FIELD_LEN*3, newVal);
					
					result++;
					
					if (indexColumn == 3) {
						IntArrayList rowIndices = index.get(col3Val);
						rowIndices.rem(rowId);
						if (rowIndices.size() == 0) {
							index.remove(col3Val);
						}
						
						if (!index.containsKey(newVal)) {
							IntArrayList empty = new IntArrayList();
							index.put(newVal, empty);
						}
						
						index.get(newVal).add(rowId);
					}
				}
			}
		}
		
		return result;
	}
}
