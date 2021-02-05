package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;

//import java.io.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.TreeMap;
import java.util.List;
import java.util.Map;


/**
 * Custom table implementation to adapt to provided query mix.
 */
public class CustomTable implements Table {

	int numCols;
    int numRows;
	private TreeMap<Integer, IntOpenHashSet> col0_index;
	private TreeMap<Integer, IntOpenHashSet> col1_index;
	//private TreeMap<Integer, IntOpenHashSet> col2_index;
    private ByteBuffer rows;
	private IntArrayList col0to2;
	private Int2LongOpenHashMap rowSumMap;


    public CustomTable() {
		this.col0_index = new TreeMap<Integer, IntOpenHashSet>();
		this.col1_index = new TreeMap<Integer, IntOpenHashSet>();
		//this.col2_index = new TreeMap<Integer, IntOpenHashSet>();
		this.rowSumMap = new Int2LongOpenHashMap();
	}

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
		this.numRows = rows.size();
		
        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);
		this.col0to2 = new IntArrayList(numRows * 3);
		for (int i = 0; i < numRows * 3; i++) {
			this.col0to2.add(0);
		}


        for (int rowId = 0; rowId < numRows; rowId++) {
			long currRowSum = 0;
            ByteBuffer curRow = rows.get(rowId);
			
            for (int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
				int val = curRow.getInt(ByteFormat.FIELD_LEN * colId);
				
				currRowSum += val;
                this.rows.putInt(offset, val);
				
				if (colId == 0) {
					this.col0to2.set(rowId, val);
					
					if (!col0_index.containsKey(val)) {
						col0_index.put(val, new IntOpenHashSet());
					}
					col0_index.get(val).add(rowId);
					
				} else if (colId == 1) {
					this.col0to2.set(numRows + rowId, val);
					if (!col1_index.containsKey(val)) {
						col1_index.put(val, new IntOpenHashSet());
					}
					col1_index.get(val).add(rowId);
					
				} else if (colId == 2) {
					this.col0to2.set(numRows*2 + rowId, val);
					/*if (!col2_index.containsKey(val)) {
						col2_index.put(val, new IntOpenHashSet());
					}
					col2_index.get(val).add(rowId);*/
				}
            }
			
			rowSumMap.put(rowId, currRowSum);
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
		if (colId <= 2) {
			return col0to2.getInt((colId * numRows) + rowId);
		}
		
        return rows.getInt(ByteFormat.FIELD_LEN * ((rowId * numCols) + colId));
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
		int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId);
        int currVal = rows.getInt(offset);
		
		int diff = field - currVal;
		long currRowSum = rowSumMap.get(rowId);
		rowSumMap.replace(rowId, currRowSum + diff);
		
		if (colId == 0) {
			col0to2.set(rowId, field);
			IntOpenHashSet relIndices = col0_index.get(currVal);
			relIndices.rem(rowId);
			if (relIndices.size() == 0) {
				col0_index.remove(currVal);
			}
			
			if (!col0_index.containsKey(field)) {
				col0_index.put(field, new IntOpenHashSet());
			}
			col0_index.get(field).add(rowId);
					
		} else if (colId == 1) {
			col0to2.set(numRows + rowId, field);
			IntOpenHashSet relIndices = col1_index.get(currVal);
			relIndices.rem(rowId);
			if (relIndices.size() == 0) {
				col1_index.remove(currVal);
			}
			
			if (!col1_index.containsKey(field)) {
				col1_index.put(field, new IntOpenHashSet());
			}
			col1_index.get(field).add(rowId);
			
		} else if (colId == 2) {
			col0to2.set(2*numRows + rowId, field);
			/*IntOpenHashSet relIndices = col2_index.get(currVal);
			relIndices.rem(rowId);
			if (relIndices.size() == 0) {
				col2_index.remove(currVal);
			}
			
			if (!col2_index.containsKey(field)) {
				col2_index.put(field, new IntOpenHashSet());
			}
			col2_index.get(field).add(rowId);*/
		}
		
		rows.putInt(offset, field);
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table;
     *
     *  Returns the sum of all elements in the first column of the table.
     */
    @Override
    public long columnSum() {
		long result = 0L;
		for (int rowId = 0; rowId < numRows; rowId++) {
			result += col0to2.getInt(rowId);
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
        long result = 0L;
		
		//Collection<IntOpenHashSet> rel1Indices = col1_index.tailMap(threshold1+1);
		//Collection<IntOpenHashSet> rel2Indices = col2_index.headMap(threshold2).values();
		if (threshold1 >= 952) {
			/*IntOpenHashSet col1_RowIdx = new IntOpenHashSet();
			for (Map.Entry<Integer, IntOpenHashSet> col1idxSet : col1_index.tailMap(threshold1+1).entrySet()) {
				col1_RowIdx.addAll(col1idxSet.getValue());
			}*/
			
			/*if (threshold2 < 128) {
				IntOpenHashSet col2_RowIdx = new IntOpenHashSet();
				for (Map.Entry<Integer, IntOpenHashSet> col2idxSet : col2_index.headMap(threshold2).entrySet()) {
					col2_RowIdx.addAll(col2idxSet.getValue());
				}
				col1_RowIdx.retainAll(col2_RowIdx);
				
				for (int rowId : col1_RowIdx) {
					result += getIntField(rowId, 0);
				}
				return result;
			}*/
		
			for (Map.Entry<Integer, IntOpenHashSet> col1idxSet : col1_index.tailMap(threshold1+1).entrySet()) {
				for (int rowId : col1idxSet.getValue()) {
					if (getIntField(rowId, 2) < threshold2) {
						result += getIntField(rowId, 0);
					}
				}
			}
			
			return result;
		} /*else if (threshold2 < 192) {
			IntOpenHashSet col2_RowIdx = new IntOpenHashSet();
			for (Map.Entry<Integer, IntOpenHashSet> col2idxSet : col2_index.headMap(threshold2).entrySet()) {
				col2_RowIdx.addAll(col2idxSet.getValue());
			}
			
			if (threshold1 >= 952) {
				IntOpenHashSet col1_RowIdx = new IntOpenHashSet();
				for (Map.Entry<Integer, IntOpenHashSet> col1idxSet : col1_index.tailMap(threshold1+1).entrySet()) {
					col1_RowIdx.addAll(col1idxSet.getValue());
				}
				col1_RowIdx.retainAll(col2_RowIdx);
				
				for (int rowId : col1_RowIdx) {
					result += getIntField(rowId, 0);
				}
				return result;
			}
		
			for (int rowId : col2_RowIdx) {
				if (getIntField(rowId, 1) > threshold1) {
					result += getIntField(rowId, 0);
				}
			}
			
			return result;
		}*/
			
		
		for (int rowId = 0; rowId < numRows; rowId++) {
			if (getIntField(rowId, 1) > threshold1 && getIntField(rowId, 2) < threshold2) {
				result += getIntField(rowId, 0);
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
        long result = 0L;
		
		if (threshold > 816) {
			Collection<IntOpenHashSet> relIndices = col0_index.tailMap(threshold+1).values();
			
			for (IntOpenHashSet rowIndices : relIndices) {
				for (int rowId : rowIndices) {
					result += rowSumMap.get(rowId);
				}
			}
		} else {
			for (int rowId = 0; rowId < numRows; rowId++) {
				if (getIntField(rowId, 0) > threshold) {
					result += rowSumMap.get(rowId);
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
		
		if (threshold <= 242) {
			Collection<IntOpenHashSet> relIndices = col0_index.headMap(threshold).values();
			
			for (IntOpenHashSet rowIndices : relIndices) {
				for (int rowId : rowIndices) {
					int col2Val = getIntField(rowId, 2);
					int col3Val = getIntField(rowId, 3);
					long currRowSum = rowSumMap.get(rowId);
					rowSumMap.replace(rowId, currRowSum + col2Val);
					
					rows.putInt(ByteFormat.FIELD_LEN * ((rowId * numCols) + 3), col2Val + col3Val);
					
					result++;
				}
			}
			
			return result;
		}
		
		for (int rowId = 0; rowId < numRows; rowId++) {
			if (getIntField(rowId, 0) < threshold) {
				int col2Val = getIntField(rowId, 2);
				int col3Val = getIntField(rowId, 3);
				long currRowSum = rowSumMap.get(rowId);
				rowSumMap.replace(rowId, currRowSum + col2Val);
				rows.putInt(ByteFormat.FIELD_LEN * ((rowId * numCols) + 3), col2Val + col3Val);
					
				result++;
			}
		}
		
		return result;
    }

}
