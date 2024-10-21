package com.github.cluelessskywatcher.chrysocyon.metadata;

import com.github.cluelessskywatcher.chrysocyon.index.HashedIndex;
import com.github.cluelessskywatcher.chrysocyon.index.TableIndex;
import com.github.cluelessskywatcher.chrysocyon.transactions.ChrysoTransaction;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.DataInfo;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.IntegerInfo;
import lombok.Getter;

@SuppressWarnings("unused")
public class IndexInfo {
    private @Getter String indexName, fieldName;
    private @Getter ChrysoTransaction tx;
    private @Getter TupleSchema schema;
    private @Getter TupleLayout layout;
    private @Getter StatisticalInfo stats;

    public IndexInfo(String indexName, String fieldName, ChrysoTransaction tx, TupleSchema schema, TupleLayout layout,
            StatisticalInfo stats) {
        this.indexName = indexName;
        this.fieldName = fieldName;
        this.tx = tx;
        this.schema = schema;
        this.layout = layout;
        this.stats = stats;
    }

    public TableIndex open() {
        TupleSchema schema = getSchema();
        return new HashedIndex(tx, indexName, layout);
    }

    public int getBlocksAccessed() {
        int recordsPerBlock = tx.getBlockSize() / layout.getSlotSize();
        int blockCount = stats.getNumRecords() / stats.getDistinctValues(fieldName);
        return HashedIndex.searchCost(blockCount, recordsPerBlock);
    }

    public int getRecordCount() {
        return stats.getNumRecords() / stats.getDistinctValues(this.fieldName);
    }

    public int getDistinctValues(String fieldName) {
        return this.fieldName.equals(fieldName) ? 1 : stats.getDistinctValues(this.fieldName);
    }

    public TupleLayout createIndexLayout() {
        TupleSchema indexSchema = new TupleSchema();
        indexSchema.addField(new IntegerInfo(), "block");
        indexSchema.addField(new IntegerInfo(), "id");

        DataInfo dataVal = schema.getField(fieldName);
        indexSchema.addField(dataVal, fieldName);
        return new TupleLayout(indexSchema);
    }
}
