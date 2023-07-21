package org.ds.dsv2;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;

import java.io.IOException;

public class KVPartitionReader implements PartitionReader<InternalRow> {

    private final KVPartition kVPartition;
    private final Lmdb lmdb;
    private InternalRow current;
    private CloseableKVIterator iterator;

    public KVPartitionReader(KVPartition KVPartition, Lmdb lmdb) {
        this.kVPartition = KVPartition;
        this.lmdb = lmdb;
    }

    @Override
    public boolean next() throws IOException {
        if(this.iterator == null) {
            this.iterator = this.lmdb.openAtPos(this.kVPartition.getStart(), this.kVPartition.getEnd());
        }
        if(this.iterator.hasNext()) {
            this.current = toRow(this.iterator.next());
            return true;
        }
        return false;
    }

    @Override
    public InternalRow get() {
        return this.current;
    }

    @Override
    public void close() throws IOException {
        if(this.iterator != null) {
            this.iterator.close();
        }
        this.iterator = null;
    }

    private KVInternalRow toRow(KV kv) {
        return new KVInternalRow(kv);
    }
}
