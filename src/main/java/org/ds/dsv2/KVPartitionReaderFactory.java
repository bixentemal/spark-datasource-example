package org.ds.dsv2;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

public class KVPartitionReaderFactory implements PartitionReaderFactory {

    private final Lmdb lmdb;

    public KVPartitionReaderFactory(Lmdb lmdb) {
        this.lmdb = lmdb;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        return new KVPartitionReader((KVPartition) partition, lmdb);
    }
}
