package org.ds.dsv2;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;


public class DsScan implements Scan, Batch {

    private final DatasetOptions datasetOptions;
    private final Lmdb lmdb;
    private final StructType structType;

    public DsScan(DatasetOptions datasetOptions, StructType structType) {
        this.datasetOptions = datasetOptions;
        this.lmdb = new Lmdb(this.datasetOptions.getInPath());
        this.structType = structType;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        return partition(this.lmdb, this.datasetOptions.getNbPartitions());
    }

    private static InputPartition[] partition(Lmdb lmdb, int partitionNum) {
        return partition((int) lmdb.count(), partitionNum);
    }

    public static InputPartition[] partition(int entriesCount, int partitionNumber) {
        int partitionNum = partitionNumber > 0 && partitionNumber <= entriesCount? partitionNumber : entriesCount;
        int partSize =  Math.round((float)entriesCount / (float)partitionNum);
        InputPartition[] result = new KVPartition[partitionNum];
        for(int i=0; i<result.length; i++){
            if(i==result.length-1) {
                result[i] = new KVPartition((i*partSize), entriesCount);
            } else {
                result[i] = new KVPartition((i*partSize),i*partSize+partSize);
            }
        }
        return result;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new KVPartitionReaderFactory(this.lmdb);
    }

    @Override
    public StructType readSchema() {
        return this.structType;
    }

    @Override
    public Batch toBatch() {
        return this;
    }

}
