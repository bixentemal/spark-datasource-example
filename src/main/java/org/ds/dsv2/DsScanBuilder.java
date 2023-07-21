package org.ds.dsv2;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;

public class DsScanBuilder implements ScanBuilder {

    private DatasetOptions datasetOptions;
    private StructType structType;

    public DsScanBuilder(DatasetOptions datasetOptions, StructType structType) {
        this.datasetOptions = datasetOptions;
        this.structType = structType;
    }

    @Override
    public Scan build() {
        return new DsScan(this.datasetOptions, this.structType);
    }
}
