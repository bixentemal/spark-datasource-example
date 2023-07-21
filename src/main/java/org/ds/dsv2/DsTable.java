package org.ds.dsv2;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.*;

public class DsTable implements Table, SupportsRead, SupportsWrite {
    private final StructType structType;
    private DatasetOptions dsOpt;

    public DsTable(Map<String, String> map) {
        this.dsOpt = toDatasetOptions(map);
        this.structType = buildStructType();
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new DsScanBuilder(this.dsOpt, this.structType);
    }

    @Override
    public String name() {
        return this.getClass().toString();
    }

    @Override
    public StructType schema() {
        return this.structType;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return EnumSet.of(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE);
        //return Collections.checkedSet(
    }

    private static StructType buildStructType() {
        return new StructType().
                add(new StructField("key", DataTypes.StringType, false, Metadata.empty())).
                add(new StructField("value", DataTypes.IntegerType, true, Metadata.empty()));
    }

    public static DatasetOptions toDatasetOptions(Map<String, String> caseInsensitiveStringMap) {
        // Read options
        String intPath = caseInsensitiveStringMap.getOrDefault(Constants.LMDB_IN_DIR, "");
        String outPath = caseInsensitiveStringMap.getOrDefault(Constants.LMDB_OUT_DIR, "");
        Optional<String> nbPartition = Optional.ofNullable(caseInsensitiveStringMap.get(Constants.PARTITION_OPT));

        return new DatasetOptions(intPath, outPath, nbPartition);
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo logicalWriteInfo) {
        return new DsWriteBuilder(this.dsOpt);
    }

}
