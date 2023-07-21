package org.ds.dsv2;

import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;

public class DsWriteBuilder implements WriteBuilder {

    private DatasetOptions datasetOptions;

    public DsWriteBuilder(DatasetOptions datasetOptions) {
        this.datasetOptions = datasetOptions;
    }

    public Write build() {
        return new DsWrite(this.datasetOptions);
    }

}
