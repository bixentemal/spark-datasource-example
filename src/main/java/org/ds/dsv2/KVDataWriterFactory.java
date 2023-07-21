package org.ds.dsv2;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;

import java.io.IOException;

public class KVDataWriterFactory implements DataWriterFactory {

    private String path;

    public KVDataWriterFactory(String path) {
        this.path = path;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        try {
            return new KVDataWriter(path,partitionId,taskId);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
