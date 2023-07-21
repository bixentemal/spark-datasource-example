package org.ds.dsv2;

import org.apache.spark.sql.connector.write.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DsWrite implements Write, BatchWrite {

    private static final Logger LOGGER = LoggerFactory.getLogger(DsWrite.class);

    private String path;

    public DsWrite(DatasetOptions datasetOptions) {
        path = datasetOptions.getOutPath();
    }

    @Override
    public String description() {
        return Write.super.description();
    }

    @Override
    public BatchWrite toBatch() {
        return this;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        return new KVDataWriterFactory(this.path);
    }

    private void logCommitMessages(WriterCommitMessage[] messages) {
        for(WriterCommitMessage wcm : messages) {
            LOGGER.info(wcm.toString());
        }
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        logCommitMessages(messages);
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        logCommitMessages(messages);
    }
}
