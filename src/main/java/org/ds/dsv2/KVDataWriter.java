package org.ds.dsv2;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.ds.dsv2.Utils.int_to_bb;

public class KVDataWriter implements DataWriter<InternalRow> {

    private transient LmdbKVWriter lmdbKVWriter;
    private int partitionId;
    long taskId;

    public KVDataWriter(String path, int partitionId, long taskId) throws IOException {
        this.lmdbKVWriter = new LmdbKVWriter(path);
        this.partitionId = partitionId;
        this.taskId = taskId;
    }

    @Override
    public void write(InternalRow internalRow) throws IOException {
        String k = internalRow.getString(0);
        ByteBuffer bb = int_to_bb(internalRow.getInt(1));
        this.lmdbKVWriter.write(new KV(k, bb));
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        this.lmdbKVWriter.commit();
        return new CommitMessage("Writer commit partition["+partitionId+"] task["+taskId+"] suceeded");
    }

    @Override
    public void abort() throws IOException {
        this.lmdbKVWriter.abort();
    }

    @Override
    public void close() throws IOException {
        this.lmdbKVWriter.close();
    }
}
