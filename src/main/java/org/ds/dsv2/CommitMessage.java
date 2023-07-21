package org.ds.dsv2;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

public class CommitMessage implements WriterCommitMessage {

    String message;

    public CommitMessage(String message) {
        this.message = message;
    }

    public String toString() {
        return this.message;
    }
}
