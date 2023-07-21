package org.ds.dsv2;

import org.apache.spark.sql.connector.read.InputPartition;

public class KVPartition implements InputPartition {

    private int start;
    private int end;

    public KVPartition(int start, int end) {
        // start inclusive
        // end exclusive
        this.start = start;
        this.end = end;
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }
}
