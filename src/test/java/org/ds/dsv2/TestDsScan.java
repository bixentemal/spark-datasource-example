package org.ds.dsv2;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDsScan {

    private void check(KVPartition[] KVPartitions, int entriesCount, int partitionNumber) {
        assertEquals(partitionNumber, KVPartitions.length);
        for(int i=0; i<partitionNumber-1; i++){
            assertEquals(KVPartitions[i].getEnd(), KVPartitions[i+1].getStart());
        }
        assertEquals(entriesCount, KVPartitions[partitionNumber-1].getEnd());
    }

    @Test
    public void testPartition() {
        check((KVPartition[]) DsScan.partition(65, 5), 65, 5);
        check((KVPartition[]) DsScan.partition(63, 5), 63, 5);
        check((KVPartition[]) DsScan.partition(5, 6), 5, 5);
        check((KVPartition[]) DsScan.partition(6, 5), 6, 5);
    }
}
