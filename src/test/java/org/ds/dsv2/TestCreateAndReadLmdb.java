package org.ds.dsv2;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;


import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.ds.dsv2.Utils.*;
import static org.lmdbjava.Env.create;

public class TestCreateAndReadLmdb {
    @Rule
    public TemporaryFolder tmp= new TemporaryFolder();

    @Test
    public void testCreateAndRead() throws IOException {

        File tmpLMDBpath = tmp.newFolder();
        Env<ByteBuffer> createEnv = create()
                .setMapSize(500 * 1024 * 1024) //10MB
                .setMaxReaders(2)
                .setMaxDbs(2)
                .open(tmpLMDBpath);
        try {
            final Dbi<ByteBuffer> db = createEnv.openDbi((String)null, null);
            try (Txn<ByteBuffer> txn = createEnv.txnWrite()) {
                int count;
                for (count = 0; count<20000000; count++) {
                    String k = String.format("%1$10s", count+"").replace(' ', '0');
                    db.put(txn, str_to_bb(k), int_to_bb(count));
                    if(count% 1000000 == 0)
                        System.out.println("Wrtiting "+ count + "/" + 20000000);
                }
                db.put(txn, str_to_bb("num-samples"), str_to_bb( count+""));
                txn.commit();

            }
        } finally {
            createEnv.close();
        }

        Env<ByteBuffer> env = create()
                .setMaxReaders(1)
                .setMaxDbs(2)
                .open(tmpLMDBpath, EnvFlags.MDB_RDONLY_ENV);
        final Dbi<ByteBuffer> db = env.openDbi((String)null, null);
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer key = allocateDirect(env.getMaxKeySize());
            ByteBuffer val = allocateDirect(700);
            key.put("num-samples".getBytes(UTF_8)).flip();
            ByteBuffer bb =  db.get(txn, key);
            Stat stat = db.stat(txn);
            System.out.println("Stats : "+ stat);
            System.out.println(bb.toString());
            System.out.println();
            System.out.println(bb_to_str(bb));

//            key = allocateDirect(10);
//            key.put("0000000001".getBytes(UTF_8)).flip();
//            bb =  db.get(txn, key);
//            System.out.println(bb_to_str(key));
//            System.out.println(bb_to_str(bb));

            System.out.println("range scan from 10 to 20");
            final ByteBuffer start = allocateDirect(env.getMaxKeySize());
            start.put("0000000010".getBytes(UTF_8)).flip();
            final ByteBuffer end = allocateDirect(env.getMaxKeySize());
            end.put("0000000020".getBytes(UTF_8)).flip();
            KeyRange<ByteBuffer> range = new KeyRange<>(KeyRangeType.FORWARD_CLOSED, start, end);

            try(CursorIterable<ByteBuffer> c = db.iterate(txn, range)) {
                for (final CursorIterable.KeyVal<ByteBuffer> kv : c) {
                    final ByteBuffer tmpK = kv.key();
                    final ByteBuffer tmpV = kv.val();
                    System.out.println(bb_to_str(tmpK));
                    System.out.println(bb_to_int(tmpV));

                }
            }
        }
        env.close();
    }
}
