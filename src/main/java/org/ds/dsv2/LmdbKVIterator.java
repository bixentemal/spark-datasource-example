package org.ds.dsv2;

import org.lmdbjava.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.ds.dsv2.Utils.bb_to_str;
import static org.lmdbjava.Env.create;

public class LmdbKVIterator implements CloseableKVIterator {

    private final String path;
    private final int start;
    private final int end;
    private int counter;
    private static Env<ByteBuffer> senv;
    private Env<ByteBuffer> env;
    private Dbi<ByteBuffer> db;
    private Txn<ByteBuffer> txn;
    private Cursor<ByteBuffer> cursor;

    public LmdbKVIterator(String path, int start, int end) throws IOException {
        this.path = path;
        this.start = start;
        this.end = end;
        this.open();
    }

    private static synchronized Env<ByteBuffer> getEnv(File path) {
        if (senv == null) {
            senv = create()
                    .setMaxReaders(10)
                    .setMaxDbs(20)
                    .open(path, EnvFlags.MDB_RDONLY_ENV);
        }
        return senv;
    }

    private void open() throws IOException {
        File path = new File(this.path);
        env = getEnv(path);
        this.db = env.openDbi((String)null, null);
        this.txn = env.txnRead();
        this.cursor = db.openCursor(txn);
        this.counter = 0;
        // seek to start
        for(int i=0; i<start; i++) {
            if(!this.cursor.next()) {
                throw new IOException("Incorrect number of entries, fail at "+i);
            }
            this.counter++;
        }
    }

    @Override
    public void close() throws IOException {
        this.cursor.close();
        this.txn.close();
        this.db.close();
        //this.env.close();
    }

    @Override
    public boolean hasNext() {
        boolean hasNext =  this.counter<this.end && this.cursor.next();
        this.counter++;
        return hasNext;
    }

    @Override
    public KV next() {
        String k = bb_to_str(this.cursor.key());
        ByteBuffer bb = this.cursor.val();
        return new KV(k, bb);
    }
}
