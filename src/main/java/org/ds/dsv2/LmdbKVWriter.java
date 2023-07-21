package org.ds.dsv2;

import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import static org.ds.dsv2.Utils.str_to_bb;
import static org.lmdbjava.Env.create;

public class LmdbKVWriter {

    private final String path;
    private static Env<ByteBuffer> senv;
    private Env<ByteBuffer> env;
    private Dbi<ByteBuffer> db;
    private Txn<ByteBuffer> txn;

    public LmdbKVWriter(String path) throws IOException {
        this.path = path;
        this.open();
    }

    private static synchronized Env<ByteBuffer> getEnv(File path) {
        if (senv == null) {
            senv = create()
                    .setMapSize(10 * 1024 * 1024) //10MB
                    .setMaxReaders(10)
                    .setMaxDbs(20)
                    .open(path);
        }
        return senv;
    }

    public static void closeEnv() {
        if (senv != null) {
            senv.close();
        }
    }

    private void open() throws IOException {
        File path = new File(this.path);
        env = getEnv(path);
        this.db = env.openDbi((String)null, null);
        this.txn = env.txnWrite();

    }

    public void write(KV kv) {
        this.db.put(txn, str_to_bb(kv.getKey()), kv.getValue());
    }

    public void commit() {
        this.txn.commit();
    }

    public void abort() {
        this.txn.abort();
    }

    public void close() {
        this.db.close();
        this.txn.close();
    }


}
