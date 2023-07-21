package org.ds.dsv2;

import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.Stat;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import static org.lmdbjava.Env.create;

public class Lmdb implements Serializable {

    private final String path;

    public Lmdb(String path) {
        this.path = path;
    }

    public long count() {
        File path = new File(this.path);

        try(Env<ByteBuffer> env = create()
                .setMaxReaders(1)
                .setMaxDbs(1)
                .open(path, EnvFlags.MDB_RDONLY_ENV)) {
            Stat stats = env.stat();
            return stats.entries;
        }

    }

    public CloseableKVIterator openAtPos(int startPos, int endPos) throws IOException {
        return new LmdbKVIterator(this.path, startPos, endPos);
    }


}
