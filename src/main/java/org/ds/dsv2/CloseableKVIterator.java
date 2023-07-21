package org.ds.dsv2;

import java.io.Closeable;
import java.util.Iterator;

public interface CloseableKVIterator extends Iterator<KV>, Closeable {
}
