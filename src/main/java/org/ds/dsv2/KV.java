package org.ds.dsv2;

import java.nio.ByteBuffer;

public class KV {

    private String key;
    private ByteBuffer value;

    public KV(String key, ByteBuffer value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public ByteBuffer getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "KV{" +
                "key='" + key + '\'' +
                ", value=" + value +
                '}';
    }
}
