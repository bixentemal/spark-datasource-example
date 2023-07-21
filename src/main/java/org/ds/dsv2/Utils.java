package org.ds.dsv2;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static java.lang.Integer.BYTES;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;

public class Utils {

    public static String bb_to_str(ByteBuffer buffer, Charset charset){
        byte[] bytes;
        if(buffer.hasArray()) {
            bytes = buffer.array();
        } else {
            bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
        }
        return new String(bytes, charset);
    }

    static int bb_to_int(final ByteBuffer value) {
        return value.getInt();
    }

    static ByteBuffer int_to_bb(final int value) {
        final ByteBuffer bb = allocateDirect(BYTES);
        bb.putInt(value).flip();
        return bb;
    }
    static ByteBuffer str_to_bb(final String value) {
        byte[] str_array = value.getBytes(UTF_8);
        final ByteBuffer bb = allocateDirect(str_array.length);
        bb.put(str_array).flip();
        return bb;
    }

    static String bb_to_str(ByteBuffer buffer){
        byte[] bytes;
        if(buffer.hasArray()) {
            bytes = buffer.array();
        } else {
            bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
        }
        return new String(bytes, UTF_8);
    }
}
