package org.ds.dsv2;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import java.nio.ByteBuffer;

public class KVInternalRow extends InternalRow {

    private final KV kv;

    public KVInternalRow(KV kv) {
        this.kv = kv;
    }

    @Override
    public boolean anyNull() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte(int arg0) {
        throw new UnsupportedOperationException();
    }


    @Override
    public double getDouble(int arg0) {
        return (double) get(arg0);
    }

    @Override
    public Decimal getDecimal(int i, int i1, int i2) {
        return (Decimal) get(i);
    }

    @Override
    public UTF8String getUTF8String(int i) {
        return UTF8String.fromString((String)get(i));
    }

    @Override
    public byte[] getBinary(int i) {
        return (byte[]) get(i);
    }

    @Override
    public CalendarInterval getInterval(int i) {
        return (CalendarInterval) get(i);
    }

    @Override
    public InternalRow getStruct(int i, int i1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ArrayData getArray(int i) {
        return new GenericArrayData(get(i));
    }

    @Override
    public MapData getMap(int i) {
        return (MapData) get(i);
    }

    @Override
    public Object get(int i, DataType dataType) {
        return get(i);
    }

    @Override
    public float getFloat(int arg0) {
        return (float) get(arg0);
    }

    @Override
    public int getInt(int arg0) {
        return (int) get(arg0);
    }

    @Override
    public long getLong(int arg0) {
        return (long) get(arg0);
    }

    @Override
    public short getShort(int arg0) {
        return (short) get(arg0);
    }

    @Override
    public int numFields() {
        return 2;
    }

    @Override
    public String getString(int arg0) {
        return (String) get(arg0);
    }

    @Override
    public void setNullAt(int i) {}

    @Override
    public void update(int i, Object value) {}

    @Override
    public InternalRow copy() {
        return null;
    }

    @Override
    public boolean isNullAt(int ordinal) {
        return ordinal>1;
    }

    @Override
    public boolean getBoolean(int i) {
        return (boolean) get(i);
    }


    public Object get(int arg0) {
        switch (arg0) {
            case 0:
                return this.kv.getKey();
            case 1:
                return Utils.bb_to_int(this.kv.getValue());
            default:
                throw new UnsupportedOperationException();
        }
    }
}
