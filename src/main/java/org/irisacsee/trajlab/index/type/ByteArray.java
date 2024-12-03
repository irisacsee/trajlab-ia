package org.irisacsee.trajlab.index.type;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class ByteArray implements Serializable, Comparable<ByteArray> {
    private byte[] bytes;

    public ByteArray(byte[] bytes) {
        this.bytes = bytes;
    }

    public ByteArray(List<byte[]> bytesList) {
        int arrLen = 0;
        for (byte[] value : bytesList) {
            arrLen += value.length;
        }
        bytes = new byte[arrLen];
        int offset = 0;
        for (byte[] value : bytesList) {
            System.arraycopy(value, 0, bytes, offset, value.length);
            offset += value.length;
        }
    }

    public static int hash(byte[] bytes) {
        return 31 + Arrays.hashCode(bytes);
    }

    public static int hashSfcValue(Long code){
        return hash(ByteBuffer.allocate(Long.BYTES).putLong(code).array());
    }

    public static String string(byte[] bytes) {
        return "ByteArray{" +
                "bytes=" + Bytes.toString(bytes) +
                '}';
    }

    public static int compare(byte[] bytes1, byte[] bytes2) {
        if (bytes1 == null) {
            return 1;
        }
        if (bytes2 == null) {
            return -1;
        }
        // lexicographical order
        for (int i = 0, j = 0; (i < bytes1.length) && (j < bytes2.length); ++i, ++j) {
            final int a = (bytes1[i] & 0xff);
            final int b = (bytes2[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return bytes1.length - bytes2.length;
    }

    public ByteArray(ByteBuffer byteBuffer) {
        this.bytes = byteBuffer.array();
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public int compareTo(ByteArray o) {
        if (o == null) {
            return -1;
        }
        // lexicographical order
        for (int i = 0, j = 0; (i < bytes.length) && (j < o.bytes.length); i++, j++) {
            final int a = (bytes[i] & 0xff);
            final int b = (o.bytes[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return bytes.length - o.bytes.length;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = (prime * result) + Arrays.hashCode(bytes);
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ByteArray other = (ByteArray) obj;
        return Arrays.equals(bytes, other.bytes);
    }

    /**
     * @return The offset of the byte buffer returned is set to the last position,
     * if you want to read values from the ByteBuffer, remember to flip the returned object.
     */
    public ByteBuffer toByteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        buffer.put(bytes);
        return buffer;
    }

    @Override
    public String toString() {
        return "ByteArray{" +
                "bytes=" + Bytes.toString(bytes) +
                '}';
    }
}
