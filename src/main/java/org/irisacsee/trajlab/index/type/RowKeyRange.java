package org.irisacsee.trajlab.index.type;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * rowKey范围
 *
 * @author irisacsee
 * @since 2024/11/20
 */
public class RowKeyRange implements Serializable {
    byte[] startKey;
    byte[] endKey;
    boolean validate;
    short shardKey;

    public RowKeyRange(byte[] startKey, byte[] endKey) {
        this.startKey = startKey;
        this.endKey = endKey;
    }

    public RowKeyRange(byte[] startKey, byte[] endKey, boolean validate, short shardKey) {
        this.startKey = startKey;
        this.endKey = endKey;
        this.validate = validate;
        this.shardKey = shardKey;
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    public boolean isValidate() {
        return validate;
    }

    public short getShardKey() {
        return shardKey;
    }

    @Override
    public String toString() {
        return "RowKeyRange{" +
                "startKey=" + ByteArray.string(startKey) +
                ", endKey=" + ByteArray.string(endKey) +
                ", validate=" + validate +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RowKeyRange that = (RowKeyRange) o;
        return Bytes.equals(startKey, that.startKey) && Bytes.equals(endKey, that.endKey);
    }

    // TODO: 检查
    @Override
    public int hashCode() {
        return Objects.hash(ByteArray.hash(startKey), ByteArray.hash(endKey), shardKey);
    }
}
