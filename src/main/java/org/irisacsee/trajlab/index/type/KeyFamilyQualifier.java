package org.irisacsee.trajlab.index.type;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;

/**
 * 存放行键，列族，列名的组合，用于排序
 *
 * @author irisacsee
 * @since 2024/11/21
 */
public class KeyFamilyQualifier implements Comparable<KeyFamilyQualifier>, Serializable {
    private final byte[] rowKey;
    private final byte[] family;
    private final byte[] qualifier;


    public KeyFamilyQualifier(byte[] rowKey, byte[] family, byte[] qualifier) {
        this.rowKey = rowKey;
        this.family = family;
        this.qualifier = qualifier;
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public byte[] getFamily() {
        return family;
    }

    public byte[] getQualifier() {
        return qualifier;
    }

    @Override
    public int compareTo(KeyFamilyQualifier o) {
        int result = Bytes.compareTo(rowKey, o.getRowKey());
        if (result == 0) {
            result = Bytes.compareTo(family, o.getFamily());
            if (result == 0) {
                result = Bytes.compareTo(qualifier, o.getQualifier());
            }
        }
        return result;
    }
}
