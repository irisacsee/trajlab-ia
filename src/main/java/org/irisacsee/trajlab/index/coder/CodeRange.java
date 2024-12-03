package org.irisacsee.trajlab.index.coder;

import org.irisacsee.trajlab.index.type.ByteArray;
import org.irisacsee.trajlab.index.coder.sfc.SFCRange;

import java.nio.ByteBuffer;

/**
 * 编码范围
 *
 * @author irisacsee
 * @since 2024/11/20
 */
public class CodeRange {
    byte[] lower;
    byte[] upper;
    boolean validated;

    public CodeRange(byte[] lower, byte[] upper, boolean validated) {
        this.lower = lower;
        this.upper = upper;
        this.validated = validated;
    }

    public CodeRange() {
    }

    public byte[] getLower() {
        return lower;
    }

    public byte[] getUpper() {
        return upper;
    }

    public boolean isValidated() {
        return validated;
    }

    public void concatSfcRange(SFCRange sfcRange) {
        if (lower == null || upper == null) {
            lower = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(sfcRange.lower).array();
            upper = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(sfcRange.upper).array();
        } else {
            lower = ByteBuffer.allocate(lower.length + Long.SIZE / Byte.SIZE)
                    .put(lower)
                    .putLong(sfcRange.lower)
                    .array();
            upper = ByteBuffer.allocate(upper.length + Long.SIZE / Byte.SIZE)
                    .put(upper)
                    .putLong(sfcRange.upper)
                    .array();
        }
        validated = sfcRange.validated;
    }

    public void concatTimeIndexRange(SFCRange timeIndexRange) {
        if (lower == null || upper == null) {
            lower = ByteBuffer.allocate(XZTCoder.BYTES).putLong(timeIndexRange.getLower()).array();
            upper = ByteBuffer.allocate(XZTCoder.BYTES).putLong(timeIndexRange.getUpper()).array();
        } else {
            lower = ByteBuffer.allocate(lower.length + XZTCoder.BYTES)
                    .put(lower)
                    .putLong(timeIndexRange.getLower())
                    .array();
            upper = ByteBuffer.allocate(upper.length + XZTCoder.BYTES)
                    .put(upper)
                    .putLong(timeIndexRange.getUpper())
                    .array();
        }
        validated = timeIndexRange.isValidated();
    }

    @Override
    public String toString() {
        return "CodingRange{" +
                "lower=" + ByteArray.string(lower) +
                ", upper=" + ByteArray.string(upper) +
                ", contained=" + validated +
                '}';
    }
}
