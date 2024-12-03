package org.irisacsee.trajlab.index.impl;

import org.irisacsee.trajlab.index.IndexStrategy;
import org.irisacsee.trajlab.index.IndexType;
import org.irisacsee.trajlab.index.type.RowKeyRange;
import org.irisacsee.trajlab.index.coder.XZ2Coder;
import org.irisacsee.trajlab.constant.CodeConstant;
import org.irisacsee.trajlab.index.type.ByteArray;
import org.irisacsee.trajlab.model.Trajectory;
import org.irisacsee.trajlab.query.condition.SpatialQueryCondition;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * XZ2索引策略，rowKey = shard(short) + xz2(long) + oid(maxOidLength) + tid(maxTidLength)
 *
 * @author irisacsee
 * @since 2024/11/20
 */
public class XZ2IndexStrategy extends IndexStrategy<SpatialQueryCondition> {
    private final XZ2Coder coder;
    private byte[] spatialCode;

    public XZ2IndexStrategy() {
        physicalKeyByteLen = Short.BYTES
                + XZ2Coder.BYTES
                + CodeConstant.MAX_OID_LENGTH
                + CodeConstant.MAX_TID_LENGTH;
        partitionKeyByteLen = XZ2Coder.BYTES;
        computeByteLen();
        indexType = IndexType.XZ2;
        coder = new XZ2Coder();
    }

    @Override
    protected void code(Trajectory trajectory) {
        spatialCode = coder.code(trajectory.getLineString());
    }

    @Override
    protected byte[] logicalIndex(Trajectory trajectory) {
        byte[] bytesEnd = trajectory.getTid().getBytes(StandardCharsets.UTF_8);
        return ByteBuffer
                .allocate(logicalKeyByteLen + bytesEnd.length)
                .put(spatialCode)
                .put(getOidCode(trajectory))
                .put(getTidCode(trajectory))
                .array();
    }

    @Override
    protected byte[] partitionIndex(Trajectory trajectory) {
        return ByteBuffer
                .allocate(partitionKeyByteLen)
                .put(spatialCode)
                .array();
    }

    @Override
    public List<RowKeyRange> getScanRanges(SpatialQueryCondition queryCondition) {
        return getRowKeyRanges(coder.ranges(queryCondition));
    }

    @Override
    public List<RowKeyRange> getPartitionedScanRanges(SpatialQueryCondition queryCondition) {
        return getRowKeyRanges(coder.rangesWithPartition(queryCondition));
    }

    @Override
    public String parsePhysicalIndex2String(ByteArray physicalIndex) {
        return "Row key index: {"
                + "shardNum="
                + getShardNum(physicalIndex)
                + ", xz2="
                + extractSpatialCode(physicalIndex)
                + '}';
    }

    @Override
    public String parseScanIndex2String(ByteArray scanIndex) {
        return "Row key index: {"
                + "shardNum="
                + getShardNum(scanIndex)
                + ", xz2="
                + extractSpatialCode(scanIndex)
                + ", oidAndTid="
                + getOid(scanIndex)
                + "-"
                + getTid(scanIndex)
                + '}';
    }

    @Override
    public short getShardNum(ByteArray byteArray) {
        ByteBuffer buffer = byteArray.toByteBuffer();
        buffer.flip();
        return buffer.getShort();
    }

    @Override
    public String getOid(ByteArray byteArray) {
        ByteBuffer buffer = byteArray.toByteBuffer();
        buffer.flip();
        buffer.getShort();
        buffer.getLong();
        byte[] stringBytes = new byte[CodeConstant.MAX_OID_LENGTH];
        buffer.get(stringBytes);
        return new String(stringBytes, StandardCharsets.UTF_8);
    }

    @Override
    public String getTid(ByteArray byteArray) {
        ByteBuffer buffer = byteArray.toByteBuffer();
        buffer.flip();
        buffer.getShort();
        buffer.getLong();
        byte[] oidBytes = new byte[CodeConstant.MAX_OID_LENGTH];
        buffer.get(oidBytes);
        int validTidLength = buffer.remaining();
        byte[] validTidBytes = new byte[validTidLength];
        buffer.get(validTidBytes);
        return new String(validTidBytes, StandardCharsets.UTF_8);
    }

    public long extractSpatialCode(ByteArray byteArray) {
        ByteBuffer buffer = byteArray.toByteBuffer();
        buffer.flip();
        buffer.getShort();
        return buffer.getLong();
    }

    @Override
    public String toString() {
        return "XZ2IndexStrategy{"
                + "shardNum="
                + shardNum
                + ", indexType="
                + indexType
                + ", xz2Coder="
                + coder
                + '}';
    }
}
