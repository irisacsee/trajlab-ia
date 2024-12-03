package org.irisacsee.trajlab.index.impl;

import org.irisacsee.trajlab.index.IndexStrategy;
import org.irisacsee.trajlab.index.IndexType;
import org.irisacsee.trajlab.index.type.RowKeyRange;
import org.irisacsee.trajlab.index.coder.CodeRange;
import org.irisacsee.trajlab.index.coder.XZTCoder;
import org.irisacsee.trajlab.constant.CodeConstant;
import org.irisacsee.trajlab.constant.IndexConstant;
import org.irisacsee.trajlab.index.type.ByteArray;
import org.irisacsee.trajlab.model.Trajectory;
import org.irisacsee.trajlab.query.condition.TemporalQueryCondition;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * XZT索引策略
 *
 * @author irisacsee
 * @since 2024/11/21
 */
public class XZTIndexStrategy extends IndexStrategy<TemporalQueryCondition> {
    private final XZTCoder coder;
    private long timeCodeValue;

    public XZTIndexStrategy() {
        physicalKeyByteLen = Short.BYTES
                + XZTCoder.BYTES
                + CodeConstant.MAX_OID_LENGTH
                + CodeConstant.MAX_TID_LENGTH;
        partitionKeyByteLen = XZTCoder.BYTES;
        computeByteLen();
        indexType = IndexType.XZT;
        coder = new XZTCoder();
    }

    @Override
    protected void code(Trajectory trajectory) {
        timeCodeValue = coder.value(
                trajectory.getTrajectoryFeatures().getStartTime(),
                trajectory.getTrajectoryFeatures().getEndTime());
    }

    @Override
    protected byte[] logicalIndex(Trajectory trajectory) {
        byte[] bytesEnd = trajectory.getTid().getBytes(StandardCharsets.UTF_8);
        return ByteBuffer
                .allocate(logicalKeyByteLen + bytesEnd.length)
                .putLong(timeCodeValue)
                .put(getOidCode(trajectory))
                .put(getTidCode(trajectory))
                .array();
    }

    @Override
    protected byte[] partitionIndex(Trajectory trajectory) {
        return ByteBuffer
                .allocate(partitionKeyByteLen)
                .putLong(timeCodeValue)
                .array();
    }

    @Override
    public List<RowKeyRange> getScanRanges(TemporalQueryCondition queryCondition) {
        return getRowKeyRanges(coder.ranges(queryCondition));
    }

    @Override
    public List<RowKeyRange> getPartitionedScanRanges(TemporalQueryCondition queryCondition) {
        List<CodeRange> codeRanges = coder.rangesWithPartition(queryCondition);
        List<RowKeyRange> ranges = new ArrayList<>(codeRanges.size());
        for (CodeRange cr : codeRanges) {
            short shard = (short) Math.abs(
                    (ByteArray.hash(cr.getLower()) / IndexConstant.DEFAULT_RANGE_NUM % shardNum));
            ranges.add(new RowKeyRange(
                    toRowKeyRangeBoundary(shard, cr.getLower(), false),
                    toRowKeyRangeBoundary(shard, cr.getUpper(), true),
                    cr.isValidated(), shard));
        }
        return ranges;
    }

    @Override
    public String parsePhysicalIndex2String(ByteArray physicalIndex) {
        return "Row key index: {"
                + "shardNum = "
                + getShardNum(physicalIndex)
                + ", XZT = "
                + getTimeCode(physicalIndex)
                + ", oidAndTid="
                + getOid(physicalIndex)
                + "-"
                + getTid(physicalIndex)
                + '}';
    }

    @Override
    public String parseScanIndex2String(ByteArray scanIndex) {
        return "Row key index: {"
                + "shardNum = "
                + getShardNum(scanIndex)
                + ", XZT = "
                + getTimeCode(scanIndex)
                + '}';
    }

    @Override
    public short getShardNum(ByteArray physicalIndex) {
        ByteBuffer buffer = physicalIndex.toByteBuffer();
        buffer.flip();
        return buffer.getShort();
    }

    @Override
    public String getOid(ByteArray physicalIndex) {
        ByteBuffer buffer = physicalIndex.toByteBuffer();
        buffer.flip();
        buffer.getShort(); // shard
        buffer.getLong();
        byte[] oidBytes = new byte[CodeConstant.MAX_OID_LENGTH];
        buffer.get(oidBytes);
        return new String(oidBytes, StandardCharsets.UTF_8);
    }

    @Override
    public String getTid(ByteArray physicalIndex) {
        ByteBuffer buffer = physicalIndex.toByteBuffer();
        buffer.flip();
        // shard
        buffer.getShort();
        // time code
        buffer.getLong();
        // OID
        byte[] oidBytes = new byte[CodeConstant.MAX_OID_LENGTH];
        buffer.get(oidBytes);
        // TID
        int validTidLength = buffer.remaining();
        byte[] validTidBytes = new byte[validTidLength];
        buffer.get(validTidBytes);
        return new String(validTidBytes, StandardCharsets.UTF_8);
    }

    public long getTimeCode(ByteArray byteArray) {
        ByteBuffer buffer = byteArray.toByteBuffer();
        buffer.flip();
        buffer.getShort();
        return buffer.getLong();
    }
}
