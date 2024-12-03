package org.irisacsee.trajlab.index.impl;

import org.apache.hadoop.hbase.util.Bytes;
import org.irisacsee.trajlab.index.IndexStrategy;
import org.irisacsee.trajlab.index.IndexType;
import org.irisacsee.trajlab.index.type.RowKeyRange;
import org.irisacsee.trajlab.index.coder.CodeRange;
import org.irisacsee.trajlab.index.coder.XZ2Coder;
import org.irisacsee.trajlab.index.coder.XZTCoder;
import org.irisacsee.trajlab.constant.CodeConstant;
import org.irisacsee.trajlab.constant.IndexConstant;
import org.irisacsee.trajlab.index.type.ByteArray;
import org.irisacsee.trajlab.model.Trajectory;
import org.irisacsee.trajlab.query.condition.SpatialQueryCondition;
import org.irisacsee.trajlab.query.condition.SpatialTemporalQueryCondition;
import org.irisacsee.trajlab.query.condition.TemporalQueryCondition;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * XZ2T索引策略
 *
 * @author irisacsee
 * @since 2022/11/21
 */
public class XZ2TIndexStrategy extends IndexStrategy<SpatialTemporalQueryCondition> {
    private final XZ2Coder xz2Coder;
    private final XZTCoder xztCoder;
    private byte[] spatialCode;

    public XZ2TIndexStrategy() {
        physicalKeyByteLen = Short.BYTES
                + XZ2Coder.BYTES
                + XZTCoder.BYTES
                + CodeConstant.MAX_OID_LENGTH
                + CodeConstant.MAX_TID_LENGTH;
        partitionKeyByteLen = XZ2Coder.BYTES;
        computeByteLen();
        indexType = IndexType.XZ2T;
        xz2Coder = new XZ2Coder();
        xztCoder = new XZTCoder();
    }

    @Override
    protected void code(Trajectory trajectory) {
        spatialCode = xz2Coder.code(trajectory.getLineString());
    }

    @Override
    protected byte[] logicalIndex(Trajectory trajectory) {
        byte[] bytesEnd = trajectory.getTid().getBytes(StandardCharsets.UTF_8);
        long timeCodeValue = xztCoder.value(
                trajectory.getTrajectoryFeatures().getStartTime(),
                trajectory.getTrajectoryFeatures().getEndTime());
        return ByteBuffer
                .allocate(logicalKeyByteLen + bytesEnd.length)
                .put(spatialCode)
                .putLong(timeCodeValue)
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
    public List<RowKeyRange> getScanRanges(SpatialTemporalQueryCondition queryCondition) {
        SpatialQueryCondition sqc = queryCondition.getSpatialQueryCondition();
        List<CodeRange> scrList = xz2Coder.ranges(sqc);
        TemporalQueryCondition tqc = queryCondition.getTemporalQueryCondition();
        List<CodeRange> tcrList = xztCoder.ranges(tqc);

        // 先计算需要的List容量
        long[] lowerRecord = new long[scrList.size()];
        long[] upperRecord = new long[scrList.size()];
        int offset = 0, capacity = 0;
        for (CodeRange scr : scrList) {
            lowerRecord[offset] = Bytes.toLong(scr.getLower());
            upperRecord[offset] = Bytes.toLong(scr.getUpper());
            capacity += (int) (upperRecord[offset] - lowerRecord[offset] + 1);
            ++offset;
        }
        capacity *= tcrList.size() * shardNum;

        List<RowKeyRange> ranges = new ArrayList<>(capacity);
        for (int i = 0; i < scrList.size(); ++i) {
            boolean sValidated = scrList.get(i).isValidated();
            for (long xz2Code = lowerRecord[i]; xz2Code <= upperRecord[i]; ++xz2Code) {
                for (CodeRange tcr : tcrList) {
                    boolean validated = sValidated && tcr.isValidated();
                    for (short shard = 0; shard < shardNum; ++shard) {
                        ranges.add(newRowKeyRange(shard, Bytes.toBytes(xz2Code), tcr, validated));
                    }
                }
            }
        }

        return ranges;
    }

    @Override
    public List<RowKeyRange> getPartitionedScanRanges(SpatialTemporalQueryCondition queryCondition) {
        SpatialQueryCondition sqc = queryCondition.getSpatialQueryCondition();
        List<CodeRange> scrList = xz2Coder.ranges(sqc);
        TemporalQueryCondition tqc = queryCondition.getTemporalQueryCondition();
        List<CodeRange> tcrList = xztCoder.ranges(tqc);

        // 先计算需要的List容量
        long[] lowerRecord = new long[scrList.size()];
        long[] upperRecord = new long[scrList.size()];
        int offset = 0, capacity = 0;
        for (CodeRange scr : scrList) {
            lowerRecord[offset] = Bytes.toLong(scr.getLower());
            upperRecord[offset] = Bytes.toLong(scr.getUpper());
            capacity += (int) (upperRecord[offset] - lowerRecord[offset] + 1);
            ++offset;
        }
        capacity *= tcrList.size();

        List<RowKeyRange> ranges = new ArrayList<>(capacity);
        for (int i = 0; i < scrList.size(); ++i) {
            CodeRange scr = scrList.get(i);
            boolean sValidated = scr.isValidated();
            for (long xz2Code = lowerRecord[i]; xz2Code <= upperRecord[i]; ++xz2Code) {
                short shard = (short) Math.abs(
                        ByteArray.hashSfcValue(xz2Code) / IndexConstant.DEFAULT_RANGE_NUM % shardNum);
                for (CodeRange tcr : tcrList) {
                    boolean validated = sValidated && tcr.isValidated();
                    ranges.add(newRowKeyRange(shard, Bytes.toBytes(xz2Code), tcr, validated));
                }
            }
        }

        return ranges;
    }

    @Override
    public String parsePhysicalIndex2String(ByteArray physicalIndex) {
        return "Row key index: {"
                + "shardNum="
                + getShardNum(physicalIndex)
                + ", xz2="
                + extractSpatialCode(physicalIndex)
                + ", timeCoding = "
                + getTimeElementCode(physicalIndex)
                + ", oidAndTid="
                + getOid(physicalIndex)
                + "-"
                + getTid(physicalIndex)
                + '}';
    }

    @Override
    public String parseScanIndex2String(ByteArray scanIndex) {
        return "Row key index: {"
                + "shardNum="
                + getShardNum(scanIndex)
                + ", xz2="
                + extractSpatialCode(scanIndex)
                + ", timeCoding = "
                + getTimeElementCode(scanIndex)
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
        buffer.getLong();
        byte[] oidBytes = new byte[CodeConstant.MAX_OID_LENGTH];
        buffer.get(oidBytes);
        return new String(oidBytes, StandardCharsets.UTF_8);
    }

    @Override
    public String getTid(ByteArray byteArray) {
        ByteBuffer buffer = byteArray.toByteBuffer();
        buffer.flip();
        buffer.getShort();
        buffer.getLong();
        buffer.getLong();
        byte[] oidBytes = new byte[CodeConstant.MAX_OID_LENGTH];
        buffer.get(oidBytes);
        int validTidLength = buffer.remaining();
        byte[] validTidBytes = new byte[validTidLength];
        buffer.get(validTidBytes);
        return new String(validTidBytes, StandardCharsets.UTF_8);
    }

    public ByteArray extractSpatialCode(ByteArray byteArray) {
        ByteBuffer buffer = byteArray.toByteBuffer();
        buffer.flip();
        buffer.getShort();
        byte[] bytes = new byte[Long.BYTES];
        buffer.get(bytes);
        return new ByteArray(bytes);
    }

    public long getTimeElementCode(ByteArray byteArray) {
        ByteBuffer buffer = byteArray.toByteBuffer();
        buffer.flip();
        buffer.getShort();
        buffer.getLong();
        return xztCoder.getElementCode(buffer.getLong());
    }
}
