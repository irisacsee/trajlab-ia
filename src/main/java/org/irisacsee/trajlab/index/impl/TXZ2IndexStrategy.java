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
 * TXZ2索引策略
 *
 * @author irisacsee
 * @since 2024/11/21
 */
public class TXZ2IndexStrategy extends IndexStrategy<SpatialTemporalQueryCondition> {
    private final XZ2Coder xz2Coder;
    private final XZTCoder xztCoder;
    private long timeCodeValue;

    public TXZ2IndexStrategy() {
        physicalKeyByteLen = Short.BYTES
                + XZTCoder.BYTES
                + XZ2Coder.BYTES
                + CodeConstant.MAX_OID_LENGTH
                + CodeConstant.MAX_TID_LENGTH;
        partitionKeyByteLen = XZTCoder.BYTES;
        computeByteLen();
        indexType = IndexType.TXZ2;
        this.xz2Coder = new XZ2Coder();
        this.xztCoder = new XZTCoder();
    }

    @Override
    protected void code(Trajectory trajectory) {
        timeCodeValue = xztCoder.value(
                trajectory.getTrajectoryFeatures().getStartTime(),
                trajectory.getTrajectoryFeatures().getEndTime());
    }

    @Override
    protected byte[] logicalIndex(Trajectory trajectory) {
        byte[] bytesEnd = trajectory.getTid().getBytes(StandardCharsets.UTF_8);
        byte[] spatialCode = xz2Coder.code(trajectory.getLineString());
        return ByteBuffer
                .allocate(logicalKeyByteLen + bytesEnd.length)
                .putLong(timeCodeValue)
                .put(spatialCode)
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
    public List<RowKeyRange> getScanRanges(SpatialTemporalQueryCondition queryCondition) {
        SpatialQueryCondition sqc = queryCondition.getSpatialQueryCondition();
        List<CodeRange> scrList = xz2Coder.ranges(sqc);
        TemporalQueryCondition tqc = queryCondition.getTemporalQueryCondition();
        List<CodeRange> tcrList = xztCoder.ranges(tqc);

        // 先计算需要的List容量
        long[] lowerRecord = new long[tcrList.size()];
        long[] upperRecord = new long[tcrList.size()];
        int offset = 0, capacity = 0;
        for (CodeRange tcr : tcrList) {
            lowerRecord[offset] = Bytes.toLong(tcr.getLower());
            upperRecord[offset] = Bytes.toLong(tcr.getUpper());
            capacity += (int) (upperRecord[offset] - lowerRecord[offset] + 1);
            ++offset;
        }
        capacity *= scrList.size() * shardNum;

        List<RowKeyRange> ranges = new ArrayList<>(capacity);
        for (int i = 0; i < tcrList.size(); ++i) {
            boolean tValidated = tcrList.get(i).isValidated();
            for (long xztCode = lowerRecord[i]; xztCode <= upperRecord[i]; ++xztCode) {
                for (CodeRange scr : scrList) {
                    boolean validated = tValidated && scr.isValidated();
                    for (short shard = 0; shard < shardNum; ++shard) {
                        ranges.add(newRowKeyRange(shard, Bytes.toBytes(xztCode), scr, validated));
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
        long[] lowerRecord = new long[tcrList.size()];
        long[] upperRecord = new long[tcrList.size()];
        int offset = 0, capacity = 0;
        for (CodeRange tcr : tcrList) {
            lowerRecord[offset] = Bytes.toLong(tcr.getLower());
            upperRecord[offset] = Bytes.toLong(tcr.getUpper());
            capacity += (int) (upperRecord[offset] - lowerRecord[offset] + 1);
            ++offset;
        }
        capacity *= scrList.size();

        List<RowKeyRange> ranges = new ArrayList<>(capacity);
        for (int i = 0; i < tcrList.size(); ++i) {
            CodeRange tcr = tcrList.get(i);
            boolean tValidated = tcr.isValidated();
            for (long xztCode = lowerRecord[i]; xztCode <= upperRecord[i]; ++xztCode) {
                short shard = (short) Math.abs(
                        ByteArray.hashSfcValue(xztCode) / IndexConstant.DEFAULT_RANGE_NUM % shardNum);
                for (CodeRange scr : scrList) {
                    boolean validated = tValidated && scr.isValidated();
                    ranges.add(newRowKeyRange(shard, Bytes.toBytes(xztCode), scr, validated));
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
                + ", timeCoding = "
                + getTimeElementCode(physicalIndex)
                + ", xz2="
                + extractSpatialCode(physicalIndex)
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
                + ", timeCoding = "
                + getTimeElementCode(scanIndex)
                + ", xz2="
                + extractSpatialCode(scanIndex)
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
        buffer.getShort(); // shard
        buffer.getLong(); // time code
        byte[] bytes = new byte[XZ2Coder.BYTES];
        buffer.get(bytes);
        return new ByteArray(bytes);
    }

    public long getTimeElementCode(ByteArray byteArray) {
        ByteBuffer buffer = byteArray.toByteBuffer();
        buffer.flip();
        buffer.getShort();
        long xztCode = buffer.getLong();
        return xztCoder.getElementCode(xztCode);
    }
}
