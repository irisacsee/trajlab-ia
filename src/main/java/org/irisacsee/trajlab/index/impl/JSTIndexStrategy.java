package org.irisacsee.trajlab.index.impl;

import org.apache.hadoop.hbase.util.Bytes;
import org.irisacsee.trajlab.constant.CodeConstant;
import org.irisacsee.trajlab.index.IndexStrategy;
import org.irisacsee.trajlab.index.IndexType;
import org.irisacsee.trajlab.index.coder.CodeRange;
import org.irisacsee.trajlab.index.coder.XZ2Coder;
import org.irisacsee.trajlab.index.coder.XZTCoder;
import org.irisacsee.trajlab.index.type.ByteArray;
import org.irisacsee.trajlab.index.type.RowKeyRange;
import org.irisacsee.trajlab.index.type.TimeLine;
import org.irisacsee.trajlab.index.type.TimePeriod;
import org.irisacsee.trajlab.model.Trajectory;
import org.irisacsee.trajlab.query.condition.SpatialQueryCondition;
import org.irisacsee.trajlab.query.condition.SpatialTemporalQueryCondition;
import org.irisacsee.trajlab.query.condition.TemporalQueryCondition;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * JUST时空索引策略
 */
public class JSTIndexStrategy extends IndexStrategy<SpatialTemporalQueryCondition> {
    private static final ZonedDateTime EPOCH = ZonedDateTime.ofInstant(Instant.EPOCH, CodeConstant.TIME_ZONE);
    private final XZ2Coder xz2Coder;
    private final TimePeriod timePeriod;
    private final Random random;
    private long periodCodeValue;

    public JSTIndexStrategy() {
        physicalKeyByteLen = Short.BYTES
                + XZTCoder.BYTES
                + XZ2Coder.BYTES
                + CodeConstant.MAX_OID_LENGTH
                + CodeConstant.MAX_TID_LENGTH;
        partitionKeyByteLen = XZTCoder.BYTES;
        computeByteLen();
        indexType = IndexType.JST;
        this.xz2Coder = new XZ2Coder();
        this.timePeriod = CodeConstant.DEFAULT_TIME_PERIOD;
        this.random = new Random();
    }

    @Override
    protected void code(Trajectory trajectory) {
        periodCodeValue = codeTime(trajectory.getTrajectoryFeatures().getStartTime());
    }

    private long codeTime(ZonedDateTime zonedDateTime) {
        return timePeriod.getChronoUnit().between(EPOCH, zonedDateTime);
    }

    @Override
    protected byte[] logicalIndex(Trajectory trajectory) {
        byte[] bytesEnd = trajectory.getTid().getBytes(StandardCharsets.UTF_8);
        byte[] spatialCode = xz2Coder.code(trajectory.getLineString());
        return ByteBuffer
                .allocate(logicalKeyByteLen + bytesEnd.length)
                .putLong(periodCodeValue)
                .put(spatialCode)
                .put(getOidCode(trajectory))
                .put(getTidCode(trajectory))
                .array();
    }

    @Override
    protected byte[] partitionIndex(Trajectory trajectory) {
        return ByteBuffer
                .allocate(partitionKeyByteLen)
                .putLong(random.nextInt(24))
                .array();
    }

    @Override
    public List<RowKeyRange> getScanRanges(SpatialTemporalQueryCondition queryCondition) {
        SpatialQueryCondition sqc = queryCondition.getSpatialQueryCondition();
        List<CodeRange> scrList = xz2Coder.ranges(sqc);
        TemporalQueryCondition tqc = queryCondition.getTemporalQueryCondition();
        List<TimeLine> timeLines = tqc.getQueryWindows();

        long[] lowerRecord = new long[timeLines.size()];
        long[] upperRecord = new long[timeLines.size()];
        int offset = 0, capacity = 0;
        for (TimeLine timeLine : timeLines) {
            lowerRecord[offset] = codeTime(timeLine.getTimeStart());
            upperRecord[offset] = codeTime(timeLine.getTimeStart());
            capacity += (int) (upperRecord[offset] - lowerRecord[offset] + 1);
            ++offset;
        }
        capacity *= scrList.size() * shardNum;

        List<RowKeyRange> ranges = new ArrayList<>(capacity);
        for (int i = 0; i < timeLines.size(); ++i) {
            for (long xztCode = lowerRecord[i]; xztCode <= upperRecord[i]; ++xztCode) {
                for (CodeRange scr : scrList) {
                    boolean validated = scr.isValidated();
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
        return getScanRanges(queryCondition);
    }

    @Override
    public String parsePhysicalIndex2String(ByteArray byteArray) {
        return null;
    }

    @Override
    public String parseScanIndex2String(ByteArray byteArray) {
        return null;
    }

    @Override
    public short getShardNum(ByteArray byteArray) {
        return 0;
    }

    @Override
    public String getOid(ByteArray byteArray) {
        return null;
    }

    @Override
    public String getTid(ByteArray byteArray) {
        return null;
    }
}
