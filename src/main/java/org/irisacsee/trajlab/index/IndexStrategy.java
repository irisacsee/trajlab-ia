package org.irisacsee.trajlab.index;

import org.apache.hadoop.hbase.util.Bytes;
import org.irisacsee.trajlab.index.coder.CodeRange;
import org.irisacsee.trajlab.constant.CodeConstant;
import org.irisacsee.trajlab.constant.IndexConstant;
import org.irisacsee.trajlab.index.type.ByteArray;
import org.irisacsee.trajlab.index.type.RowKeyRange;
import org.irisacsee.trajlab.model.Trajectory;
import org.irisacsee.trajlab.query.condition.AbstractQueryCondition;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 索引策略抽象类
 *
 * @author irisacsee
 * @since 2024/11/20
 */
public abstract class IndexStrategy<QC extends AbstractQueryCondition> implements Serializable {
    protected int physicalKeyByteLen;
    protected int logicalKeyByteLen;
    protected int partitionKeyByteLen;
    protected int scanRangeByteLen;
    protected short shardNum = IndexConstant.DEFAULT_SHARD_NUM;
    protected short rangeNum = IndexConstant.DEFAULT_RANGE_NUM;
    protected IndexType indexType;

    public IndexType getIndexType() {
        return indexType;
    }

    /**
     * 在构造方法里调用
     */
    protected void computeByteLen() {
        logicalKeyByteLen = physicalKeyByteLen - Short.BYTES - CodeConstant.MAX_TID_LENGTH;
        scanRangeByteLen = physicalKeyByteLen - CodeConstant.MAX_TID_LENGTH - CodeConstant.MAX_OID_LENGTH;
    }

    /**
     * 获取轨迹在数据库中的物理索引, 即在逻辑索引之前拼接上shard, shard由逻辑索引的hash值作模运算得到
     *
     * @param trajectory 轨迹对象
     * @return 轨迹索引
     */
    public byte[] index(Trajectory trajectory) {
        code(trajectory);
        byte[] partitionIndex= partitionIndex(trajectory);
        short shard = (short) Math.abs((ByteArray.hash(partitionIndex) / rangeNum % shardNum));
        byte[] logicalIndex = logicalIndex(trajectory);
        ByteBuffer buffer = ByteBuffer.allocate(logicalIndex.length + Short.BYTES);
        buffer.put(Bytes.toBytes(shard));
        buffer.put(logicalIndex);
        return buffer.array();
    }

    public byte[] getOidCode(Trajectory trajectory) {
        return getOidCode(trajectory.getOid());
    }

    public byte[] getOidCode(String oid) {
        byte[] bytes = oid.getBytes(StandardCharsets.UTF_8);
        byte[] b3 = new byte[CodeConstant.MAX_OID_LENGTH];
        if (bytes.length < CodeConstant.MAX_OID_LENGTH) {
            byte[] bytes1 = new byte[CodeConstant.MAX_OID_LENGTH - bytes.length];
            ByteBuffer buffer = ByteBuffer.allocate(CodeConstant.MAX_OID_LENGTH);
            buffer.put(bytes1);
            buffer.put(bytes);
            b3 = buffer.array();
        } else {
            System.arraycopy(bytes, 0, b3, 0, CodeConstant.MAX_OID_LENGTH);
        }
        return b3;
    }

    public byte[] getTidCode(Trajectory trajectory) {
        byte[] bytes = trajectory.getTid().getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= CodeConstant.MAX_TID_LENGTH) {
            return bytes;
        } else {
            byte[] b3 = new byte[CodeConstant.MAX_TID_LENGTH];
            System.arraycopy(bytes, 0, b3, 0, CodeConstant.MAX_TID_LENGTH);
            return b3;
        }
    }

    /**
     * 为避免hotspot问题, 在index中设计了salt shard.
     * 为进一步确保不同shard的数据分发至不同region server, 需要对相应的索引表作pre-split操作.
     * 本方法根据shard的数量, 生成shard-1个分割点，从而将表pre-splt为shard个region.
     * @return 本索引表的split points.
     */
    public byte[][] getSplits() {
        byte[][] splits = new byte[shardNum - 1][];
        for (short i = 0; i < shardNum - 1; i++) {
            short split = (short) (i + 1);
            byte[] bytes = new byte[2];
            bytes[0] = (byte)((split >> 8) & 0xff);
            bytes[1] = (byte)(split & 0xff);
            splits[i] = bytes;
        }
        return splits;
    }

    public short getShardByOid(String oid) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(CodeConstant.MAX_OID_LENGTH);
        byte[] oidCode = getOidCode(oid);
        byteBuffer.put(oidCode);
        ByteArray byteArray = new ByteArray(byteBuffer);
        return (short) Math.abs((byteArray.hashCode() / rangeNum % shardNum));
    }

    public List<RowKeyRange> getRowKeyRanges(List<CodeRange> codeRanges) {
        List<RowKeyRange> ranges = new ArrayList<>(shardNum * codeRanges.size());
        for (CodeRange cr : codeRanges) {
            for (short shard = 0; shard < shardNum; ++shard) {
                ranges.add(newRowKeyRange(shard, cr, cr.isValidated()));
            }
        }
        return ranges;
    }

    protected byte[] toRowKeyRangeBoundary(short shard, byte[] code, boolean isEndCode) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(scanRangeByteLen);
        byteBuffer.putShort(shard);
        if (isEndCode) {
            byteBuffer.putLong(Bytes.toLong(code) + 1);
        } else {
            byteBuffer.put(code);
        }
        return byteBuffer.array();
    }

    protected RowKeyRange newRowKeyRange(short shard, CodeRange codeRange, boolean validated) {
        return new RowKeyRange(
                toRowKeyRangeBoundary(shard, codeRange.getLower(), false),
                toRowKeyRangeBoundary(shard, codeRange.getUpper(), true),
                validated, shard);
    }

    protected byte[] toRowKeyRangeBoundary(short shard, byte[] code1, byte[] code2, boolean isEndCode) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(scanRangeByteLen);
        byteBuffer.putShort(shard);
        byteBuffer.put(code1);
        if (isEndCode) {
            byteBuffer.putLong(Bytes.toLong(code2) + 1);
        } else {
            byteBuffer.put(code2);
        }
        return byteBuffer.array();
    }

    protected RowKeyRange newRowKeyRange(short shard, byte[] otherCode, CodeRange codeRange, boolean validated) {
        return new RowKeyRange(
                toRowKeyRangeBoundary(shard, otherCode, codeRange.getLower(), false),
                toRowKeyRangeBoundary(shard, otherCode, codeRange.getUpper(), true),
                validated, shard);
    }

    // 对轨迹编码
    protected abstract void code(Trajectory trajectory);
    protected abstract byte[] logicalIndex(Trajectory trajectory);
    protected abstract byte[] partitionIndex(Trajectory trajectory);

    /**
     * Get RowKey pairs for scan operation, based on spatial and temporal range.
     * A pair of RowKey is the startKey and endKey of a single scan.
     * @param queryCondition 查询框
     * @return RowKey pairs
     */
    public abstract List<RowKeyRange> getScanRanges(QC queryCondition);
    public abstract List<RowKeyRange> getPartitionedScanRanges(QC queryCondition);

    public abstract String parsePhysicalIndex2String(ByteArray byteArray);
    public abstract String parseScanIndex2String(ByteArray byteArray);

    public abstract short getShardNum(ByteArray byteArray);
    public abstract String getOid(ByteArray byteArray);
    public abstract String getTid(ByteArray byteArray);

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexStrategy that = (IndexStrategy) o;
        return shardNum == that.shardNum && indexType == that.indexType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardNum, indexType);
    }
}
