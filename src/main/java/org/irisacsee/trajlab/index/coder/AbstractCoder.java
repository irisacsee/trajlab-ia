package org.irisacsee.trajlab.index.coder;

import org.irisacsee.trajlab.index.coder.sfc.SFCRange;
import org.irisacsee.trajlab.constant.IndexConstant;
import org.irisacsee.trajlab.index.type.ByteArray;
import org.irisacsee.trajlab.query.condition.AbstractQueryCondition;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractCoder<QC extends AbstractQueryCondition> implements Serializable {
    public abstract List<CodeRange> ranges(QC queryCondition);
    public abstract List<CodeRange> rangesWithPartition(QC queryCondition);

    public List<SFCRange> splitPartitionSFCRange(List<SFCRange> sfcRanges) {
        List<SFCRange> ranges = new ArrayList<>();
        for (SFCRange sfcRange : sfcRanges) {
            long low = sfcRange.lower;
            long up = sfcRange.upper;
            ByteArray cShardKey = new ByteArray(ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(low));
            short cShard = (short) Math.abs(
                    cShardKey.hashCode() / IndexConstant.DEFAULT_RANGE_NUM % IndexConstant.DEFAULT_SHARD_NUM);
            for (long key = sfcRange.lower + 1; key <= sfcRange.upper; ++key){
                ByteArray shardKey = new ByteArray(ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(key));
                short shard = (short) Math.abs(
                        shardKey.hashCode() / IndexConstant.DEFAULT_RANGE_NUM % IndexConstant.DEFAULT_SHARD_NUM);
                if (shard != cShard) {
                    ranges.add(new SFCRange(low, key-1, sfcRange.validated));
                    low = key;
                    cShard = shard;
                }
            }
            ranges.add(new SFCRange(low, up, sfcRange.validated));
        }
        return ranges;
    }
}
